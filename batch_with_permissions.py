import csv
import json
import requests
import time

# Columns that must never be sent to SharePoint
SYSTEM_COLUMNS = {
    "Action", "action", "ID", "Id", "ItemId", "ItemInternalId", "Modified", "Created",
    "Author", "Editor", "GUID", "VersionNumber", "{Identifier}",
    "{IsFolder}", "{Thumbnail}", "{Link}", "{Name}", "{FilenameWithExtension}",
    "{Path}", "{FullPath}", "{HasAttachments}", "{ContentType}",
    "{ContentType}#Id", "Add_UserIDs", "Remove_UserIDs", "@odata.etag"  # Permission columns
}

def is_system_column(col):
    """Detect SharePoint system columns that must not be sent."""
    return (
        col in SYSTEM_COLUMNS
        or col.startswith("{")
        or col.lower().startswith("odata")
        or col.endswith("Claims")
        or col.endswith("#Id")
        or col.endswith("#Claims")
        or col.endswith("#Value")
        or "@odata" in col
    )

def clean_payload(row):
    """Remove SharePoint system/metadata fields + permission columns."""
    MAX_FIELD_LENGTH = 2000

    cleaned = {}
    for k, v in row.items():
        if not k or not v or not v.strip():
            continue

        if is_system_column(k):
            continue

        if v.startswith("{") and v.endswith("}"):
            continue

        value = v.strip()
        if len(value) > MAX_FIELD_LENGTH:
            value = value[:MAX_FIELD_LENGTH]

        cleaned[k] = value

    return cleaned


def parse_user_ids(user_id_string):
    """Parse semicolon-separated user IDs."""
    if not user_id_string or not user_id_string.strip():
        return []
    return [uid.strip() for uid in user_id_string.split(';') if uid.strip()]


def get_action(row):
    """Get action from row, case-insensitive."""
    return (row.get("Action") or row.get("action") or "").lower().strip()


def get_item_id(row):
    """Get ItemId from row, checking multiple possible column names."""
    return (row.get("ItemId") or row.get("ID") or row.get("Id") or "").strip()


def generate_create_batch(rows, base_url, batch_number=1):
    """
    Generate batch request for CREATE operations only (no permissions).
    Used as first batch for ADD items that need permissions.
    """
    batch_id = f"batch_{batch_number:04d}"
    changeset_id = f"changeset_{batch_number:04d}"

    output = []
    output.append(f"--{batch_id}")
    output.append(f"Content-Type: multipart/mixed; boundary={changeset_id}")
    output.append("")

    processed_count = 0

    for row in rows:
        action = get_action(row)
        if action != "add":
            continue

        payload = clean_payload(row)

        output.append(f"--{changeset_id}")
        output.append("Content-Type: application/http")
        output.append("Content-Transfer-Encoding: binary")
        output.append("")
        output.append(f"POST {base_url} HTTP/1.1")
        output.append("Content-Type: application/json;odata=nometadata")
        output.append("Accept: application/json;odata=nometadata")
        output.append("")
        output.append(json.dumps(payload))
        output.append("")

        processed_count += 1

    output.append(f"--{changeset_id}--")
    output.append(f"--{batch_id}--")
    output.append("")

    return "\n".join(output), batch_id, processed_count


def generate_permissions_batch(item_ids, user_permissions, base_url, role_id, batch_number=1):
    """
    Generate batch request for permission operations on existing items.

    Args:
        item_ids: List of SharePoint item IDs
        user_permissions: List of dicts with 'add_users' and 'remove_users' keys
        base_url: SharePoint list items endpoint
        role_id: Role definition ID for adding permissions
        batch_number: Batch sequence number
    """
    batch_id = f"batch_{batch_number:04d}"
    changeset_id = f"changeset_{batch_number:04d}"

    output = []
    output.append(f"--{batch_id}")
    output.append(f"Content-Type: multipart/mixed; boundary={changeset_id}")
    output.append("")

    processed_count = 0

    for item_id, perms in zip(item_ids, user_permissions):
        add_users = perms.get('add_users', [])
        remove_users = perms.get('remove_users', [])

        # Only process if there are permissions to set
        if not add_users and not remove_users:
            continue

        # Break inheritance if adding users
        if add_users:
            output.append(f"--{changeset_id}")
            output.append("Content-Type: application/http")
            output.append("Content-Transfer-Encoding: binary")
            output.append("")
            output.append(f"POST {base_url}({item_id})/breakroleinheritance(copyRoleAssignments=false) HTTP/1.1")
            output.append("Accept: application/json;odata=nometadata")
            output.append("Content-Type: application/json;odata=verbose")
            output.append("")
            output.append("")

            # Add each user
            for user_id in add_users:
                output.append(f"--{changeset_id}")
                output.append("Content-Type: application/http")
                output.append("Content-Transfer-Encoding: binary")
                output.append("")
                output.append(f"POST {base_url}({item_id})/roleassignments/addroleassignment(principalid=@p,roledefid=@r)?@p={user_id}&@r={role_id} HTTP/1.1")
                output.append("Accept: application/json;odata=nometadata")
                output.append("Content-Type: application/json;odata=verbose")
                output.append("")
                output.append("")

        # Remove users
        for user_id in remove_users:
            output.append(f"--{changeset_id}")
            output.append("Content-Type: application/http")
            output.append("Content-Transfer-Encoding: binary")
            output.append("")
            output.append(f"POST {base_url}({item_id})/roleassignments/removeroleassignment(principalid=@p)?@p={user_id} HTTP/1.1")
            output.append("Accept: application/json;odata=nometadata")
            output.append("Content-Type: application/json;odata=verbose")
            output.append("")
            output.append("")

        processed_count += 1

    output.append(f"--{changeset_id}--")
    output.append(f"--{batch_id}--")
    output.append("")

    return "\n".join(output), batch_id, processed_count


def generate_batch_with_permissions(rows, base_url, role_id, batch_number=1):
    """
    Generate batch request for UPDATE/DELETE operations with permissions.
    (Used for operations where ItemId already exists)
    """
    batch_id = f"batch_{batch_number:04d}"
    changeset_id = f"changeset_{batch_number:04d}"

    output = []
    output.append(f"--{batch_id}")
    output.append(f"Content-Type: multipart/mixed; boundary={changeset_id}")
    output.append("")

    processed_count = 0

    for row in rows:
        action = get_action(row)
        item_id = get_item_id(row)

        # Skip ADD actions - they're handled separately
        if action == "add":
            continue

        # Parse permission columns
        add_user_ids = parse_user_ids(row.get("Add_UserIDs", ""))
        remove_user_ids = parse_user_ids(row.get("Remove_UserIDs", ""))

        # Clean data payload (excludes permission columns)
        payload = clean_payload(row)

        if action == "update" and item_id:
            # Step 1: Update item
            output.append(f"--{changeset_id}")
            output.append("Content-Type: application/http")
            output.append("Content-Transfer-Encoding: binary")
            output.append("")
            output.append(f"PATCH {base_url}({item_id}) HTTP/1.1")
            output.append("Content-Type: application/json;odata=nometadata")
            output.append("Accept: application/json;odata=nometadata")
            output.append("IF-MATCH: *")
            output.append("")
            output.append(json.dumps(payload))
            output.append("")

            # Step 2: Break inheritance if adding users
            if add_user_ids:
                output.append(f"--{changeset_id}")
                output.append("Content-Type: application/http")
                output.append("Content-Transfer-Encoding: binary")
                output.append("")
                output.append(f"POST {base_url}({item_id})/breakroleinheritance(copyRoleAssignments=false) HTTP/1.1")
                output.append("Accept: application/json;odata=nometadata")
                output.append("Content-Type: application/json;odata=verbose")
                output.append("")
                output.append("")

            # Step 3: Add permissions
            for user_id in add_user_ids:
                output.append(f"--{changeset_id}")
                output.append("Content-Type: application/http")
                output.append("Content-Transfer-Encoding: binary")
                output.append("")
                output.append(f"POST {base_url}({item_id})/roleassignments/addroleassignment(principalid=@p,roledefid=@r)?@p={user_id}&@r={role_id} HTTP/1.1")
                output.append("Accept: application/json;odata=nometadata")
                output.append("Content-Type: application/json;odata=verbose")
                output.append("")
                output.append("")

            # Step 4: Remove permissions
            for user_id in remove_user_ids:
                output.append(f"--{changeset_id}")
                output.append("Content-Type: application/http")
                output.append("Content-Transfer-Encoding: binary")
                output.append("")
                output.append(f"POST {base_url}({item_id})/roleassignments/removeroleassignment(principalid=@p)?@p={user_id} HTTP/1.1")
                output.append("Accept: application/json;odata=nometadata")
                output.append("Content-Type: application/json;odata=verbose")
                output.append("")
                output.append("")

            processed_count += 1

        elif action == "delete" and item_id:
            output.append(f"--{changeset_id}")
            output.append("Content-Type: application/http")
            output.append("Content-Transfer-Encoding: binary")
            output.append("")
            output.append(f"DELETE {base_url}({item_id}) HTTP/1.1")
            output.append("Accept: application/json;odata=nometadata")
            output.append("IF-MATCH: *")
            output.append("")
            output.append("")

            processed_count += 1

    # Close boundaries
    output.append(f"--{changeset_id}--")
    output.append(f"--{batch_id}--")
    output.append("")

    return "\n".join(output), batch_id, processed_count


def read_csv_in_chunks(csv_file_path, chunk_size=100):
    """Read CSV file and yield chunks of rows."""
    with open(csv_file_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        chunk = []
        for row in reader:
            chunk.append(row)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk


def parse_batch_response_for_item_ids(response_data):
    """
    Extract ItemIds from Power Automate/SharePoint batch response.

    Returns:
        List of item IDs created in the batch
    """
    try:
        # PA returns the response in a specific format
        # This is a simplified parser - adjust based on actual PA response format
        if isinstance(response_data, dict):
            # Look for item IDs in the response
            # You may need to adjust this based on actual response structure
            return []
        return []
    except Exception as e:
        print(f"   ⚠️ Error parsing response for ItemIds: {e}")
        return []


def send_to_power_automate(batch_body, target_url, batch_boundary, pa_webhook_url, batch_num=1):
    """Send the batch payload to Power Automate HTTP trigger."""
    payload = {
        "batchBody": batch_body,
        "targetUrl": target_url,
        "batchBoundary": batch_boundary
    }

    headers = {
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(pa_webhook_url, json=payload, headers=headers, timeout=20)
        response.raise_for_status()

        result = response.json() if response.text else {}

        return {
            "success": True,
            "batch_number": batch_num,
            "status_code": response.status_code,
            "response": result
        }

    except requests.exceptions.Timeout:
        return {
            "success": True,
            "batch_number": batch_num,
            "status": "timeout"
        }

    except requests.exceptions.RequestException as e:
        error_msg = str(e)
        if hasattr(e, 'response') and e.response is not None:
            error_msg = e.response.text

        return {
            "success": False,
            "batch_number": batch_num,
            "error": error_msg
        }


def process_csv_with_permissions(csv_file_path, base_url, role_id, pa_webhook_url, chunk_size=50, delay_between_batches=2, max_batches=None):
    """
    Process CSV file with TWO-BATCH approach:
    - Batch 1: Create ADD items
    - Batch 2: Set permissions on newly created items
    - Single batch for UPDATE/DELETE with permissions
    """

    print(f"\n{'='*60}")
    print(f"Starting TWO-BATCH processing WITH PERMISSIONS")
    print(f"{'='*60}")
    print(f"CSV File: {csv_file_path}")
    print(f"Chunk Size: {chunk_size} rows per batch")
    print(f"Role ID for Add Access: {role_id}")
    print(f"Delay Between Batches: {delay_between_batches} seconds")
    if max_batches:
        print(f"Max Batches (TEST MODE): {max_batches}")
    print(f"{'='*60}\n")

    results = []
    batch_number = 1
    total_rows = 0
    total_processed = 0
    failed_batches = []
    timeout_batches = []

    for chunk in read_csv_in_chunks(csv_file_path, chunk_size):
        if max_batches and batch_number > max_batches:
            print(f"⚠ Reached max batch limit ({max_batches}). Stopping.\n")
            break

        total_rows += len(chunk)

        # Separate ADD rows from UPDATE/DELETE rows
        add_rows = [r for r in chunk if get_action(r) == 'add']
        other_rows = [r for r in chunk if get_action(r) in ['update', 'delete']]

        # ======================
        # Process ADD rows (TWO-BATCH approach)
        # ======================
        if add_rows:
            # Check which ADD rows need permissions
            add_with_perms = []
            add_perm_data = []

            for row in add_rows:
                add_users = parse_user_ids(row.get("Add_UserIDs", ""))
                remove_users = parse_user_ids(row.get("Remove_UserIDs", ""))

                if add_users or remove_users:
                    add_with_perms.append(row)
                    add_perm_data.append({
                        'add_users': add_users,
                        'remove_users': remove_users
                    })

            print(f"📦 Batch {batch_number}: Creating {len(add_rows)} items ({len(add_with_perms)} need permissions)...")

            try:
                # BATCH 1: Create items
                batch_content, batch_id, processed = generate_create_batch(
                    add_rows, base_url, batch_number
                )

                if processed > 0:
                    total_processed += processed

                    debug_file = f"batch_{batch_number:04d}_create.txt"
                    with open(debug_file, "w", encoding="utf-8") as f:
                        f.write(batch_content)

                    print(f"   📄 Saved to {debug_file}")
                    print(f"   🚀 Sending CREATE batch...")

                    result = send_to_power_automate(
                        batch_body=batch_content,
                        target_url=base_url,
                        batch_boundary=batch_id,
                        pa_webhook_url=pa_webhook_url,
                        batch_num=batch_number
                    )

                    results.append(result)

                    if result.get("status") == "timeout":
                        print(f"   ⏱️  Batch {batch_number} timed out (likely successful, verify in PA)")
                        timeout_batches.append(batch_number)
                    elif result["success"]:
                        print(f"   ✅ Batch {batch_number} CREATE completed")

                        # Extract ItemIds from PA response
                        if add_with_perms:
                            item_ids = []

                            # Parse itemIds from PA response
                            if "itemIds" in result.get("response", {}):
                                item_ids_data = result["response"]["itemIds"]
                                item_ids = [item["Id"] for item in item_ids_data if "Id" in item]
                                print(f"   📋 Extracted {len(item_ids)} ItemIds from response")

                            if len(item_ids) == len(add_with_perms):
                                # BATCH 2: Set permissions automatically
                                batch_number += 1
                                print(f"\n📦 Batch {batch_number}: Setting permissions on {len(item_ids)} items...")

                                perm_batch_content, perm_batch_id, perm_processed = generate_permissions_batch(
                                    item_ids, add_perm_data, base_url, role_id, batch_number
                                )

                                perm_debug_file = f"batch_{batch_number:04d}_permissions.txt"
                                with open(perm_debug_file, "w", encoding="utf-8") as f:
                                    f.write(perm_batch_content)

                                print(f"   📄 Saved to {perm_debug_file}")
                                print(f"   🚀 Sending PERMISSIONS batch...")

                                perm_result = send_to_power_automate(
                                    batch_body=perm_batch_content,
                                    target_url=base_url,
                                    batch_boundary=perm_batch_id,
                                    pa_webhook_url=pa_webhook_url,
                                    batch_num=batch_number
                                )

                                results.append(perm_result)

                                if perm_result.get("status") == "timeout":
                                    print(f"   ⏱️  Permissions batch timed out")
                                    timeout_batches.append(batch_number)
                                elif perm_result["success"]:
                                    print(f"   ✅ Permissions set successfully")
                                else:
                                    print(f"   ❌ Permissions batch failed")
                                    failed_batches.append(batch_number)

                            elif len(item_ids) == 0:
                                print(f"   ⚠️  No ItemIds returned from PA, skipping permissions")
                            else:
                                print(f"   ⚠️  ItemId mismatch: Expected {len(add_with_perms)}, got {len(item_ids)}")
                                print(f"   ⚠️  Skipping permissions for this batch")

                        if delay_between_batches > 0:
                            print(f"   ⏳ Waiting {delay_between_batches}s...\n")
                            time.sleep(delay_between_batches)

                    else:
                        print(f"   ❌ CREATE batch failed")
                        failed_batches.append(batch_number)

            except Exception as e:
                print(f"   ❌ Exception in batch {batch_number}: {str(e)}")
                failed_batches.append(batch_number)
                results.append({
                    "success": False,
                    "batch_number": batch_number,
                    "error": str(e)
                })

            batch_number += 1

        # ======================
        # Process UPDATE/DELETE rows (SINGLE-BATCH with permissions)
        # ======================
        if other_rows:
            print(f"📦 Batch {batch_number}: Processing {len(other_rows)} UPDATE/DELETE rows...")

            try:
                batch_content, batch_id, processed = generate_batch_with_permissions(
                    other_rows, base_url, role_id, batch_number
                )

                if processed > 0:
                    total_processed += processed

                    debug_file = f"batch_{batch_number:04d}.txt"
                    with open(debug_file, "w", encoding="utf-8") as f:
                        f.write(batch_content)

                    print(f"   📄 Saved to {debug_file}")
                    print(f"   🚀 Sending to Power Automate...")

                    result = send_to_power_automate(
                        batch_body=batch_content,
                        target_url=base_url,
                        batch_boundary=batch_id,
                        pa_webhook_url=pa_webhook_url,
                        batch_num=batch_number
                    )

                    results.append(result)

                    if result.get("status") == "timeout":
                        print(f"   ⏱️  Batch {batch_number} timed out")
                        timeout_batches.append(batch_number)
                    elif result["success"]:
                        print(f"   ✅ Batch {batch_number} completed")
                    else:
                        print(f"   ❌ Batch {batch_number} failed")
                        failed_batches.append(batch_number)

                if delay_between_batches > 0:
                    print(f"   ⏳ Waiting {delay_between_batches}s...\n")
                    time.sleep(delay_between_batches)

            except Exception as e:
                print(f"   ❌ Exception in batch {batch_number}: {str(e)}")
                failed_batches.append(batch_number)
                results.append({
                    "success": False,
                    "batch_number": batch_number,
                    "error": str(e)
                })

            batch_number += 1

    # Summary
    print(f"\n{'='*60}")
    print(f"PROCESSING COMPLETE")
    print(f"{'='*60}")
    print(f"Total Rows Read: {total_rows}")
    print(f"Total Rows Processed: {total_processed}")
    print(f"Total Batches: {batch_number - 1}")
    print(f"✅ Successful: {len([r for r in results if r['success'] and r.get('status') != 'timeout'])}")
    print(f"⏱️  Timed Out: {len(timeout_batches)} (verify in PA)")
    print(f"❌ Failed: {len(failed_batches)}")

    if timeout_batches:
        print(f"\n⏱️  Timeout Batches: {timeout_batches}")

    if failed_batches:
        print(f"\n❌ Failed Batches: {failed_batches}")

    print(f"{'='*60}\n")

    return results


if __name__ == "__main__":
    # Configuration
    target_url = "https://razrgroup-my.sharepoint.com/personal/communication_razor-group_com/_api/web/lists/GetByTitle('OT_4')/items"
    csv_file = "output.csv"

    # Power Automate webhook URL
    PA_WEBHOOK_URL = "https://default0922decaaf3c4870acea84b9557b04.6a.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/b67ce8f71f8643ca9f389aedd590a7c4/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=gNckNGyNWSVsioZAjaGybgowiJ-shdo39WLth0lE6HA"

    # Permission Configuration
    ROLE_ID = 1073741827  # SharePoint Role ID (1073741826 = Edit/Contribute, 1073741827 = Read, 1073741829 = Full Control)

    # Batch Configuration
    CHUNK_SIZE = 50
    DELAY_BETWEEN_BATCHES = 2
    TEST_MODE_MAX_BATCHES = None  # Set to a number for testing, None for all

    # Process CSV with permissions
    if TEST_MODE_MAX_BATCHES:
        print(f"⚠ TEST MODE: Processing only first {TEST_MODE_MAX_BATCHES} batches")
        print(f"   Set TEST_MODE_MAX_BATCHES = None to process all\n")

    results = process_csv_with_permissions(
        csv_file_path=csv_file,
        base_url=target_url,
        role_id=ROLE_ID,
        pa_webhook_url=PA_WEBHOOK_URL,
        chunk_size=CHUNK_SIZE,
        delay_between_batches=DELAY_BETWEEN_BATCHES,
        max_batches=TEST_MODE_MAX_BATCHES
    )

    # Save results
    results_file = "batch_permissions_results.json"
    with open(results_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print(f"📄 Detailed results saved to {results_file}")
