# SharePoint List Batch Manager

A Python tool for bulk managing SharePoint lists with batch operations, permission handling, and data validation. Fetches CSV data from Power Automate webhooks and processes it safely for SharePoint API operations.

## Features

- **Bulk CSV Import**: Fetch and process CSV data from SharePoint lists via Power Automate webhooks
- **Permission Management**: Add/remove user permissions in batch operations
- **System Column Filtering**: Automatically strips SharePoint system and metadata columns
- **Data Validation**: Field length limits, null handling, and format validation
- **Flexible Actions**: Support for Create, Update, and Delete batch operations

## Tech Stack

- **Python 3.6+**
- **requests** - HTTP client for webhook communication
- **csv, json** - Data processing and serialization

## Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/sharepoint-list-manager.git
   cd sharepoint-list-manager
   ```

2. Create and activate a virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install requests
   ```

4. Configure your Power Automate webhook URL (see Usage section)

## Usage

### Fetch CSV from SharePoint List

```bash
python fetch_csv.py OT_3
```

This fetches CSV data from the specified SharePoint list via the configured Power Automate webhook.

### Batch Process Items with Permissions

```bash
python batch_with_permissions.py
```

Reads CSV data, validates fields, filters system columns, and generates batch operations for:
- Creating new items
- Updating existing items with permission changes
- Managing user access (Add_UserIDs, Remove_UserIDs columns)

### Configuration

Set your Power Automate webhook URL as an environment variable or in the scripts. System columns are automatically filtered to prevent conflicts with SharePoint metadata.

## Notes

- Maximum field length: 2,000 characters
- Requires valid Power Automate webhook endpoint
- System/metadata columns are automatically excluded from operations