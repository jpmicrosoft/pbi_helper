# Power BI Admin API Wrapper

Comprehensive Python wrapper for Microsoft Power BI Admin REST APIs with specialized tools for Dataflow Gen2 scanning.

## Features

- ✅ **Workspace Management**: List, scan, get details, users
- ✅ **Dataset Operations**: Get datasets, datasources, refresh history
- ✅ **Report Operations**: Get reports, users, details
- ✅ **Dashboard Operations**: Get dashboards, tiles, users
- ✅ **Dataflow Operations**: Get dataflows, users
- ✅ **Dataflow Gen2 Scanner**: Single workspace scanner for Dataflow Gen2 connections
- ✅ **Dataflow Definition Extractor**: Extract Power Query M code and metadata from Dataflow Gen2
- ✅ **App Operations**: List apps, get users
- ✅ **Activity Logs**: Audit logs and activity events
- ✅ **Tenant Settings**: Check metadata scanning settings
- ✅ **Capacity Management**: Get capacities and workloads
- ✅ **Token Caching**: Automatic OAuth2 token management

## Installation

```bash
pip install requests
```

## Quick Start

```python
from powerbi_admin_api import PowerBIAdminAPI

# Initialize the API client
api = PowerBIAdminAPI(
    tenant_id="your-tenant-id",
    client_id="your-app-client-id",
    client_secret="your-app-client-secret"
)

# List all workspaces
workspaces = api.list_workspaces()
print(f"Found {len(workspaces)} workspaces")

# Scan a workspace for detailed metadata
scan_result = api.scan_workspace("workspace-id-here")
print(f"Datasource instances: {len(scan_result.get('datasourceInstances', []))}")
```

## Usage Examples

### 1. Workspace Operations

```python
# List all workspaces
all_workspaces = api.list_workspaces(top=5000)

# Get specific workspace
workspace = api.get_workspace("workspace-guid")
print(f"Workspace: {workspace['name']}")

# Get workspace users
users = api.get_workspace_users("workspace-guid")
for user in users:
    print(f"{user['emailAddress']}: {user['groupUserAccessRight']}")

# Scan workspace with full metadata
scan_result = api.scan_workspace(
    workspace_ids="workspace-guid",
    lineage=True,
    datasource_details=True,
    dataset_schema=True,
    dataset_expressions=True
)

# Scan multiple workspaces at once (max 100)
multi_scan = api.scan_workspace(
    workspace_ids=["workspace-1", "workspace-2", "workspace-3"]
)

# Get modified workspaces
from datetime import datetime, timedelta
last_week = datetime.now() - timedelta(days=7)
modified = api.get_modified_workspaces(modified_since=last_week)
```

### 2. Dataset Operations

```python
# Get all datasets in workspace
datasets = api.get_datasets_in_workspace("workspace-guid")

# Get dataset details
dataset = api.get_dataset("dataset-guid")
print(f"Dataset: {dataset['name']}")

# Get dataset users
users = api.get_dataset_users("dataset-guid")

# Get datasources (basic info)
datasources = api.get_datasources("dataset-guid")
for ds in datasources:
    print(f"Type: {ds.get('datasourceType')}")

# Get refresh history
refreshes = api.get_refresh_history("dataset-guid", top=10)
for refresh in refreshes:
    print(f"{refresh['startTime']}: {refresh['status']}")
```

### 3. Report Operations

```python
# Get all reports in workspace
reports = api.get_reports_in_workspace("workspace-guid")

# Get report details
report = api.get_report("report-guid")
print(f"Report: {report['name']}")
print(f"Dataset: {report['datasetId']}")

# Get report users
users = api.get_report_users("report-guid")
```

### 4. Dashboard Operations

```python
# Get all dashboards in workspace
dashboards = api.get_dashboards_in_workspace("workspace-guid")

# Get dashboard details
dashboard = api.get_dashboard("dashboard-guid")

# Get dashboard users
users = api.get_dashboard_users("dashboard-guid")

# Get dashboard tiles
tiles = api.get_tiles("dashboard-guid")
for tile in tiles:
    print(f"Tile: {tile.get('title')}")
```

### 5. Dataflow Operations

```python
# Get all dataflows in workspace
dataflows = api.get_dataflows_in_workspace("workspace-guid")

# Get dataflow details
dataflow = api.get_dataflow("dataflow-guid")

# Get dataflow users
users = api.get_dataflow_users("dataflow-guid")
```

### 6. App Operations

```python
# Get all apps
apps = api.get_apps()

# Get app details
app = api.get_app("app-guid")
print(f"App: {app['name']}")

# Get app users
users = api.get_app_users("app-guid")
```

### 7. Activity Logs (Audit)

```python
from datetime import datetime, timedelta

# Get activity events for last 24 hours
yesterday = datetime.now() - timedelta(days=1)
events = api.get_activity_events(start_date=yesterday)

print(f"Found {len(events)} activity events")
for event in events[:10]:
    print(f"{event['CreationTime']}: {event['Activity']} by {event.get('UserId')}")

# Filter by specific activity
events = api.get_activity_events(
    start_date=yesterday,
    activity_filter="ViewReport or EditReport"
)
```

### 8. Tenant Settings

```python
# Get all tenant settings
settings = api.get_tenant_settings()

# Check metadata scanning settings
metadata_settings = api.check_metadata_scanning_enabled()
for name, details in metadata_settings.items():
    status = "✅ ENABLED" if details['enabled'] else "❌ DISABLED"
    print(f"{status}: {name}")
```

### 9. Capacity Management

```python
# Get all capacities
capacities = api.get_capacities()
for capacity in capacities:
    print(f"{capacity['displayName']}: {capacity['sku']}")

# Get workspaces in capacity
workspaces = api.get_capacity_workspaces("capacity-guid")
```

### 10. Comprehensive Workspace Analysis

```python
# Get everything about a workspace
workspace_id = "your-workspace-guid"
details = api.get_all_workspace_details(workspace_id)

print(f"Workspace: {details['workspace']['name']}")
print(f"Users: {len(details['users'])}")
print(f"Datasets: {len(details['datasets'])}")
print(f"Reports: {len(details['reports'])}")
print(f"Dashboards: {len(details['dashboards'])}")
print(f"Dataflows: {len(details['dataflows'])}")

# Save to JSON
api.save_to_json(details, "workspace_details.json")
```

### 11. Advanced Scanning with Error Handling

```python
import time
from typing import List

def scan_all_workspaces(api: PowerBIAdminAPI) -> List[Dict]:
    """Scan all workspaces in batches of 100."""
    workspaces = api.list_workspaces()
    workspace_ids = [ws['id'] for ws in workspaces]
    
    results = []
    batch_size = 100
    
    for i in range(0, len(workspace_ids), batch_size):
        batch = workspace_ids[i:i+batch_size]
        print(f"Scanning batch {i//batch_size + 1}: {len(batch)} workspaces")
        
        try:
            scan_result = api.scan_workspace(
                workspace_ids=batch,
                max_wait=900  # 15 minutes for large batches
            )
            results.append(scan_result)
            
            # Save each batch
            api.save_to_json(scan_result, f"scan_batch_{i//batch_size + 1}.json")
            
            # Rate limiting: wait between batches
            if i + batch_size < len(workspace_ids):
                time.sleep(60)  # Wait 1 minute between batches
                
        except Exception as e:
            print(f"Error scanning batch {i//batch_size + 1}: {e}")
            continue
    
    return results

# Execute
api = PowerBIAdminAPI(tenant_id="...", client_id="...", client_secret="...")
all_scans = scan_all_workspaces(api)
print(f"Completed {len(all_scans)} successful scans")
```

## Configuration Requirements

### Service Principal Setup

1. **Create Azure AD App Registration**
   - Go to Azure Portal → Azure Active Directory → App registrations
   - Create new registration
   - Note the Application (client) ID and Tenant ID

2. **Create Client Secret**
   - In your app registration → Certificates & secrets
   - Create new client secret
   - Note the secret value immediately

3. **API Permissions**
   - Add permissions: `https://analysis.windows.net/powerbi/api`
   - Grant admin consent

4. **Power BI Admin Settings**
   - Add Service Principal to Power BI Admin role
   - Enable "Allow service principals to use read-only admin APIs"
   - Enable "Enhance admin APIs responses with detailed metadata" (for datasourceInstances)

### Tenant Settings for Full Metadata

For complete `datasourceInstances` data:
- ✅ **Allow service principals to use read-only admin APIs** - ENABLED
- ✅ **Enhance admin APIs responses with detailed metadata** - ENABLED
- ✅ **Enhance admin APIs responses with DAX and mashup expressions** - ENABLED

## API Reference

### Class: `PowerBIAdminAPI`

#### Workspace Methods
- `list_workspaces(top, skip, filter_expr)` - List all workspaces
- `get_workspace(workspace_id)` - Get workspace details
- `get_workspace_users(workspace_id)` - Get workspace users
- `scan_workspace(workspace_ids, ...)` - Scan workspace(s) for metadata
- `get_modified_workspaces(modified_since, ...)` - Get modified workspaces

#### Dataset Methods
- `get_datasets_in_workspace(workspace_id)` - List datasets
- `get_dataset(dataset_id)` - Get dataset details
- `get_dataset_users(dataset_id)` - Get dataset users
- `get_datasources(dataset_id)` - Get datasources
- `get_refresh_history(dataset_id, top)` - Get refresh history

#### Report Methods
- `get_reports_in_workspace(workspace_id)` - List reports
- `get_report(report_id)` - Get report details
- `get_report_users(report_id)` - Get report users

#### Dashboard Methods
- `get_dashboards_in_workspace(workspace_id)` - List dashboards
- `get_dashboard(dashboard_id)` - Get dashboard details
- `get_dashboard_users(dashboard_id)` - Get dashboard users
- `get_tiles(dashboard_id)` - Get dashboard tiles

#### Dataflow Methods
- `get_dataflows_in_workspace(workspace_id)` - List dataflows
- `get_dataflow(dataflow_id)` - Get dataflow details
- `get_dataflow_users(dataflow_id)` - Get dataflow users

#### App Methods
- `get_apps(top)` - List all apps
- `get_app(app_id)` - Get app details
- `get_app_users(app_id)` - Get app users

#### Activity Methods
- `get_activity_events(start_date, end_date, ...)` - Get activity logs

#### Tenant Methods
- `get_tenant_settings()` - Get all tenant settings
- `check_metadata_scanning_enabled()` - Check metadata scanning status

#### Capacity Methods
- `get_capacities()` - List all capacities
- `get_capacity_workspaces(capacity_id)` - Get capacity workspaces

#### Utility Methods
- `save_to_json(data, filepath, indent)` - Save data to JSON file
- `get_all_workspace_details(workspace_id)` - Get comprehensive workspace info

## Rate Limits

Microsoft Power BI Admin API rate limits:
- **Scanner API**: 500 requests/hour tenant-wide, 16 concurrent max
- **Other Admin APIs**: Varies by endpoint, generally 600 requests/hour

## Error Handling

The wrapper raises standard HTTP exceptions:

```python
try:
    workspace = api.get_workspace("invalid-id")
except requests.exceptions.HTTPError as e:
    print(f"HTTP Error: {e}")
except requests.exceptions.RequestException as e:
    print(f"Request Error: {e}")
```

## Best Practices

1. **Token Caching**: The wrapper automatically caches and reuses tokens
2. **Batch Scanning**: Scan up to 100 workspaces at once to minimize API calls
3. **Rate Limiting**: Add delays between large batch operations
4. **Error Handling**: Always wrap API calls in try-except blocks
5. **Save Results**: Save scan results immediately to avoid data loss on timeout

## Single Workspace Dataflow Gen2 Scanner

The `single_workspace_dataflow_scanner.py` script provides a targeted approach to scanning individual workspaces for Dataflow Gen2 and Dataset metadata. This is ideal for:
- **Avoiding rate limits** when dealing with large tenants (247k+ workspaces)
- **Focused analysis** of specific workspaces
- **Detailed metadata extraction** including datasets, dataflows, and datasource instances
- **Automatic dual file output** with both processed and raw scan results

### Features

- ✅ **Extracts Datasets**: Tables, columns, measures, relationships, expressions, roles
- ✅ **Extracts Dataflows**: Dataflow Gen2 objects with tables and connections
- ✅ **Extracts Datasource Instances**: All datasource connection details
- ✅ **Dual Output**: Processed JSON + Raw API response
- ✅ **Auto Filename Generation**: Workspace name + timestamp
- ✅ **Flexible Output**: Local files or Fabric lakehouse
- ✅ **No Configuration File Edits**: Pass all parameters at runtime

### Usage

**All parameters are passed when calling the function - no need to edit the file!**

#### Basic Usage (Local Python)

```python
from single_workspace_dataflow_scanner import scan_workspace_for_dataflows

# Run with all parameters
results = scan_workspace_for_dataflows(
    workspace_id="your-workspace-guid",
    tenant_id="your-tenant-id",
    client_id="your-client-id",
    client_secret="your-client-secret",
    output_directory=None,  # Current directory
    print_to_console=True
)
```

#### Save to Custom Directory

```python
results = scan_workspace_for_dataflows(
    workspace_id="abc123-def456-789...",
    tenant_id="tenant-id",
    client_id="client-id",
    client_secret="client-secret",
    output_directory="C:/pbi_scans/dataflows",
    print_to_console=True
)

# Outputs:
# C:/pbi_scans/dataflows/workspace_Sales_Analytics_20251212_143000_dataflows.json (processed)
# C:/pbi_scans/dataflows/workspace_Sales_Analytics_20251212_143000_raw_scan.json (raw)
```

#### Fabric Notebook (Lakehouse)

```python
# PySpark/Fabric notebook
results = scan_workspace_for_dataflows(
    workspace_id="workspace-guid",
    tenant_id="tenant-id",
    client_id="client-id",
    client_secret="client-secret",
    output_directory="/lakehouse/default/Files/pbi_scans",
    print_to_console=True
)

# Outputs to lakehouse:
# /lakehouse/default/Files/pbi_scans/workspace_Sales_Analytics_20251212_143000_dataflows.json
# /lakehouse/default/Files/pbi_scans/workspace_Sales_Analytics_20251212_143000_raw_scan.json
```

#### Azure Blob Storage Path (abfss)

```python
results = scan_workspace_for_dataflows(
    workspace_id="workspace-guid",
    tenant_id="tenant-id",
    client_id="client-id",
    client_secret="client-secret",
    output_directory="abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse.Lakehouse/Files/scans",
    print_to_console=True
)
```

### Output Files

The scanner generates **TWO files** per run:

#### 1. Processed Results: `workspace_{Name}_{Timestamp}_dataflows.json`

Structured, processed metadata:

```json
{
  "scan_timestamp": "2025-12-12T14:30:00.123456",
  "workspace_id": "abc123...",
  "workspace_name": "Sales Analytics",
  "dataset_count": 2,
  "dataflow_count": 3,
  "total_connections": 5,
  
  "datasets": [
    {
      "name": "Sales Dataset",
      "id": "dataset-guid",
      "target_storage_mode": "Import",
      "tables": [
        {
          "name": "Customers",
          "column_count": 15,
          "measure_count": 3,
          "columns": [
            {"name": "CustomerID", "data_type": "Int64", "is_hidden": false}
          ],
          "measures": [
            {"name": "Total Sales", "expression": "SUM([Amount])"}
          ]
        }
      ],
      "relationships": [...],
      "expressions": [...],
      "datasource_usages": [...]
    }
  ],
  
  "dataflows": [
    {
      "name": "Sales ETL",
      "id": "dataflow-guid",
      "tables": ["Customers", "Orders"],
      "connections": [
        {
          "datasource_type": "AzureSqlDatabase",
          "connection_details": {
            "server": "sqlserver.database.windows.net",
            "database": "SalesDB"
          },
          "gateway_id": null
        }
      ]
    }
  ],
  
  "datasource_instances": [
    {
      "datasourceType": "Sql",
      "connectionDetails": {...},
      "datasourceId": "guid",
      "gatewayId": "gateway-guid"
    }
  ],
  
  "connection_breakdown": {
    "AzureSqlDatabase": 2,
    "SharePointList": 1
  }
}
```

#### 2. Raw Scan Results: `workspace_{Name}_{Timestamp}_raw_scan.json`

Complete, unmodified API response from the Scanner API - matches the structure from [Microsoft's example](https://learn.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-get-scan-result#example).

### Console Output

```
============================================================
SCANNING WORKSPACE FOR DATAFLOW GEN2 CONNECTIONS
============================================================
Workspace ID: abc123-def456-789...
Started: 2025-12-12 14:30:00

🔄 Initiating workspace scan...
✅ Scan completed!

📊 Found 2 dataset(s) and 3 dataflow(s) in workspace

Dataset: Sales Dataset
  ID: dataset-123
  Configured by: user@contoso.com
  Storage Mode: Import
  Tables: 5
    • Customers (Hidden: False)
      - Columns: 15
      - Measures: 3
  Expressions/Parameters: 2
    • DatabaseParam - Database connection parameter
  Datasource Usages: 1

Dataflow: Sales ETL
  ID: dataflow-456
  Configured by: user@contoso.com
  Tables: 3
    • Customers
    • Orders
    • Products
  Connections: 2

    📡 Connection Type: AzureSqlDatabase
       Details: {
         "server": "sqlserver.database.windows.net",
         "database": "SalesDB"
       }
       Gateway ID: None

✅ Saved processed results to lakehouse: /lakehouse/.../workspace_Sales_Analytics_20251212_143000_dataflows.json
✅ Saved raw scan results to lakehouse: /lakehouse/.../workspace_Sales_Analytics_20251212_143000_raw_scan.json

======================================================================
📋 SCAN SUMMARY
======================================================================
Workspace: Sales Analytics
Datasets: 2
Dataflows: 3
Total Connections: 5
Datasource Instances: 8
Misconfigured Datasources: 0

Connection Breakdown:
  • AzureSqlDatabase: 2
  • SharePointList: 1
  • AzureBlobStorage: 2
```

### Function Parameters

```python
def scan_workspace_for_dataflows(
    workspace_id: str,           # Required: Workspace GUID
    tenant_id: str,              # Required: Azure AD tenant ID
    client_id: str,              # Required: Service Principal client ID
    client_secret: str,          # Required: Service Principal secret
    output_directory: str = None, # Optional: Output path (None = current directory)
    print_to_console: bool = True # Optional: Print detailed output
)
```

### Key Features

- **No File Editing**: All configuration passed as parameters
- **Dual Output**: Processed + raw API response
- **Automatic Filenames**: `workspace_{Name}_{Timestamp}_dataflows.json` and `_raw_scan.json`
- **Flexible Storage**: Local files, custom paths, or Fabric lakehouse
- **Full Metadata**: Datasets, dataflows, datasources, expressions, relationships
- **Lakehouse Support**: Works with `/lakehouse/` or `abfss://` paths
- **Rate Limit Friendly**: 1 API call per workspace (vs 2,470 for 247k workspaces)
- **Fabric Compatible**: Works in local Python and Fabric notebooks

### When to Use

- ✅ **Large tenants** (100k+ workspaces) - avoid rate limits
- ✅ **Targeted analysis** - focus on specific workspaces
- ✅ **Full metadata extraction** - datasets, dataflows, datasources
- ✅ **Raw API data needed** - get both processed and raw results
- ✅ **Repeated scans** - scan different workspaces without hitting limits
- ✅ **Fabric integration** - save directly to lakehouse

---

## Dataflow Definition Extractor

**File**: `dataflow_definition_extractor.py`

Extracts complete Dataflow Gen2 definitions including Power Query M code, query metadata, and platform settings from all dataflows in a workspace using the Microsoft Fabric Dataflow Definition API.

### Key Features

- ✅ **Power Query M Code**: Automatically decoded from base64 (mashup.pq)
- ✅ **Query Metadata**: Full query configuration and settings (queryMetadata.json)
- ✅ **Platform Metadata**: Platform-specific settings (.platform)
- ✅ **Automatic Decoding**: Base64 payloads decoded automatically
- ✅ **Individual Files**: Each dataflow saved separately with full workspace context
- ✅ **Summary File**: Consolidated summary of all extractions
- ✅ **Console Display**: View decoded M code and metadata during extraction
- ✅ **Flexible Output**: Local files or Fabric lakehouse
- ✅ **No Configuration File Edits**: Pass all parameters at runtime
- ✅ **LRO Handling**: Automatic polling for long-running operations

### Usage

**All parameters are passed when calling the function - no need to edit the file!**

#### Basic Usage (Local Python)

```python
from dataflow_definition_extractor import extract_dataflow_definitions

# Run with all parameters
results = extract_dataflow_definitions(
    workspace_id="your-workspace-guid",
    tenant_id="your-tenant-id",
    client_id="your-client-id",
    client_secret="your-client-secret",
    output_directory=None,  # Current directory
    print_to_console=True
)
```

#### Fabric Notebook (Lakehouse)

```python
# PySpark/Fabric notebook
results = extract_dataflow_definitions(
    workspace_id="workspace-guid",
    tenant_id="tenant-id",
    client_id="client-id",
    client_secret="client-secret",
    output_directory="/lakehouse/default/Files/dataflow_definitions",
    print_to_console=True
)

# Outputs to lakehouse:
# /lakehouse/default/Files/dataflow_definitions/dataflow_Sales_ETL_20251212_143000_definition.json
# /lakehouse/default/Files/dataflow_definitions/workspace_Sales_Analytics_20251212_143000_definitions_summary.json
```

### Output Files

The extractor generates **individual files per dataflow** plus a **summary file**:

#### 1. Individual Dataflow Files: `dataflow_{Name}_{Timestamp}_definition.json`

Each file contains full workspace context and decoded definition:

```json
{
  "workspace_id": "abc123-def456-789...",
  "workspace_name": "Sales Analytics",
  "dataflow_id": "dataflow-guid",
  "dataflow_name": "Sales ETL",
  "configured_by": "user@contoso.com",
  "modified_date": "2025-12-12T14:30:00Z",
  "extraction_timestamp": "2025-12-12T15:00:00",
  
  "definition": {
    "parts": [
      {
        "path": "mashup.pq",
        "payload": "section Section1;\n\nshared Customers = let\n    Source = Sql.Database(\"server.database.windows.net\", \"SalesDB\"),\n    CustomersTable = Source{[Schema=\"dbo\",Item=\"Customers\"]}[Data],\n    FilteredRows = Table.SelectRows(CustomersTable, each [Country] = \"USA\")\nin\n    FilteredRows;\n\nshared Orders = let\n    Source = Sql.Database(\"server.database.windows.net\", \"SalesDB\"),\n    OrdersTable = Source{[Schema=\"dbo\",Item=\"Orders\"]}[Data]\nin\n    OrdersTable;",
        "payloadType": "DecodedText"
      },
      {
        "path": "queryMetadata.json",
        "payload": {
          "formatVersion": "202502",
          "name": "Sales ETL",
          "description": "Sales data transformation",
          "queries": [
            {
              "name": "Customers",
              "queryGroup": null,
              "loadEnabled": true
            },
            {
              "name": "Orders",
              "queryGroup": null,
              "loadEnabled": true
            }
          ]
        },
        "payloadType": "DecodedJSON"
      },
      {
        "path": ".platform",
        "payload": {
          "version": "1.0",
          "dataflowRefreshSchedule": {...}
        },
        "payloadType": "DecodedJSON"
      }
    ]
  }
}
```

#### 2. Summary File: `workspace_{WorkspaceName}_{Timestamp}_definitions_summary.json`

```json
{
  "extraction_timestamp": "2025-12-12T15:00:00",
  "workspace_id": "abc123...",
  "workspace_name": "Sales Analytics",
  "total_dataflows": 3,
  "successful_extractions": 3,
  "failed_extractions": 0,
  
  "dataflows": [
    {
      "name": "Sales ETL",
      "id": "dataflow-guid",
      "status": "success",
      "file_saved": "dataflow_Sales_ETL_20251212_150000_definition.json",
      "parts_count": 3,
      "parts": ["mashup.pq", "queryMetadata.json", ".platform"]
    },
    {
      "name": "Marketing Data",
      "id": "dataflow-guid-2",
      "status": "success",
      "file_saved": "dataflow_Marketing_Data_20251212_150030_definition.json",
      "parts_count": 3,
      "parts": ["mashup.pq", "queryMetadata.json", ".platform"]
    }
  ]
}
```

### Console Output

```
============================================================
EXTRACTING DATAFLOW DEFINITIONS FROM WORKSPACE
============================================================
Workspace ID: abc123-def456-789...
Started: 2025-12-12 15:00:00

🔄 Scanning workspace to find dataflows...
✅ Found 3 dataflow(s)

======================================================================
📄 DATAFLOW 1/3: Sales ETL
======================================================================
Dataflow ID: dataflow-123
Configured by: user@contoso.com
Modified: 2025-12-11T10:30:00Z

🔄 Extracting definition...
✅ Definition extracted successfully!

--- Power Query M Code (mashup.pq) ---
section Section1;

shared Customers = let
    Source = Sql.Database("server.database.windows.net", "SalesDB"),
    CustomersTable = Source{[Schema="dbo",Item="Customers"]}[Data],
    FilteredRows = Table.SelectRows(CustomersTable, each [Country] = "USA")
in
    FilteredRows;

shared Orders = let
    Source = Sql.Database("server.database.windows.net", "SalesDB"),
    OrdersTable = Source{[Schema="dbo",Item="Orders"]}[Data]
in
    OrdersTable;

--- Query Metadata (queryMetadata.json) ---
{
  "formatVersion": "202502",
  "name": "Sales ETL",
  "queries": [
    {"name": "Customers", "loadEnabled": true},
    {"name": "Orders", "loadEnabled": true}
  ]
}

--- Platform Metadata (.platform) ---
{"version": "1.0", "dataflowRefreshSchedule": {...}}

✅ Saved to: dataflow_Sales_ETL_20251212_150000_definition.json

======================================================================
📋 EXTRACTION SUMMARY
======================================================================
Workspace: Sales Analytics
Total Dataflows: 3
Successful: 3
Failed: 0

✅ Saved summary to: workspace_Sales_Analytics_20251212_150000_definitions_summary.json
```

### Function Parameters

```python
def extract_dataflow_definitions(
    workspace_id: str,           # Required: Workspace GUID
    tenant_id: str,              # Required: Azure AD tenant ID
    client_id: str,              # Required: Service Principal client ID
    client_secret: str,          # Required: Service Principal secret
    output_directory: str = None, # Optional: Output path (None = current directory)
    print_to_console: bool = True # Optional: Print detailed output with M code
)
```

### API Details

- **Endpoint**: `POST https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/dataflows/{dataflowId}/getDefinition`
- **Authentication**: OAuth2 token (scope: `https://analysis.windows.net/powerbi/api/.default`)
- **Required Permissions**: `Dataflow.ReadWrite.All` or `Item.ReadWrite.All`
- **Response Pattern**: Long Running Operation (LRO)
  - Initial response: `202 Accepted` with `Location` header
  - Polling: GET to Location URL every 5 seconds
  - Final response: `200 OK` with definition
  - Timeout: 300 seconds (configurable)

### Direct API Usage

```python
from powerbi_admin_api import PowerBIAdminAPI

api = PowerBIAdminAPI(tenant_id, client_id, client_secret)

# Get definition with automatic decoding (default)
definition = api.get_dataflow_definition(
    workspace_id="workspace-guid",
    dataflow_id="dataflow-guid",
    decode_payloads=True,  # Default: automatically decode base64
    timeout=300,           # Max wait time for LRO
    poll_interval=5        # Seconds between polls
)

# Access decoded M code
for part in definition['definition']['parts']:
    if part['path'] == 'mashup.pq':
        m_code = part['payload']  # Already decoded text
        print(m_code)
    elif part['path'] == 'queryMetadata.json':
        metadata = part['payload']  # Already parsed as dict
        print(f"Format Version: {metadata['formatVersion']}")

# Get raw base64 (no decoding)
raw_definition = api.get_dataflow_definition(
    workspace_id="workspace-guid",
    dataflow_id="dataflow-guid",
    decode_payloads=False  # Keep base64 payloads
)
```

### Definition Parts

| File | Content | Decoded As |
|------|---------|------------|
| `mashup.pq` | Power Query M code (transformation logic) | Text string |
| `queryMetadata.json` | Query configuration, names, load settings | Python dict |
| `.platform` | Platform metadata, refresh schedules | Python dict |

### When to Use

- ✅ **Analyze transformation logic** - Extract and review Power Query M code
- ✅ **Documentation** - Document dataflow transformations and queries
- ✅ **Migration/backup** - Archive dataflow definitions
- ✅ **Troubleshooting** - Debug M code and query configurations
- ✅ **Version control** - Track changes to dataflow definitions over time
- ✅ **Metadata analysis** - Analyze query metadata and platform settings
- ✅ **Workspace documentation** - Extract all dataflows for workspace documentation

---

## License

This is a utility wrapper for Microsoft Power BI Admin REST APIs. See Microsoft's API documentation for official terms and conditions.

## Resources

- [Power BI Admin REST API Documentation](https://learn.microsoft.com/en-us/rest/api/power-bi/admin)
- [Fabric Dataflow Definition API](https://learn.microsoft.com/en-us/rest/api/fabric/dataflow/items/get-dataflow-definition)
- [Scanner API Setup Guide](https://learn.microsoft.com/en-us/power-bi/admin/service-admin-metadata-scanning)
- [Scanner API Get Scan Result Example](https://learn.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-get-scan-result)
- [Service Principal Setup](https://learn.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal)
