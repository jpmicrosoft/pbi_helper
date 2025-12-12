# Power BI Admin API Wrapper

Comprehensive Python wrapper for Microsoft Power BI Admin REST APIs with specialized tools for Dataflow Gen2 scanning.

## Features

- ✅ **Workspace Management**: List, scan, get details, users
- ✅ **Dataset Operations**: Get datasets, datasources, refresh history
- ✅ **Report Operations**: Get reports, users, details
- ✅ **Dashboard Operations**: Get dashboards, tiles, users
- ✅ **Dataflow Operations**: Get dataflows, users
- ✅ **Dataflow Gen2 Scanner**: Single workspace scanner for Dataflow Gen2 connections
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

The `single_workspace_dataflow_scanner.py` script provides a targeted approach to scanning individual workspaces for Dataflow Gen2 connections. This is ideal for:
- **Avoiding rate limits** when dealing with large tenants (247k+ workspaces)
- **Focused analysis** of specific workspaces
- **Detailed connection extraction** with automatic file naming

### Configuration

```python
# Required credentials
TENANT_ID = "your-tenant-id"
CLIENT_ID = "your-client-id"
CLIENT_SECRET = "your-client-secret"

# Target workspace
WORKSPACE_ID = "your-workspace-id"

# Output directory (None = current directory)
OUTPUT_DIRECTORY = None  # Or "C:/output" or "/lakehouse/default/Files/pbi_scans"
```

### Usage Examples

#### Example 1: Scan Single Workspace (Local)

```python
from single_workspace_dataflow_scanner import scan_workspace_for_dataflows

# Scan workspace - saves to current directory
scan_workspace_for_dataflows("workspace-guid-here")

# Output file: workspace_Sales_Analytics_20251212_143000_dataflows.json
```

#### Example 2: Save to Specific Directory

```python
# Update configuration
OUTPUT_DIRECTORY = "C:/pbi_scans/dataflows"

# Scan workspace
scan_workspace_for_dataflows("workspace-guid-here")

# Output: C:/pbi_scans/dataflows/workspace_Sales_Analytics_20251212_143000_dataflows.json
```

#### Example 3: Save to Fabric Lakehouse

```python
# Update configuration
OUTPUT_DIRECTORY = "/lakehouse/default/Files/pbi_scans"

# Scan workspace in Fabric notebook
scan_workspace_for_dataflows("workspace-guid-here")

# Output: /lakehouse/default/Files/pbi_scans/workspace_Sales_Analytics_20251212_143000_dataflows.json
```

#### Example 4: Run as Standalone Script

```bash
# Update credentials in the file, then run:
python single_workspace_dataflow_scanner.py
```

### Output Format

The scanner generates a comprehensive JSON file with auto-generated filename:

**Filename Pattern**: `workspace_{WorkspaceName}_{YYYYMMDD_HHMMSS}_dataflows.json`

**Example**: `workspace_Sales_Analytics_20251212_143000_dataflows.json`

```json
{
  "scan_timestamp": "2025-12-12T14:30:00.123456",
  "workspace_id": "abc123...",
  "workspace_name": "Sales Analytics",
  "dataflow_count": 3,
  "total_connections": 5,
  "dataflows": [
    {
      "name": "Sales ETL",
      "id": "dataflow-guid",
      "tables": ["Customers", "Orders", "Products"],
      "connections": [
        {
          "dataflow_name": "Sales ETL",
          "dataflow_id": "dataflow-guid",
          "workspace_id": "workspace-guid",
          "workspace_name": "Sales Analytics",
          "datasource_type": "AzureSqlDatabase",
          "connection_details": {
            "server": "sqlserver.database.windows.net",
            "database": "SalesDB"
          },
          "gateway_id": null,
          "datasource_id": "datasource-guid"
        }
      ]
    }
  ],
  "all_connections": [...],
  "connection_breakdown": {
    "AzureSqlDatabase": 2,
    "SharePointList": 1,
    "AzureBlobStorage": 2
  }
}
```

### Console Output

```
Scanning workspace: abc123...
✅ Scan initiated. Scan ID: scan-123...
⏳ Scan in progress... (attempt 1/60)
✅ Scan completed successfully

Processing dataflows...
  📊 Dataflow: Sales ETL (3 tables, 2 connections)
  📊 Dataflow: Finance Import (2 tables, 1 connection)
  📊 Dataflow: HR Data (1 table, 2 connections)

✅ Saved to: workspace_Sales_Analytics_20251212_143000_dataflows.json

============================================================
SUMMARY
============================================================
Workspace: Sales Analytics
Dataflows found: 3
Total connections: 5

Connection Breakdown:
  - AzureSqlDatabase: 2
  - SharePointList: 1
  - AzureBlobStorage: 2
```

### Key Features

- **Automatic Filename Generation**: Workspace name + timestamp
- **Flexible Output Location**: Current directory, custom path, or Fabric lakehouse
- **Detailed Connection Info**: Type, server, database, gateway, datasource ID
- **Flattened Connections**: `all_connections` array for easy filtering
- **Connection Type Summary**: Breakdown by datasource type
- **Rate Limit Friendly**: Scan one workspace at a time (1 API call vs 2,470 for 247k workspaces)
- **Fabric Compatible**: Works in local Python and Fabric notebooks

### When to Use

- ✅ **Large tenants** (100k+ workspaces) - avoid rate limits
- ✅ **Targeted analysis** - focus on specific workspaces
- ✅ **Dataflow Gen2 connections** - extract connection metadata
- ✅ **Repeated scans** - scan different workspaces without hitting limits
- ✅ **Fabric integration** - save directly to lakehouse

## License

This is a utility wrapper for Microsoft Power BI Admin REST APIs. See Microsoft's API documentation for official terms and conditions.

## Resources

- [Power BI Admin REST API Documentation](https://learn.microsoft.com/en-us/rest/api/power-bi/admin)
- [Scanner API Setup Guide](https://learn.microsoft.com/en-us/power-bi/admin/service-admin-metadata-scanning)
- [Service Principal Setup](https://learn.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal)
