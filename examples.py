"""
Example: Using PowerBIAdminAPI Wrapper
Demonstrates common use cases for the Power BI Admin API wrapper
"""

from powerbi_admin_api import PowerBIAdminAPI
from datetime import datetime, timedelta
import json

# ==================== CONFIGURATION ====================

TENANT_ID = "your-tenant-id"
CLIENT_ID = "your-app-client-id"
CLIENT_SECRET = "your-app-client-secret"

# Initialize API client
api = PowerBIAdminAPI(
    tenant_id=TENANT_ID,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET
)


# ==================== EXAMPLE 1: Check Tenant Configuration ====================

def check_tenant_setup():
    """Verify tenant is configured for metadata scanning."""
    print("\n" + "="*60)
    print("TENANT CONFIGURATION CHECK")
    print("="*60)
    
    settings = api.check_metadata_scanning_enabled()
    
    for name, details in settings.items():
        status = "✅ ENABLED" if details['enabled'] else "❌ DISABLED"
        print(f"{status}: {name}")
        
        if details['enabledSecurityGroups']:
            print(f"   → Applied to {len(details['enabledSecurityGroups'])} security group(s)")
    
    print("="*60)


# ==================== EXAMPLE 2: Workspace Inventory ====================

def create_workspace_inventory():
    """Create inventory of all workspaces with basic stats."""
    print("\n" + "="*60)
    print("WORKSPACE INVENTORY")
    print("="*60)
    
    workspaces = api.list_workspaces()
    print(f"Total workspaces: {len(workspaces)}\n")
    
    inventory = []
    for ws in workspaces:
        inventory.append({
            'id': ws['id'],
            'name': ws['name'],
            'type': ws.get('type'),
            'state': ws.get('state'),
            'isOnDedicatedCapacity': ws.get('isOnDedicatedCapacity', False),
            'capacityId': ws.get('capacityId')
        })
        
        print(f"📁 {ws['name']}")
        print(f"   ID: {ws['id']}")
        print(f"   Type: {ws.get('type')}")
        print(f"   Capacity: {ws.get('capacityId', 'Shared')}\n")
    
    # Save inventory
    api.save_to_json(inventory, "workspace_inventory.json")
    print(f"✅ Saved inventory to workspace_inventory.json")
    print("="*60)
    
    return inventory


# ==================== EXAMPLE 3: Find Cloud Connections ====================

def find_cloud_connections(workspace_id: str):
    """Scan workspace and extract cloud connection information."""
    print("\n" + "="*60)
    print(f"SCANNING WORKSPACE FOR CLOUD CONNECTIONS")
    print("="*60)
    
    # Scan workspace with full metadata
    scan_result = api.scan_workspace(
        workspace_ids=workspace_id,
        lineage=True,
        datasource_details=True,
        dataset_schema=True,
        dataset_expressions=True
    )
    
    # Extract datasource instances
    datasources = scan_result.get('datasourceInstances', [])
    print(f"\n✅ Found {len(datasources)} datasource instance(s)")
    
    cloud_connections = []
    for ds in datasources:
        ds_type = ds.get('datasourceType', 'Unknown')
        conn_details = ds.get('connectionDetails', {})
        
        print(f"\n📊 Datasource Type: {ds_type}")
        print(f"   Connection Details: {json.dumps(conn_details, indent=3)}")
        print(f"   Datasource ID: {ds.get('datasourceId')}")
        print(f"   Gateway ID: {ds.get('gatewayId', 'None')}")
        
        cloud_connections.append({
            'type': ds_type,
            'connectionDetails': conn_details,
            'datasourceId': ds.get('datasourceId'),
            'gatewayId': ds.get('gatewayId')
        })
    
    # Save full scan result
    api.save_to_json(scan_result, f"workspace_{workspace_id}_scan.json")
    print(f"\n✅ Saved full scan to workspace_{workspace_id}_scan.json")
    print("="*60)
    
    return cloud_connections


# ==================== EXAMPLE 4: User Access Audit ====================

def audit_workspace_access(workspace_id: str):
    """Audit all users with access to workspace and its artifacts."""
    print("\n" + "="*60)
    print("WORKSPACE ACCESS AUDIT")
    print("="*60)
    
    workspace = api.get_workspace(workspace_id)
    print(f"\nWorkspace: {workspace['name']}\n")
    
    # Get workspace users
    ws_users = api.get_workspace_users(workspace_id)
    print(f"Workspace Users: {len(ws_users)}")
    for user in ws_users:
        print(f"  • {user.get('emailAddress', user.get('displayName'))}: {user['groupUserAccessRight']}")
    
    # Get datasets and their users
    datasets = api.get_datasets_in_workspace(workspace_id)
    print(f"\nDatasets: {len(datasets)}")
    for dataset in datasets:
        print(f"\n  📊 {dataset['name']}")
        try:
            ds_users = api.get_dataset_users(dataset['id'])
            for user in ds_users:
                print(f"     → {user.get('emailAddress')}: {user.get('datasetUserAccessRight')}")
        except:
            print(f"     → Could not retrieve users")
    
    # Get reports and their users
    reports = api.get_reports_in_workspace(workspace_id)
    print(f"\nReports: {len(reports)}")
    for report in reports:
        print(f"\n  📄 {report['name']}")
        try:
            rpt_users = api.get_report_users(report['id'])
            for user in rpt_users:
                print(f"     → {user.get('emailAddress')}: {user.get('reportUserAccessRight')}")
        except:
            print(f"     → Could not retrieve users")
    
    print("="*60)


# ==================== EXAMPLE 5: Activity Log Analysis ====================

def analyze_recent_activity():
    """Analyze recent Power BI activity."""
    print("\n" + "="*60)
    print("RECENT ACTIVITY ANALYSIS (Last 24 Hours)")
    print("="*60)
    
    yesterday = datetime.now() - timedelta(days=1)
    events = api.get_activity_events(start_date=yesterday)
    
    print(f"\nTotal Events: {len(events)}\n")
    
    # Count by activity type
    activity_counts = {}
    user_counts = {}
    
    for event in events:
        activity = event.get('Activity', 'Unknown')
        user = event.get('UserId', 'Unknown')
        
        activity_counts[activity] = activity_counts.get(activity, 0) + 1
        user_counts[user] = user_counts.get(user, 0) + 1
    
    # Top activities
    print("Top 10 Activities:")
    sorted_activities = sorted(activity_counts.items(), key=lambda x: x[1], reverse=True)
    for activity, count in sorted_activities[:10]:
        print(f"  {count:4d} - {activity}")
    
    # Top users
    print("\nTop 10 Active Users:")
    sorted_users = sorted(user_counts.items(), key=lambda x: x[1], reverse=True)
    for user, count in sorted_users[:10]:
        print(f"  {count:4d} - {user}")
    
    # Save detailed events
    api.save_to_json(events, "activity_events_24h.json")
    print(f"\n✅ Saved detailed events to activity_events_24h.json")
    print("="*60)


# ==================== EXAMPLE 6: Dataset Refresh Monitoring ====================

def monitor_dataset_refreshes(workspace_id: str):
    """Monitor refresh status of all datasets in workspace."""
    print("\n" + "="*60)
    print("DATASET REFRESH MONITORING")
    print("="*60)
    
    datasets = api.get_datasets_in_workspace(workspace_id)
    print(f"\nFound {len(datasets)} dataset(s)\n")
    
    refresh_summary = []
    
    for dataset in datasets:
        print(f"📊 {dataset['name']}")
        print(f"   ID: {dataset['id']}")
        
        try:
            # Get last 5 refreshes
            refreshes = api.get_refresh_history(dataset['id'], top=5)
            
            if refreshes:
                latest = refreshes[0]
                print(f"   Last Refresh: {latest.get('startTime')}")
                print(f"   Status: {latest.get('status')}")
                
                if latest.get('status') == 'Failed':
                    print(f"   ⚠️  Error: {latest.get('serviceExceptionJson', 'Unknown error')}")
                
                refresh_summary.append({
                    'datasetId': dataset['id'],
                    'datasetName': dataset['name'],
                    'lastRefresh': latest.get('startTime'),
                    'status': latest.get('status'),
                    'refreshType': latest.get('refreshType')
                })
            else:
                print(f"   No refresh history available")
                
        except Exception as e:
            print(f"   ❌ Error retrieving refresh history: {e}")
        
        print()
    
    # Save summary
    api.save_to_json(refresh_summary, "refresh_summary.json")
    print(f"✅ Saved refresh summary to refresh_summary.json")
    print("="*60)


# ==================== EXAMPLE 7: Comprehensive Workspace Report ====================

def generate_workspace_report(workspace_id: str):
    """Generate comprehensive report for a workspace."""
    print("\n" + "="*60)
    print("COMPREHENSIVE WORKSPACE REPORT")
    print("="*60)
    
    # Get all details
    details = api.get_all_workspace_details(workspace_id)
    
    workspace = details['workspace']
    print(f"\nWorkspace: {workspace['name']}")
    print(f"ID: {workspace['id']}")
    print(f"Type: {workspace.get('type')}")
    print(f"State: {workspace.get('state')}")
    print(f"Capacity: {workspace.get('capacityId', 'Shared')}")
    
    print(f"\n📊 Summary:")
    print(f"  Users: {len(details['users'])}")
    print(f"  Datasets: {len(details['datasets'])}")
    print(f"  Reports: {len(details['reports'])}")
    print(f"  Dashboards: {len(details['dashboards'])}")
    print(f"  Dataflows: {len(details['dataflows'])}")
    
    # Save comprehensive report
    api.save_to_json(details, f"workspace_{workspace_id}_full_report.json")
    print(f"\n✅ Saved full report to workspace_{workspace_id}_full_report.json")
    print("="*60)


# ==================== MAIN EXECUTION ====================

if __name__ == "__main__":
    print("\n" + "="*60)
    print("POWER BI ADMIN API - EXAMPLE USAGE")
    print("="*60)
    
    # Example 1: Check tenant setup
    check_tenant_setup()
    
    # Example 2: Create workspace inventory
    # inventory = create_workspace_inventory()
    
    # Example 3: Find cloud connections (replace with your workspace ID)
    # workspace_id = "your-workspace-guid-here"
    # cloud_connections = find_cloud_connections(workspace_id)
    
    # Example 4: Audit workspace access
    # audit_workspace_access(workspace_id)
    
    # Example 5: Analyze recent activity
    # analyze_recent_activity()
    
    # Example 6: Monitor dataset refreshes
    # monitor_dataset_refreshes(workspace_id)
    
    # Example 7: Generate comprehensive workspace report
    # generate_workspace_report(workspace_id)
    
    print("\n✅ Examples completed!")
    print("\nUncomment specific examples in the code to run them.")
    print("Make sure to replace placeholder IDs with your actual workspace/dataset IDs.\n")
