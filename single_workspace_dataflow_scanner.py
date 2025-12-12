"""
Single Workspace Dataflow Gen2 Scanner
Scans one workspace at a time to extract Dataflow Gen2 connection information
"""

from powerbi_admin_api import PowerBIAdminAPI
import json
import time
from datetime import datetime

# ============================================================
# CONFIGURATION
# ============================================================

TENANT_ID = "your-tenant-id"
CLIENT_ID = "your-client-id"
CLIENT_SECRET = "your-client-secret"

# Target workspace
WORKSPACE_ID = "your-workspace-id"

# Output settings
OUTPUT_DIRECTORY = None  # Set to directory path like "C:/output" or "/lakehouse/default/Files/pbi_scans" (None = current directory)
PRINT_TO_CONSOLE = True

# ============================================================
# SCAN SINGLE WORKSPACE
# ============================================================

def scan_workspace_for_dataflows(workspace_id: str):
    """
    Scan a single workspace and extract all Dataflow Gen2 connection information.
    
    Args:
        workspace_id: Single workspace GUID
    
    Returns:
        Dictionary with dataflow and connection details
    """
    
    # Initialize API
    api = PowerBIAdminAPI(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET
    )
    
    print(f"\n{'='*60}")
    print(f"SCANNING WORKSPACE FOR DATAFLOW GEN2 CONNECTIONS")
    print(f"{'='*60}")
    print(f"Workspace ID: {workspace_id}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Scan workspace with full metadata
    print("🔄 Initiating workspace scan...")
    
    scan_result = api.scan_workspace(
        workspace_ids=workspace_id,
        lineage=True,
        datasource_details=True,      # Required for connection details
        dataset_schema=True,
        dataset_expressions=True,      # Gets Power Query M code
        get_artifact_users=True
    )
    
    print("✅ Scan completed!\n")
    
    # Extract workspace info
    workspace_info = None
    if scan_result.get('workspaces'):
        workspace_info = scan_result['workspaces'][0]
    
    # Extract dataflows
    dataflows = scan_result.get('dataflows', [])
    
    print(f"📊 Found {len(dataflows)} dataflow(s) in workspace\n")
    
    # Process each dataflow
    dataflow_details = []
    all_connections = []
    
    for df in dataflows:
        print(f"Dataflow: {df.get('name')}")
        print(f"  ID: {df.get('objectId')}")
        print(f"  Configured by: {df.get('configuredBy')}")
        print(f"  Modified: {df.get('modifiedDateTime')}")
        
        # Extract tables
        tables = df.get('tables', [])
        print(f"  Tables: {len(tables)}")
        for table in tables:
            print(f"    • {table.get('name')}")
        
        # Extract datasource connections
        datasources = df.get('datasources', [])
        print(f"  Connections: {len(datasources)}")
        
        dataflow_connections = []
        
        for ds in datasources:
            ds_type = ds.get('datasourceType')
            conn_details = ds.get('connectionDetails', {})
            
            print(f"\n    📡 Connection Type: {ds_type}")
            print(f"       Details: {json.dumps(conn_details, indent=10)}")
            print(f"       Gateway ID: {ds.get('gatewayId', 'None')}")
            
            connection = {
                'dataflow_name': df.get('name'),
                'dataflow_id': df.get('objectId'),
                'workspace_id': workspace_id,
                'workspace_name': workspace_info.get('name') if workspace_info else 'Unknown',
                'datasource_type': ds_type,
                'connection_details': conn_details,
                'gateway_id': ds.get('gatewayId'),
                'datasource_id': ds.get('datasourceId')
            }
            
            dataflow_connections.append(connection)
            all_connections.append(connection)
        
        # Dataflow summary
        dataflow_detail = {
            'name': df.get('name'),
            'id': df.get('objectId'),
            'description': df.get('description'),
            'configured_by': df.get('configuredBy'),
            'modified_by': df.get('modifiedBy'),
            'modified_date': df.get('modifiedDateTime'),
            'table_count': len(tables),
            'tables': [t.get('name') for t in tables],
            'connection_count': len(datasources),
            'connections': dataflow_connections
        }
        
        dataflow_details.append(dataflow_detail)
        print()
    
    # Compile results
    results = {
        'scan_timestamp': datetime.now().isoformat(),
        'workspace_id': workspace_id,
        'workspace_name': workspace_info.get('name') if workspace_info else 'Unknown',
        'workspace_type': workspace_info.get('type') if workspace_info else 'Unknown',
        'capacity_id': workspace_info.get('capacityId') if workspace_info else None,
        'dataflow_count': len(dataflows),
        'total_connections': len(all_connections),
        'dataflows': dataflow_details,
        'all_connections': all_connections
    }
    
    # Create filename with workspace name and timestamp
    workspace_name_clean = results['workspace_name'].replace(' ', '_').replace('/', '_').replace('\\', '_')
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"workspace_{workspace_name_clean}_{timestamp_str}_dataflows.json"
    
    # Combine with output directory if provided
    if OUTPUT_DIRECTORY:
        if OUTPUT_DIRECTORY.startswith('/lakehouse/'):
            # Lakehouse path - ensure proper formatting
            output_file = OUTPUT_DIRECTORY.rstrip('/') + '/' + filename
        else:
            # Local path
            import os
            output_file = os.path.join(OUTPUT_DIRECTORY, filename)
    else:
        # Current directory
        output_file = filename
    
    # Check if lakehouse path (supports both /lakehouse/ and abfss://)
    if output_file.startswith('/lakehouse/') or output_file.startswith('abfss://'):
        try:
            from notebookutils import mssparkutils
            mssparkutils.fs.put(output_file, json.dumps(results, indent=2), overwrite=True)
            print(f"✅ Saved to lakehouse: {output_file}")
        except ImportError:
            print("⚠️  Lakehouse path specified but mssparkutils not available")
    else:
        # Regular file save
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"✅ Saved to file: {output_file}")
    
    # Print summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Workspace: {results['workspace_name']}")
    print(f"Dataflows found: {results['dataflow_count']}")
    print(f"Total connections: {results['total_connections']}")
    
    if all_connections:
        print(f"\nConnection breakdown:")
        from collections import Counter
        conn_types = Counter([c['datasource_type'] for c in all_connections])
        for conn_type, count in conn_types.most_common():
            print(f"  {count:4d} - {conn_type}")
    
    print(f"{'='*60}\n")
    
    return results


# ============================================================
# MAIN EXECUTION
# ============================================================

if __name__ == "__main__":
    results = scan_workspace_for_dataflows(WORKSPACE_ID)
    
    # Optional: Display specific connection details
    if PRINT_TO_CONSOLE:
        print("\n📋 DETAILED CONNECTION LIST:")
        for conn in results['all_connections']:
            print(f"\nDataflow: {conn['dataflow_name']}")
            print(f"  Type: {conn['datasource_type']}")
            print(f"  Details: {json.dumps(conn['connection_details'], indent=4)}")
            print(f"  Gateway: {conn['gateway_id'] or 'Cloud (no gateway)'}")
