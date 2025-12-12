"""
Single Workspace Dataflow Gen2 Scanner
Scans one workspace at a time to extract Dataflow Gen2 connection information
"""

import json
import time
from datetime import datetime

# ============================================================
# SCAN SINGLE WORKSPACE
# ============================================================

def scan_workspace_for_dataflows(
    workspace_id: str,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    output_directory: str = None,
    print_to_console: bool = True
):
    """
    Scan a single workspace and extract all Dataflow Gen2 connection information.
    
    Args:
        workspace_id: Single workspace GUID
        tenant_id: Azure AD tenant ID
        client_id: Service Principal client ID
        client_secret: Service Principal client secret
        output_directory: Directory path for output files (None = current directory)
        print_to_console: Whether to print detailed output to console
    
    Returns:
        Dictionary with dataflow and connection details
    """
    
    # Initialize API
    api = PowerBIAdminAPI(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret
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
    
    # Extract datasets and dataflows
    datasets = scan_result.get('datasets', [])
    dataflows = scan_result.get('dataflows', [])
    
    print(f"📊 Found {len(datasets)} dataset(s) and {len(dataflows)} dataflow(s) in workspace\n")
    
    # Process datasets
    dataset_details = []
    all_connections = []
    
    for ds in datasets:
        print(f"Dataset: {ds.get('name')}")
        print(f"  ID: {ds.get('id')}")
        print(f"  Configured by: {ds.get('configuredBy')}")
        print(f"  Storage Mode: {ds.get('targetStorageMode')}")
        
        # Extract tables
        tables = ds.get('tables', [])
        print(f"  Tables: {len(tables)}")
        for table in tables:
            print(f"    • {table.get('name')} (Hidden: {table.get('isHidden', False)})")
            columns = table.get('columns', [])
            measures = table.get('measures', [])
            if columns:
                print(f"      - Columns: {len(columns)}")
            if measures:
                print(f"      - Measures: {len(measures)}")
        
        # Extract relationships
        relationships = ds.get('relationships', [])
        if relationships:
            print(f"  Relationships: {len(relationships)}")
        
        # Extract expressions (parameters, queries)
        expressions = ds.get('expressions', [])
        if expressions:
            print(f"  Expressions/Parameters: {len(expressions)}")
            for expr in expressions:
                print(f"    • {expr.get('name')} - {expr.get('description', 'No description')}")
        
        # Extract datasource usages
        datasource_usages = ds.get('datasourceUsages', [])
        print(f"  Datasource Usages: {len(datasource_usages)}")
        
        # Dataset summary
        dataset_detail = {
            'name': ds.get('name'),
            'id': ds.get('id'),
            'description': ds.get('description'),
            'configured_by': ds.get('configuredBy'),
            'created_date': ds.get('createdDate'),
            'target_storage_mode': ds.get('targetStorageMode'),
            'endorsement': ds.get('endorsementDetails'),
            'sensitivity_label': ds.get('sensitivityLabel'),
            'table_count': len(tables),
            'tables': [{
                'name': t.get('name'),
                'is_hidden': t.get('isHidden', False),
                'description': t.get('description'),
                'column_count': len(t.get('columns', [])),
                'measure_count': len(t.get('measures', [])),
                'columns': [{
                    'name': c.get('name'),
                    'data_type': c.get('dataType'),
                    'is_hidden': c.get('isHidden', False)
                } for c in t.get('columns', [])],
                'measures': [{
                    'name': m.get('name'),
                    'expression': m.get('expression'),
                    'description': m.get('description')
                } for m in t.get('measures', [])]
            } for t in tables],
            'relationship_count': len(relationships),
            'relationships': relationships,
            'expression_count': len(expressions),
            'expressions': [{
                'name': e.get('name'),
                'description': e.get('description'),
                'expression': e.get('expression')
            } for e in expressions],
            'datasource_usage_count': len(datasource_usages),
            'datasource_usages': datasource_usages,
            'upstream_dataflows': ds.get('upstreamDataflows', []),
            'roles': ds.get('roles', [])
        }
        
        dataset_details.append(dataset_detail)
        print()
    
    # Process dataflows
    dataflow_details = []
    
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
        
        # Extract datasource usages (references to datasourceInstances)
        datasource_usages = df.get('datasourceUsages', [])
        
        # Dataflow summary
        dataflow_detail = {
            'name': df.get('name'),
            'id': df.get('objectId'),
            'description': df.get('description'),
            'configured_by': df.get('configuredBy'),
            'modified_by': df.get('modifiedBy'),
            'modified_date': df.get('modifiedDateTime'),
            'endorsement': df.get('endorsementDetails'),
            'sensitivity_label': df.get('sensitivityLabel'),
            'table_count': len(tables),
            'tables': [t.get('name') for t in tables],
            'connection_count': len(datasources),
            'connections': dataflow_connections,
            'datasource_usage_count': len(datasource_usages),
            'datasource_usages': datasource_usages
        }
        
        dataflow_details.append(dataflow_detail)
        print()
    
    # Extract datasourceInstances from root level
    datasource_instances = scan_result.get('datasourceInstances', [])
    misconfigured_datasources = scan_result.get('misconfiguredDatasourceInstances', [])
    
    # Compile results
    results = {
        'scan_timestamp': datetime.now().isoformat(),
        'workspace_id': workspace_id,
        'workspace_name': workspace_info.get('name') if workspace_info else 'Unknown',
        'workspace_type': workspace_info.get('type') if workspace_info else 'Unknown',
        'capacity_id': workspace_info.get('capacityId') if workspace_info else None,
        'dataset_count': len(datasets),
        'dataflow_count': len(dataflows),
        'total_connections': len(all_connections),
        'datasets': dataset_details,
        'dataflows': dataflow_details,
        'all_connections': all_connections,
        'datasource_instances': datasource_instances,
        'misconfigured_datasources': misconfigured_datasources
    }
    
    # Create filename with workspace name and timestamp
    workspace_name_clean = results['workspace_name'].replace(' ', '_').replace('/', '_').replace('\\', '_')
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"workspace_{workspace_name_clean}_{timestamp_str}_dataflows.json"
    raw_filename = f"workspace_{workspace_name_clean}_{timestamp_str}_raw_scan.json"
    
    # Combine with output directory if provided
    if output_directory:
        if output_directory.startswith('/lakehouse/'):
            # Lakehouse path - ensure proper formatting
            output_file = output_directory.rstrip('/') + '/' + filename
            raw_output_file = output_directory.rstrip('/') + '/' + raw_filename
        else:
            # Local path
            import os
            output_file = os.path.join(output_directory, filename)
            raw_output_file = os.path.join(output_directory, raw_filename)
    else:
        # Current directory
        output_file = filename
        raw_output_file = raw_filename
    
    # Save processed results
    # Check if lakehouse path (supports both /lakehouse/ and abfss://)
    if output_file.startswith('/lakehouse/') or output_file.startswith('abfss://'):
        try:
            from notebookutils import mssparkutils
            mssparkutils.fs.put(output_file, json.dumps(results, indent=2), overwrite=True)
            print(f"✅ Saved processed results to lakehouse: {output_file}")
            
            # Save raw scan results
            mssparkutils.fs.put(raw_output_file, json.dumps(scan_result, indent=2), overwrite=True)
            print(f"✅ Saved raw scan results to lakehouse: {raw_output_file}")
        except ImportError:
            print("⚠️  Lakehouse path specified but mssparkutils not available")
    else:
        # Regular file save - processed results
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"✅ Saved processed results to file: {output_file}")
        
        # Save raw scan results
        with open(raw_output_file, 'w') as f:
            json.dump(scan_result, f, indent=2)
        print(f"✅ Saved raw scan results to file: {raw_output_file}")
    
    # Connection breakdown by type
    connection_types = {}
    for conn in all_connections:
        conn_type = conn['datasource_type']
        connection_types[conn_type] = connection_types.get(conn_type, 0) + 1
    
    results['connection_breakdown'] = connection_types
    
    # Print summary
    print("\n" + "="*70)
    print("📋 SCAN SUMMARY")
    print("="*70)
    print(f"Workspace: {results['workspace_name']}")
    print(f"Datasets: {results['dataset_count']}")
    print(f"Dataflows: {results['dataflow_count']}")
    print(f"Total Connections: {results['total_connections']}")
    print(f"Datasource Instances: {len(datasource_instances)}")
    print(f"Misconfigured Datasources: {len(misconfigured_datasources)}")
    print("\nConnection Breakdown:")
    for conn_type, count in connection_types.items():
        print(f"  • {conn_type}: {count}")
    
    if all_connections:
        print(f"\nDetailed Connections:")
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
    # Example usage - pass parameters directly
    results = scan_workspace_for_dataflows(
        workspace_id="your-workspace-guid",
        tenant_id="your-tenant-id",
        client_id="your-client-id",
        client_secret="your-client-secret",
        output_directory="/lakehouse/default/Files/pbi_scans",  # or None
        print_to_console=True
    )
    
    # Optional: Display specific connection details
    print("\n📋 DETAILED CONNECTION LIST:")
    for conn in results['all_connections']:
        print(f"\nDataflow: {conn['dataflow_name']}")
        print(f"  Type: {conn['datasource_type']}")
        print(f"  Details: {json.dumps(conn['connection_details'], indent=4)}")
        print(f"  Gateway: {conn['gateway_id'] or 'Cloud (no gateway)'}")
