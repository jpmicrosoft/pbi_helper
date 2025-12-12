"""
Dataflow Gen2 Definition Extractor
Extracts Dataflow Gen2 definitions including Power Query M code for all dataflows in a workspace
"""

import json
from datetime import datetime

# ============================================================
# EXTRACT DATAFLOW DEFINITIONS
# ============================================================

def extract_dataflow_definitions(
    workspace_id: str,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    output_directory: str = None,
    print_to_console: bool = True
):
    """
    Extract all Dataflow Gen2 definitions from a workspace.
    
    This will:
    1. Scan the workspace to find all dataflows
    2. Get the full definition (M code, metadata) for each dataflow
    3. Display decoded M code and metadata on screen
    4. Save raw JSON definitions to files
    
    Args:
        workspace_id: Workspace GUID
        tenant_id: Azure AD tenant ID
        client_id: Service Principal client ID
        client_secret: Service Principal client secret
        output_directory: Directory for output files (None = current directory)
        print_to_console: Whether to print M code and metadata to console
    
    Returns:
        Dictionary with all dataflow definitions
    """
    
    # Initialize API
    from powerbi_admin_api import PowerBIAdminAPI
    api = PowerBIAdminAPI(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret
    )
    
    print(f"\n{'='*70}")
    print(f"EXTRACTING DATAFLOW GEN2 DEFINITIONS")
    print(f"{'='*70}")
    print(f"Workspace ID: {workspace_id}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Step 1: Scan workspace to get dataflows
    print("🔄 Scanning workspace for dataflows...")
    
    scan_result = api.scan_workspace(
        workspace_ids=workspace_id,
        datasource_details=True
    )
    
    workspace_info = scan_result.get('workspaces', [{}])[0]
    workspace_name = workspace_info.get('name', 'Unknown')
    dataflows = scan_result.get('dataflows', [])
    
    print(f"✅ Found {len(dataflows)} dataflow(s) in workspace: {workspace_name}\n")
    
    if len(dataflows) == 0:
        print("⚠️  No dataflows found in workspace")
        return {'workspace_id': workspace_id, 'workspace_name': workspace_name, 'dataflows': []}
    
    # Step 2: Extract definition for each dataflow
    all_definitions = []
    
    for idx, df in enumerate(dataflows, 1):
        dataflow_id = df.get('objectId')
        dataflow_name = df.get('name')
        
        print(f"\n{'='*70}")
        print(f"📊 Dataflow {idx}/{len(dataflows)}: {dataflow_name}")
        print(f"{'='*70}")
        print(f"ID: {dataflow_id}")
        print(f"Configured by: {df.get('configuredBy')}")
        print(f"Modified: {df.get('modifiedDateTime')}\n")
        
        try:
            # Get full definition with automatic decoding
            print("🔄 Retrieving dataflow definition...")
            definition = api.get_dataflow_definition(
                workspace_id=workspace_id,
                dataflow_id=dataflow_id,
                decode_payloads=True  # Automatic decoding
            )
            
            # Display decoded content to console
            if print_to_console:
                print("\n" + "="*70)
                print("📄 DATAFLOW DEFINITION PARTS")
                print("="*70)
                
                for part in definition['definition']['parts']:
                    part_path = part['path']
                    payload_type = part.get('payloadType', 'Unknown')
                    
                    print(f"\n📁 File: {part_path}")
                    print(f"   Type: {payload_type}")
                    print("-" * 70)
                    
                    if part_path == 'mashup.pq':
                        # Display Power Query M code
                        m_code = part['payload']
                        print("\n🔹 POWER QUERY M CODE:")
                        print(m_code)
                        print()
                    
                    elif part_path == 'queryMetadata.json':
                        # Display query metadata
                        metadata = part['payload']
                        print("\n🔹 QUERY METADATA:")
                        print(json.dumps(metadata, indent=2))
                        print()
                    
                    elif part_path == '.platform':
                        # Display platform metadata
                        platform = part['payload']
                        print("\n🔹 PLATFORM METADATA:")
                        print(json.dumps(platform, indent=2))
                        print()
            
            # Save raw definition to file
            timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            dataflow_name_clean = dataflow_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
            filename = f"dataflow_{dataflow_name_clean}_{timestamp_str}_definition.json"
            
            # Add workspace context to the definition
            definition_with_context = {
                'workspace_id': workspace_id,
                'workspace_name': workspace_name,
                'dataflow_id': dataflow_id,
                'dataflow_name': dataflow_name,
                'configured_by': df.get('configuredBy'),
                'modified_date': df.get('modifiedDateTime'),
                'extraction_timestamp': datetime.now().isoformat(),
                'definition': definition
            }
            
            # Determine output path
            if output_directory:
                if output_directory.startswith('/lakehouse/') or output_directory.startswith('abfss://'):
                    output_file = output_directory.rstrip('/') + '/' + filename
                else:
                    import os
                    output_file = os.path.join(output_directory, filename)
            else:
                output_file = filename
            
            # Save file
            if output_file.startswith('/lakehouse/') or output_file.startswith('abfss://'):
                try:
                    from notebookutils import mssparkutils
                    mssparkutils.fs.put(output_file, json.dumps(definition_with_context, indent=2), overwrite=True)
                    print(f"\n✅ Saved definition to lakehouse: {output_file}")
                except ImportError:
                    print("\n⚠️  Lakehouse path specified but mssparkutils not available")
            else:
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(definition_with_context, f, indent=2, ensure_ascii=False)
                print(f"\n✅ Saved definition to file: {output_file}")
            
            # Store in results
            all_definitions.append({
                'dataflow_id': dataflow_id,
                'dataflow_name': dataflow_name,
                'configured_by': df.get('configuredBy'),
                'modified_date': df.get('modifiedDateTime'),
                'definition_file': output_file,
                'definition': definition
            })
            
        except Exception as e:
            print(f"\n❌ Error retrieving definition for {dataflow_name}: {str(e)}")
            all_definitions.append({
                'dataflow_id': dataflow_id,
                'dataflow_name': dataflow_name,
                'error': str(e)
            })
    
    # Final summary
    print("\n\n" + "="*70)
    print("📋 EXTRACTION SUMMARY")
    print("="*70)
    print(f"Workspace: {workspace_name}")
    print(f"Total Dataflows: {len(dataflows)}")
    print(f"Successful Extractions: {len([d for d in all_definitions if 'definition' in d])}")
    print(f"Failed Extractions: {len([d for d in all_definitions if 'error' in d])}")
    
    results = {
        'extraction_timestamp': datetime.now().isoformat(),
        'workspace_id': workspace_id,
        'workspace_name': workspace_name,
        'dataflow_count': len(dataflows),
        'dataflows': all_definitions
    }
    
    # Save summary file
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    workspace_name_clean = workspace_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
    summary_filename = f"workspace_{workspace_name_clean}_{timestamp_str}_definitions_summary.json"
    
    if output_directory:
        if output_directory.startswith('/lakehouse/') or output_directory.startswith('abfss://'):
            summary_file = output_directory.rstrip('/') + '/' + summary_filename
        else:
            import os
            summary_file = os.path.join(output_directory, summary_filename)
    else:
        summary_file = summary_filename
    
    if summary_file.startswith('/lakehouse/') or summary_file.startswith('abfss://'):
        try:
            from notebookutils import mssparkutils
            mssparkutils.fs.put(summary_file, json.dumps(results, indent=2), overwrite=True)
            print(f"\n✅ Saved summary to lakehouse: {summary_file}")
        except ImportError:
            print("\n⚠️  Lakehouse path specified but mssparkutils not available")
    else:
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"\n✅ Saved summary to file: {summary_file}")
    
    print(f"\n{'='*70}\n")
    
    return results


# ============================================================
# MAIN EXECUTION
# ============================================================

if __name__ == "__main__":
    # Example usage
    results = extract_dataflow_definitions(
        workspace_id="your-workspace-guid",
        tenant_id="your-tenant-id",
        client_id="your-client-id",
        client_secret="your-client-secret",
        output_directory="/lakehouse/default/Files/dataflow_definitions",  # or None
        print_to_console=True
    )
