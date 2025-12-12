# Running Power BI Admin API in PySpark (Fabric Notebooks)

## Method 1: Upload Python File to Lakehouse (Recommended)

### Step 1: Upload `powerbi_admin_api.py` to Lakehouse
1. Open your Fabric workspace
2. Navigate to your Lakehouse
3. Go to **Files** folder
4. Upload `powerbi_admin_api.py`

### Step 2: Import in Notebook
```python
# Add lakehouse Files folder to Python path
import sys
sys.path.append('/lakehouse/default/Files')

# Now you can import
from powerbi_admin_api import PowerBIAdminAPI
```

### Step 3: Use the API
```python
# Initialize
api = PowerBIAdminAPI(
    tenant_id="your-tenant-id",
    client_id="your-client-id",
    client_secret="your-client-secret"
)

# Use it
scan_result = api.scan_workspace("workspace-id")
```

---

## Method 2: Inline Code (Copy/Paste Entire Class)

Just copy the entire contents of `powerbi_admin_api.py` into a notebook cell and run it. Then use the class in subsequent cells.

**Cell 1:**
```python
# Paste entire powerbi_admin_api.py content here
import requests
import json
import time
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timedelta

class PowerBIAdminAPI:
    # ... entire class code ...
```

**Cell 2:**
```python
# Use the class
api = PowerBIAdminAPI(tenant_id="...", client_id="...", client_secret="...")
```

---

## Method 3: Install from GitHub (If Published)

```python
%pip install git+https://github.com/jpmicrosoft/pbi_helper.git
from powerbi_admin_api import PowerBIAdminAPI
```

---

## Complete PySpark Example

```python
# ============================================================
# CELL 1: Setup and Import
# ============================================================
import sys
sys.path.append('/lakehouse/default/Files')

from powerbi_admin_api import PowerBIAdminAPI
from pyspark.sql import SparkSession
import json

# ============================================================
# CELL 2: Initialize API
# ============================================================
TENANT_ID = "your-tenant-id"
CLIENT_ID = "your-client-id" 
CLIENT_SECRET = "your-client-secret"

api = PowerBIAdminAPI(
    tenant_id=TENANT_ID,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET
)

# ============================================================
# CELL 3: Scan Workspace
# ============================================================
workspace_id = "your-workspace-id"

scan_result = api.scan_workspace(
    workspace_ids=workspace_id,
    lineage=True,
    datasource_details=True,
    dataset_schema=True,
    dataset_expressions=True
)

print(f"✅ Scan completed")
print(f"Datasource instances: {len(scan_result.get('datasourceInstances', []))}")
print(f"Dataflows: {len(scan_result.get('dataflows', []))}")

# ============================================================
# CELL 4: Save to Lakehouse using mssparkutils
# ============================================================
from notebookutils import mssparkutils

# Save scan result
output_path = f"/lakehouse/default/Files/pbi_scans/workspace_{workspace_id}_scan.json"
json_content = json.dumps(scan_result, indent=2)

mssparkutils.fs.put(output_path, json_content, overwrite=True)
print(f"✅ Saved to: {output_path}")

# ============================================================
# CELL 5: Convert to Spark DataFrame (Optional)
# ============================================================
spark = SparkSession.builder.getOrCreate()

# Extract datasource instances
datasources = scan_result.get('datasourceInstances', [])

# Convert to Spark DataFrame
df_datasources = spark.createDataFrame(
    [(ds.get('datasourceType'), 
      str(ds.get('connectionDetails')), 
      ds.get('datasourceId'),
      ds.get('gatewayId'))
     for ds in datasources],
    ['datasourceType', 'connectionDetails', 'datasourceId', 'gatewayId']
)

df_datasources.show(truncate=False)

# Save as Delta table
df_datasources.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("pbi_datasources")

print("✅ Saved to Delta table: pbi_datasources")

# ============================================================
# CELL 6: Extract Dataflow Gen2 Information
# ============================================================
dataflows = scan_result.get('dataflows', [])

print(f"\n📊 Found {len(dataflows)} dataflows:\n")

for df_item in dataflows:
    print(f"Dataflow: {df_item.get('name')}")
    print(f"  ID: {df_item.get('objectId')}")
    print(f"  Tables: {len(df_item.get('tables', []))}")
    
    # Extract datasources for this dataflow
    df_datasources = df_item.get('datasources', [])
    for ds in df_datasources:
        print(f"    → Datasource: {ds.get('datasourceType')}")
        print(f"      Connection: {ds.get('connectionDetails')}")
    
    # Get Mashup/M code (Power Query)
    if 'mashup' in df_item:
        print(f"  Has Mashup code: Yes")
    
    print()

# ============================================================
# CELL 7: Batch Process All Workspaces
# ============================================================
import time

# Get all workspaces
all_workspaces = api.list_workspaces()
workspace_ids = [ws['id'] for ws in all_workspaces]

print(f"📋 Total workspaces: {len(workspace_ids)}")

# Process in batches of 100
batch_size = 100
all_results = []

for i in range(0, len(workspace_ids), batch_size):
    batch = workspace_ids[i:i+batch_size]
    batch_num = i//batch_size + 1
    
    print(f"\n🔄 Processing batch {batch_num}: {len(batch)} workspaces")
    
    try:
        scan_result = api.scan_workspace(
            workspace_ids=batch,
            max_wait=900  # 15 minutes for large batches
        )
        
        # Save batch
        batch_path = f"/lakehouse/default/Files/pbi_scans/batch_{batch_num}.json"
        mssparkutils.fs.put(batch_path, json.dumps(scan_result, indent=2), overwrite=True)
        
        all_results.append(scan_result)
        print(f"  ✅ Batch {batch_num} completed")
        
        # Rate limiting
        if i + batch_size < len(workspace_ids):
            print(f"  ⏳ Waiting 60 seconds...")
            time.sleep(60)
            
    except Exception as e:
        print(f"  ❌ Error: {e}")
        continue

print(f"\n✅ Completed {len(all_results)} batches")
```

---

## Key Differences for PySpark:

### 1. **File Paths**
- ❌ Local: `"output.json"`
- ✅ Lakehouse: `"/lakehouse/default/Files/output.json"`

### 2. **File Operations**
- ❌ `open()` and `json.dump()`
- ✅ `mssparkutils.fs.put(path, content, overwrite=True)`

### 3. **Module Imports**
- Add lakehouse to path: `sys.path.append('/lakehouse/default/Files')`

### 4. **Data Processing**
- Convert JSON to Spark DataFrames for analysis
- Save as Delta tables for querying

### 5. **Dependencies**
- `requests` library is pre-installed in Fabric
- No pip install needed for standard libraries

---

## Troubleshooting

### Issue: Module not found
```python
# Make sure file is uploaded to Files folder
mssparkutils.fs.ls('/lakehouse/default/Files')
```

### Issue: Import error
```python
# Check Python path
import sys
print(sys.path)

# Add lakehouse explicitly
sys.path.insert(0, '/lakehouse/default/Files')
```

### Issue: Authentication fails
- Verify Service Principal has Power BI Admin API permissions
- Check tenant settings are enabled for metadata scanning

---

## Best Practices

1. **Store credentials securely** - Use Azure Key Vault or environment variables
2. **Use lakehouse paths** - Always prefix with `/lakehouse/default/Files/`
3. **Save incrementally** - Don't lose data if notebook crashes
4. **Rate limiting** - Add delays between large batch operations
5. **Error handling** - Wrap API calls in try-except blocks
