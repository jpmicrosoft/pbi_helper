"""
Power BI Admin API Wrapper
Comprehensive Python class for Microsoft Power BI Admin REST APIs
"""

import requests
import json
import time
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timedelta


class PowerBIAdminAPI:
    """
    Comprehensive wrapper for Power BI Admin REST APIs and Fabric APIs.
    
    Supports:
    - Workspace management and scanning
    - Dataset, report, dashboard operations
    - User and permission management
    - Activity logs and audit
    - Tenant settings
    - Capacity management
    - Apps and dataflows
    - Dataflow Gen2 definitions (Power Query M code)
    """
    
    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        """
        Initialize the Power BI Admin API client.
        
        Args:
            tenant_id: Azure AD tenant ID
            client_id: Service Principal (App) client ID
            client_secret: Service Principal client secret
        """
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = "https://api.powerbi.com/v1.0/myorg/admin"
        self._token = None
        self._token_expiry = None
    
    # ==================== AUTHENTICATION ====================
    
    def _get_token(self) -> str:
        """
        Get OAuth2 access token using Service Principal credentials.
        Caches token and reuses until expiry.
        """
        # Return cached token if still valid
        if self._token and self._token_expiry and datetime.now() < self._token_expiry:
            return self._token
        
        auth_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://analysis.windows.net/powerbi/api/.default",
            "grant_type": "client_credentials",
        }
        
        response = requests.post(auth_url, data=data)
        response.raise_for_status()
        token_data = response.json()
        
        self._token = token_data.get("access_token")
        # Set expiry to 5 minutes before actual expiry for safety
        expires_in = token_data.get("expires_in", 3600)
        self._token_expiry = datetime.now() + timedelta(seconds=expires_in - 300)
        
        return self._token
    
    def _get_headers(self) -> Dict[str, str]:
        """Get HTTP headers with authorization token."""
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json"
        }
    
    # ==================== WORKSPACE OPERATIONS ====================
    
    def list_workspaces(self, top: int = 5000, skip: int = 0, filter_expr: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get list of all workspaces in the tenant.
        
        Args:
            top: Number of workspaces to return (max 5000)
            skip: Number of workspaces to skip
            filter_expr: OData filter expression (optional)
        
        Returns:
            List of workspace objects
        """
        params = {"$top": top}
        if skip > 0:
            params["$skip"] = skip
        if filter_expr:
            params["$filter"] = filter_expr
        
        response = requests.get(
            f"{self.base_url}/groups",
            headers=self._get_headers(),
            params=params
        )
        response.raise_for_status()
        return response.json().get("value", [])
    
    def get_workspace(self, workspace_id: str) -> Dict[str, Any]:
        """
        Get details of a specific workspace.
        
        Args:
            workspace_id: Workspace GUID
        
        Returns:
            Workspace object
        """
        response = requests.get(
            f"{self.base_url}/groups/{workspace_id}",
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json()
    
    def get_workspace_users(self, workspace_id: str) -> List[Dict[str, Any]]:
        """
        Get all users with access to a workspace.
        
        Args:
            workspace_id: Workspace GUID
        
        Returns:
            List of user objects with access rights
        """
        response = requests.get(
            f"{self.base_url}/groups/{workspace_id}/users",
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json().get("value", [])
    
    def scan_workspace(
        self,
        workspace_ids: Union[str, List[str]],
        lineage: bool = True,
        datasource_details: bool = True,
        dataset_schema: bool = True,
        dataset_expressions: bool = True,
        get_artifact_users: bool = True,
        max_wait: int = 600,
        poll_interval: int = 10
    ) -> Dict[str, Any]:
        """
        Scan one or more workspaces for detailed metadata.
        
        Args:
            workspace_ids: Single workspace ID or list of IDs (max 100)
            lineage: Include lineage information
            datasource_details: Include datasource details (enables datasourceInstances)
            dataset_schema: Include dataset schema (tables, columns, measures)
            dataset_expressions: Include DAX and Mashup expressions
            get_artifact_users: Include user details for artifacts
            max_wait: Maximum wait time in seconds (default: 600)
            poll_interval: Polling interval in seconds (default: 10)
        
        Returns:
            Scan result with workspaces and datasourceInstances
        """
        # Normalize to list
        if isinstance(workspace_ids, str):
            workspace_ids = [workspace_ids]
        
        # Step 1: Initiate scan
        scan_body = {"workspaces": workspace_ids}
        params = {
            "lineage": str(lineage),
            "datasourceDetails": str(datasource_details),
            "datasetSchema": str(dataset_schema),
            "datasetExpressions": str(dataset_expressions),
            "getArtifactUsers": str(get_artifact_users)
        }
        
        scan_response = requests.post(
            f"{self.base_url}/workspaces/getInfo",
            headers=self._get_headers(),
            params=params,
            json=scan_body
        )
        scan_response.raise_for_status()
        scan_id = scan_response.json().get("id")
        
        # Step 2: Poll for completion
        elapsed = 0
        while elapsed < max_wait:
            status_response = requests.get(
                f"{self.base_url}/workspaces/scanStatus/{scan_id}",
                headers=self._get_headers()
            )
            
            if status_response.status_code == 200:
                status = status_response.json().get("status")
                if status == "Succeeded":
                    break
                elif status in {"Failed", "Cancelled"}:
                    raise RuntimeError(f"Scan failed with status: {status}")
            elif status_response.status_code != 202:
                status_response.raise_for_status()
            
            time.sleep(poll_interval)
            elapsed += poll_interval
        
        if elapsed >= max_wait:
            raise TimeoutError(f"Scan timed out after {max_wait} seconds")
        
        # Step 3: Get results
        result_response = requests.get(
            f"{self.base_url}/workspaces/scanResult/{scan_id}",
            headers=self._get_headers()
        )
        result_response.raise_for_status()
        return result_response.json()
    
    def get_modified_workspaces(
        self,
        modified_since: Optional[datetime] = None,
        exclude_personal_workspaces: bool = False,
        exclude_inactive_workspaces: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Get workspaces modified after a certain date.
        
        Args:
            modified_since: DateTime to filter from (default: None = all)
            exclude_personal_workspaces: Exclude My Workspace
            exclude_inactive_workspaces: Exclude inactive workspaces
        
        Returns:
            List of modified workspace objects
        """
        params = {}
        if modified_since:
            params["modifiedSince"] = modified_since.isoformat()
        if exclude_personal_workspaces:
            params["excludePersonalWorkspaces"] = "true"
        if exclude_inactive_workspaces:
            params["excludeInActiveWorkspaces"] = "true"
        
        response = requests.get(
            f"{self.base_url}/workspaces/modified",
            headers=self._get_headers(),
            params=params
        )
        response.raise_for_status()
        return response.json()
    
    # ==================== DATASET OPERATIONS ====================
    
    def get_datasets_in_workspace(self, workspace_id: str) -> List[Dict[str, Any]]:
        """Get all datasets in a workspace."""
        response = requests.get(
            f"{self.base_url}/groups/{workspace_id}/datasets",
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json().get("value", [])
    
    def get_dataset(self, dataset_id: str) -> Dict[str, Any]:
        """Get details of a specific dataset."""
        response = requests.get(
            f"{self.base_url}/datasets/{dataset_id}",
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json()
    
    def get_dataset_users(self, dataset_id: str) -> List[Dict[str, Any]]:
        """Get users with access to a dataset."""
        response = requests.get(
            f"{self.base_url}/datasets/{dataset_id}/users",
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json().get("value", [])
    
    def get_datasources(self, dataset_id: str) -> List[Dict[str, Any]]:
        """
        Get datasources for a dataset.
        Note: This returns basic datasource info, not as detailed as Scanner API.
        """
        response = requests.get(
            f"{self.base_url}/datasets/{dataset_id}/datasources",
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json().get("value", [])
    
    def get_refresh_history(
        self,
        dataset_id: str,
        top: int = 10
    ) -> List[Dict[str, Any]]:
        """Get refresh history for a dataset."""
        response = requests.get(
            f"{self.base_url}/datasets/{dataset_id}/refreshes",
            headers=self._get_headers(),
            params={"$top": top}
        )
        response.raise_for_status()
        return response.json().get("value", [])
    
    # ==================== REPORT OPERATIONS ====================
    
    def get_reports_in_workspace(self, workspace_id: str) -> List[Dict[str, Any]]:
        """Get all reports in a workspace."""
        response = requests.get(
            f"{self.base_url}/groups/{workspace_id}/reports",
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json().get("value", [])
    
    def get_report(self, report_id: str) -> Dict[str, Any]:
        """Get details of a specific report."""
        response = requests.get(
            f"{self.base_url}/reports/{report_id}",
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json()
    
    def get_report_users(self, report_id: str) -> List[Dict[str, Any]]:
        """Get users with access to a report."""
        response = requests.get(
            f"{self.base_url}/reports/{report_id}/users",
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json().get("value", [])
    
    # ==================== DASHBOARD OPERATIONS ====================
    
    def get_dashboards_in_workspace(self, workspace_id: str) -> List[Dict[str, Any]]:
        """Get all dashboards in a workspace."""
        response = requests.get(
            f"{self.base_url}/groups/{workspace_id}/dashboards",
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json().get("value", [])
    
    def get_dashboard(self, dashboard_id: str) -> Dict[str, Any]:
        """Get details of a specific dashboard."""
        response = requests.get(
            f"{self.base_url}/dashboards/{dashboard_id}",
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json()
    
    def get_dashboard_users(self, dashboard_id: str) -> List[Dict[str, Any]]:
        """Get users with access to a dashboard."""
        response = requests.get(
            f"{self.base_url}/dashboards/{dashboard_id}/users",
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json().get("value", [])
    
    def get_tiles(self, dashboard_id: str) -> List[Dict[str, Any]]:
        """Get tiles in a dashboard."""
        response = requests.get(
            f"{self.base_url}/dashboards/{dashboard_id}/tiles",
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json().get("value", [])
    
    # ==================== DATAFLOW OPERATIONS ====================
    
    def get_dataflows_in_workspace(self, workspace_id: str) -> List[Dict[str, Any]]:
        """Get all dataflows in a workspace."""
        response = requests.get(
            f"{self.base_url}/groups/{workspace_id}/dataflows",
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json().get("value", [])
    
    def get_dataflow(self, dataflow_id: str) -> Dict[str, Any]:
        """Get details of a specific dataflow."""
        response = requests.get(
            f"{self.base_url}/dataflows/{dataflow_id}",
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json()
    
    def get_dataflow_users(self, dataflow_id: str) -> List[Dict[str, Any]]:
        """Get users with access to a dataflow."""
        response = requests.get(
            f"{self.base_url}/dataflows/{dataflow_id}/users",
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json().get("value", [])
    
    def get_dataflow_definition(
        self,
        workspace_id: str,
        dataflow_id: str,
        decode_payloads: bool = True,
        timeout: int = 300,
        poll_interval: int = 5
    ) -> Dict[str, Any]:
        """
        Get Dataflow Gen2 definition including Power Query M code.
        
        This uses the Fabric API (not Power BI Admin API) to retrieve the full
        dataflow definition including queryMetadata.json, mashup.pq (M code),
        and platform metadata.
        
        Args:
            workspace_id: Workspace GUID containing the dataflow
            dataflow_id: Dataflow GUID
            decode_payloads: If True, automatically decodes base64 payloads (default: True)
            timeout: Maximum seconds to wait for long-running operation (default: 300)
            poll_interval: Seconds between status checks (default: 5)
        
        Returns:
            Dictionary with 'definition' containing 'parts' array:
            - queryMetadata.json: Query metadata (decoded JSON if decode_payloads=True)
            - mashup.pq: Power Query M code (decoded string if decode_payloads=True)
            - .platform: Platform metadata (decoded JSON if decode_payloads=True)
        
        Example:
            # Get with automatic decoding (default)
            definition = api.get_dataflow_definition(workspace_id, dataflow_id)
            
            for part in definition['definition']['parts']:
                if part['path'] == 'mashup.pq':
                    print(part['payload'])  # Already decoded M code
                elif part['path'] == 'queryMetadata.json':
                    print(part['payload'])  # Already decoded and parsed JSON
        """
        fabric_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataflows/{dataflow_id}/getDefinition"
        
        # Initial POST request
        response = requests.post(
            fabric_url,
            headers={
                "Authorization": f"Bearer {self._get_token()}",
                "Content-Type": "application/json"
            }
        )
        
        # Check if immediate success (200) or long-running operation (202)
        if response.status_code == 200:
            result = response.json()
            
            # Decode payloads if requested
            if decode_payloads:
                result = self._decode_dataflow_definition(result)
            
            return result
        
        if response.status_code == 202:
            # Long-running operation - poll for completion
            operation_id = response.headers.get('x-ms-operation-id')
            location_url = response.headers.get('Location')
            
            if not location_url:
                raise Exception("Long-running operation started but no Location header provided")
            
            print(f"Retrieving dataflow definition... (Operation ID: {operation_id})")
            
            start_time = time.time()
            attempts = 0
            
            while time.time() - start_time < timeout:
                attempts += 1
                time.sleep(poll_interval)
                
                # Poll the operation status
                status_response = requests.get(
                    location_url,
                    headers={"Authorization": f"Bearer {self._get_token()}"}
                )
                
                if status_response.status_code == 200:
                    # Operation completed successfully
                    print(f"✅ Dataflow definition retrieved successfully (attempt {attempts})")
                    result = status_response.json()
                    
                    # Decode payloads if requested
                    if decode_payloads:
                        result = self._decode_dataflow_definition(result)
                    
                    return result
                
                elif status_response.status_code == 202:
                    # Still in progress
                    print(f"⏳ Definition retrieval in progress... (attempt {attempts}/{timeout // poll_interval})")
                    continue
                
                else:
                    # Error occurred
                    status_response.raise_for_status()
            
            raise TimeoutError(f"Dataflow definition retrieval timed out after {timeout} seconds")
        
        else:
            response.raise_for_status()
    
    def _decode_dataflow_definition(self, definition_response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Decode base64-encoded payloads in dataflow definition.
        
        Args:
            definition_response: Raw API response with base64 payloads
        
        Returns:
            Definition with decoded payloads (strings for .pq, parsed JSON for .json)
        """
        import base64
        
        if 'definition' not in definition_response or 'parts' not in definition_response['definition']:
            return definition_response
        
        for part in definition_response['definition']['parts']:
            if part.get('payloadType') == 'InlineBase64':
                try:
                    # Decode base64
                    decoded_bytes = base64.b64decode(part['payload'])
                    decoded_str = decoded_bytes.decode('utf-8')
                    
                    # Parse JSON for .json files
                    if part['path'].endswith('.json'):
                        part['payload'] = json.loads(decoded_str)
                        part['payloadType'] = 'DecodedJSON'
                    else:
                        # Keep as string for .pq and other text files
                        part['payload'] = decoded_str
                        part['payloadType'] = 'DecodedText'
                    
                except Exception as e:
                    # If decoding fails, keep original and add error info
                    part['decode_error'] = str(e)
        
        return definition_response
    
    # ==================== APP OPERATIONS ====================
    
    def get_apps(self, top: int = 5000) -> List[Dict[str, Any]]:
        """Get all apps in the tenant."""
        response = requests.get(
            f"{self.base_url}/apps",
            headers=self._get_headers(),
            params={"$top": top}
        )
        response.raise_for_status()
        return response.json().get("value", [])
    
    def get_app(self, app_id: str) -> Dict[str, Any]:
        """Get details of a specific app."""
        response = requests.get(
            f"{self.base_url}/apps/{app_id}",
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json()
    
    def get_app_users(self, app_id: str) -> List[Dict[str, Any]]:
        """Get users with access to an app."""
        response = requests.get(
            f"{self.base_url}/apps/{app_id}/users",
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json().get("value", [])
    
    # ==================== ACTIVITY LOGS ====================
    
    def get_activity_events(
        self,
        start_date: datetime,
        end_date: Optional[datetime] = None,
        activity_filter: Optional[str] = None,
        user_id_filter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get activity events (audit logs) for the tenant.
        
        Args:
            start_date: Start date for activity logs
            end_date: End date (default: start_date + 1 day)
            activity_filter: Filter by activity type (optional)
            user_id_filter: Filter by user ID (optional)
        
        Returns:
            List of activity event objects
        """
        if not end_date:
            end_date = start_date + timedelta(days=1)
        
        # Format dates as ISO strings with quotes
        start_str = f"'{start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}Z'"
        end_str = f"'{end_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}Z'"
        
        params = {
            "startDateTime": start_str,
            "endDateTime": end_str
        }
        
        if activity_filter:
            params["$filter"] = activity_filter
        
        response = requests.get(
            f"{self.base_url}/activityevents",
            headers=self._get_headers(),
            params=params
        )
        response.raise_for_status()
        
        # Activity events return as newline-delimited JSON
        events = []
        for line in response.text.strip().split('\n'):
            if line:
                events.append(json.loads(line))
        return events
    
    # ==================== TENANT SETTINGS ====================
    
    def get_tenant_settings(self) -> Dict[str, Any]:
        """Get all tenant settings."""
        response = requests.get(
            f"{self.base_url}/tenantsettings",
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json()
    
    def check_metadata_scanning_enabled(self) -> Dict[str, Any]:
        """
        Check if metadata scanning and admin API settings are enabled.
        
        Returns:
            Dictionary of relevant settings with their enabled status
        """
        settings = self.get_tenant_settings()
        relevant_settings = {}
        
        if 'tenantSettings' in settings:
            for setting in settings['tenantSettings']:
                setting_name = setting.get('settingName', '')
                if any(keyword in setting_name.lower() for keyword in ['metadata', 'scan', 'admin', 'api']):
                    relevant_settings[setting_name] = {
                        'enabled': setting.get('enabled', False),
                        'canSpecifySecurityGroups': setting.get('canSpecifySecurityGroups', False),
                        'enabledSecurityGroups': setting.get('enabledSecurityGroups', [])
                    }
        
        return relevant_settings
    
    # ==================== CAPACITY OPERATIONS ====================
    
    def get_capacities(self) -> List[Dict[str, Any]]:
        """Get all capacities in the tenant."""
        response = requests.get(
            f"{self.base_url}/capacities",
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json().get("value", [])
    
    def get_capacity_workspaces(self, capacity_id: str) -> List[Dict[str, Any]]:
        """Get workspaces assigned to a capacity."""
        response = requests.get(
            f"{self.base_url}/capacities/{capacity_id}/workloads",
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json().get("value", [])
    
    # ==================== UTILITY METHODS ====================
    
    def save_to_json(self, data: Any, filepath: str, indent: int = 2):
        """Save data to JSON file."""
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=indent)
    
    def get_all_workspace_details(self, workspace_id: str) -> Dict[str, Any]:
        """
        Get comprehensive details about a workspace including all artifacts.
        
        Returns:
            Dictionary with workspace, datasets, reports, dashboards, dataflows, users
        """
        return {
            'workspace': self.get_workspace(workspace_id),
            'users': self.get_workspace_users(workspace_id),
            'datasets': self.get_datasets_in_workspace(workspace_id),
            'reports': self.get_reports_in_workspace(workspace_id),
            'dashboards': self.get_dashboards_in_workspace(workspace_id),
            'dataflows': self.get_dataflows_in_workspace(workspace_id)
        }
