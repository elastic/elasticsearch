---
mapped_pages:
 - https://www.elastic.co/guide/en/elasticsearch/plugins/current/ms-graph-authz-configure-azure.html
---

# Configure Azure [configure-azure]

To make API calls to Microsoft Graph, Elasticsearch requires Azure credentials with the correct permissions.

## Create a custom Azure Application

1) Log in to the [Azure portal](https://portal.azure.com) and go to Microsoft Entra ID
2) Click "Enterprise applications" and then "New application" to register a new application.
3) Click "Create your own application", provide a name, and select the "Integrate any other application you don’t find in the gallery" option.

## Configure the custom Application

1) In the [Azure portal](https://portal.azure.com), go to Microsoft Entra ID.
2) Under “App registrations”, then the “All applications” tab, find the application created in the previous section.
3) Take note of the Application (client) ID and Tenant ID shown here - these will be needed to configure Elasticsearch later.
4) Under Manage > Certificates & secrets
   - Create a new client secret
   - Take note of the Value - this is needed later, and is only shown once
5) Under Manage > API permissions
   - Go to “Add a permission”
   - Choose “Microsoft Graph”
   - Choose “Application permissions”
   - Select “Directory.ReadWrite.All, Group.ReadWrite.All, User.Read.All”
   - Note that an Azure Admin will need to approve these permissions before the credentials can be used
