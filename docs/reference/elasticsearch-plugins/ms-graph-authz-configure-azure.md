---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/ms-graph-authz-configure-azure.html
applies_to:
  stack: ga 9.1
---

# Configure Azure [configure-azure]

To make API calls to Microsoft Graph, Elasticsearch requires Azure credentials
with the correct permissions.

## Create a custom Azure application

1) Log in to the [Azure portal](https://portal.azure.com) and go to Microsoft
   Entra ID.
2) To register a new application, click **Enterprise applications** > **New
   application**.
3) Click **Create your own application**, provide a name, and select the
**Integrate any other application you don’t find in the gallery** option.

:::{image} ./images/ms-graph-authz/01-create-enterprise-application.png
:alt: "create your own application" page
:::

## Configure the custom application

1) In the [Azure portal](https://portal.azure.com), go to Microsoft Entra ID.
2) Under **App registrations**, select the **All applications** tab, and then
   find the application created in the previous section.

   :::{image} ./images/ms-graph-authz/02-find-app-registration.png
   :alt: find your app registration
   :::
3) Take note of the **Application (client) ID** and **Tenant ID** shown here.
   These will be needed to configure Elasticsearch later.

   :::{image} ./images/ms-graph-authz/03-get-application-id.png
   :alt: get your application ID
   :::
4) Under **Manage** > **Certificates & secrets**, do the following:
    - Create a new client secret.
    - Take note of your new client secret's **Value**. This is needed later, and
      is only displayed once.
      :::{image} ./images/ms-graph-authz/04-create-client-secret.png
      :alt: get your client secret
      :::
5) Under **Manage** > **API permissions**, do the following:
    1. Go to **Add a permission**.
    2. Choose **Microsoft Graph**.
    3. Choose **Application permissions**.
    4. Select `Directory.ReadWrite.All`, `Group.ReadWrite.All`, `User.Read.All`.

   :::{note}
   An Azure Admin must approve these permissions before the credentials can be used.
   :::

   :::{image} ./images/ms-graph-authz/05-configure-api-permissions.png
   :alt: configure api permissions
   :::
