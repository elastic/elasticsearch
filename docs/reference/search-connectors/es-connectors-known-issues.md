---
navigation_title: "Known issues"
---

# Connector known issues [es-connectors-known-issues]

:::{important}
Enterprise Search is not available in {{stack}} 9.0+.
:::

## Connector service [es-connectors-known-issues-connector-service]

The connector service has the following known issues:

* **OOM errors when syncing large database tables**

    Syncs after the initial sync can cause out-of-memory (OOM) errors when syncing large database tables. This occurs because database connectors load and store IDs in memory. For tables with millions of records, this can lead to memory exhaustion if the connector service has insufficient RAM.

    To mitigate this issue, you can:

    * **Increase RAM allocation**:

        * **Self-managed**: Increase RAM allocation for the machine/container running the connector service.

            ::::{dropdown} RAM sizing guidelines
            The following table shows the estimated RAM usage for loading IDs into memory.

            | **Number of IDs** | **Memory Usage in MB (2X buffer)** |
            | --- | --- |
            | 1,000,000 | ≈ 45.78 MB |
            | 10,000,000 | ≈ 457.76 MB |
            | 50,000,000 | ≈ 2288.82 MB (≈ 2.29 GB) |
            | 100,000,000 | ≈ 4577.64 MB (≈ 4.58 GB) |

            ::::

    * **Optimize** [**sync rules**](/reference/search-connectors/es-sync-rules.md):

        * Review and optimize sync rules to filter and reduce data retrieved from the source before syncing.

%    * **Use a self-managed connector** instead of a managed connector:

%        * Because self-managed connectors run on your infrastructure, they are not subject to the same RAM limitations of the Enterprise Search node.

* **Upgrades from deployments running on versions earlier than 8.9.0 can cause sync job failures**

    Due to a bug, the `job_type` field mapping will be missing after upgrading from deployments running on versions earlier than 8.9.0. Sync jobs won’t be displayed in the Kibana UI (job history) and the connector service won’t be able to start new sync jobs. **This will only occur if you have previously scheduled sync jobs.**

    To resolve this issue, you can manually add the missing field with the following command and trigger a sync job:

    ```console
    PUT .elastic-connectors-sync-jobs-v1/_mapping
    {
      "properties": {
        "job_type": {
          "type": "keyword"
        }
      }
    }
    ```
    % TEST[skip:TODO]

* **The connector service will fail to sync when the connector tries to fetch more more than 2,147,483,647 (*2^31-1*) documents from a data source**

    A workaround is to manually partition the data to be synced using multiple search indices.

* **Custom scheduling might break when upgrading from version 8.6 or earlier.**

    If you encounter the error `'custom_schedule_triggered': undefined method 'each' for nil:NilClass (NoMethodError)`, it means the custom scheduling feature migration failed. You can use the following manual workaround:

    ```console
    POST /.elastic-connectors/_update/connector-id
    {
      "doc": {
        "custom_scheduling": {}
      }
    }
    ```
    % TEST[skip:TODO]

    This error can appear on Connectors or Crawlers that aren’t the cause of the issue. If the error continues, try running the above command for every document in the `.elastic-connectors` index.

* **Connectors upgrading from 8.7 or earlier can be missing configuration fields**

    A connector that was created prior to 8.8 can sometimes be missing configuration fields. This is a known issue for the MySQL connector but could also affect other connectors.

    If the self-managed connector raises the error `Connector for <connector_id> has missing configuration fields: <field_a>, <field_b>...`, you can resolve the error by manually adding the missing configuration fields via the Dev Tools. Only the following two field properties are required, as the rest will be autopopulated by the self-managed connector:

    * `type`: one of `str`, `int`, `bool`, or `list`
    * `value`: any value, as long as it is of the correct `type` (`list` type values should be saved as comma-separated strings)

        ```console
        POST /.elastic-connectors/_update/connector_id
        {
          "doc" : {
            "configuration": {
              "field_a": {
                "type": "str",
                "value": ""
              },
              "field_b": {
                "type": "bool",
                "value": false
              },
              "field_c": {
                "type": "int",
                "value": 1
              },
              "field_d": {
                "type": "list",
                "value": "a,b"
              }
            }
          }
        }
        ```
        % TEST[skip:TODO]

* **Python connectors that upgraded from 8.7.1 will report document volumes in gigabytes (GB) instead of megabytes (MB)**

    As a result, true document volume will be under-reported by a factor of 1024.


* **DLS queries fail to match documents for content indices created on 9.0+**

    The DLS query template stored in access control documents (`.search-acl-filter-*` indices) references the sub-field `_allow_access_control.enum`. This sub-field was created by a custom dynamic mapping template that was [removed in connectors v9.0.0](https://github.com/elastic/connectors/pull/3013). Under Elasticsearch's default dynamic mapping, the correct sub-field is `_allow_access_control.keyword`. As a result, DLS-protected documents are silently filtered out — users see no results for documents that have access control set.

    **Affected versions**: 9.0.0+, for any content index created after upgrading. Indices created before 9.0 are not affected because the old mapping is preserved.

    **Workaround**: After fetching the DLS query from the access control document, replace `_allow_access_control.enum` with `_allow_access_control.keyword` before using it in an API key role descriptor.

    **Fix**: Tracked in [elastic/connectors#4005](https://github.com/elastic/connectors/issues/4005). After the fix is deployed, re-run an **access control sync** so the corrected query template is written to the `.search-acl-filter-*` documents.


* **Generic database connectors fail to sync with `ModuleNotFoundError: No module named 'pkg_resources'`**

    The pinned `python-tds` 1.12.0 dependency, loaded transitively by the generic database connectors through `sqlalchemy_pytds`, imports `pkg_resources` at module load time. Starting in 9.3.0, the official `elastic-connectors` Docker image no longer ships `setuptools` in the connector service's Python environment, and therefore does not provide `pkg_resources`. As a result, the connectors fail with `ModuleNotFoundError: No module named 'pkg_resources'` when attempting to connect to the data source, and syncs cannot start.

    **Affected versions**: `docker.elastic.co/integrations/elastic-connectors` images 9.3.0 and later. Earlier versions are not affected because their image still ships `setuptools`. Self-managed deployments that install `setuptools` into their Python environment are also unaffected.

    **Fix**: Tracked in [elastic/connectors#4014](https://github.com/elastic/connectors/issues/4014). The fix is to bump `python-tds` to `>=1.15.0`, where the `pkg_resources` import was removed.


* **Content Connectors entry in Stack Management is visible to users without the `content_connectors` capability**

    Even if a user did not have the `management.data.content_connectors` capability, they saw the **Content Connectors** entry in the Stack Management sidebar. Navigating to it returned a 403.

    **Affected versions**: Kibana 9.1 through 9.4.

    **Fix**: Resolved in [elastic/kibana#271709](https://github.com/elastic/kibana/pull/271709) and shipped in Kibana 9.3.6, 9.4.3, and 9.5.0


* **Jira Server/Data Center syncs fail to fetch issues**

    [elastic/connectors#3710](https://github.com/elastic/connectors/pull/3710) migrated the Jira issues endpoint to the cursor-based `rest/api/3/search/jql` endpoint. That endpoint is not available on Jira Server/Data Center pre-v10, so syncs against those instances fail when fetching issues.

    **Affected versions**: 8.18.8+, 8.19.5-8.19.16, 9.0.8+, 9.1.5+, 9.2.0+, 9.3.0–9.3.5, and 9.4.0–9.4.2. Jira Cloud is not affected.

    **Fix**: [elastic/connectors#4059](https://github.com/elastic/connectors/pull/4059), shipped in 8.19.17, 9.3.6, 9.4.3, and 9.5.0.


* **Outlook connector fails to sync on non-English Exchange servers**

    The connector resolved default folders by English display names (`Contacts`, `Archive`). On localized on-prem Exchange servers these names differ, raising `ErrorFolderNotFound` and aborting the sync.

    **Affected versions**: 8.11.0–8.19.16, 9.0.0–9.3.5, and 9.4.0–9.4.2. Non-English on-prem Exchange servers only.

    **Fix**: [elastic/connectors#4065](https://github.com/elastic/connectors/pull/4065), shipped in 8.19.17, 9.3.6, 9.4.3, and 9.5.0. Folders are now resolved by locale-agnostic distinguished folder IDs; the Archive leaf folder still has no distinguished ID in Exchange and is skipped on localized servers when absent.


* **Outlook connector fails when Active Directory users lack a mail attribute**

    On on-prem Exchange, the connector passed the raw LDAP `mail` attribute into `exchangelib.Account`. When the attribute is missing, `ldap3` returns `[]`, causing `ValueError: primary_smtp_address [] is not an email address` and aborting the sync.

    **Affected versions**: 8.11.0–8.19.16, 9.0.0–9.3.5, and 9.4.0–9.4.2. On-prem Exchange with Active Directory only.

    **Fix**: [elastic/connectors#4078](https://github.com/elastic/connectors/pull/4078), shipped in 8.19.17, 9.3.6, 9.4.3, and 9.5.0.


* **Outlook connector aborts the sync for mailbox-less accounts or when SSL is enabled without a certificate**

    On on-prem Exchange, AD users with an SMTP address but no mailbox caused `ErrorNonExistentMailbox` and aborted the whole sync. Separately, `ssl_enabled` with an empty certificate wrote an empty CA file and raised `NO_CERTIFICATE_OR_CRL_FOUND`.

    **Affected versions**: 8.11.0–8.19.17, 9.0.0–9.3.6, and 9.4.0–9.4.2. On-prem Exchange only.

    **Fix**: [elastic/connectors#4085](https://github.com/elastic/connectors/pull/4085), shipped in 8.19.18, 9.3.7, 9.4.3, and 9.5.0. Mailbox-less accounts are skipped with a warning; SSL with no certificate falls back to an unverified connection and logs a warning.


* **Outlook connector syncs intermittently fail with `NO_CERTIFICATE_OR_CRL_FOUND` when SSL is enabled**

    With SSL enabled, the connector wrote the configured CA to a fixed file on disk (`outlook_cert.cer`) shared across the process. Concurrent or overlapping syncs raced on it, causing an intermittent `SSLError: [X509] no certificate or crl found (NO_CERTIFICATE_OR_CRL_FOUND)` that aborted syncs with no configuration change between runs.

    **Affected versions**: 8.11.0–8.19.18, 9.0.0–9.3.7, and 9.4.0–9.4.3. On-prem Exchange with SSL enabled only.

    **Fix**: [elastic/connectors#4094](https://github.com/elastic/connectors/pull/4094), shipped in 8.19.19, 9.3.8, 9.4.4, and 9.5.0.


* **Confluence Data Center / Server syncs can fail with HTTP 500 and require site-admin credentials**

    Content search expanded unused `space.permissions` on Data Center / Server. That expansion can return HTTP 500 for non-administrator accounts (CONFSERVER-99908), which forced customers to over-grant site admin to the functional user. Confluence Cloud is not affected.

    **Affected versions**: 8.7.0–8.19.18, 9.0.0–9.3.7, and 9.4.0–9.4.3. Confluence Data Center / Server only.

    **Fix**: [elastic/connectors#4118](https://github.com/elastic/connectors/pull/4118), shipped in 8.19.19, 9.3.8, 9.4.4, and 9.5.0.


* **GitHub connector syncs can succeed while indexing little or no data**

    Page-level fetch failures were caught by a broad `except Exception`, logged as a warning, and swallowed. A sync could therefore complete successfully after failing to fetch issues, pull requests, or files — and the framework could delete previously indexed documents as a result.

    **Affected versions**: 8.10.0–8.19.18, 9.0.0–9.3.7, and 9.4.0–9.4.3.

    **Fix**: [elastic/connectors#4119](https://github.com/elastic/connectors/pull/4119), shipped in 8.19.19, 9.3.8, 9.4.4, and 9.5.0. Page-level fetch errors now fail the sync; only per-document enrichment errors are skipped.


* **Outlook connector aborts the sync when Exchange items have null field values**

    A single mail, calendar, contact, or attachment item with a missing nullable field (for example `mail.sender` → `'NoneType' object has no attribute 'email_address'`) aborted the entire sync. Optional folders that were absent also failed the account.

    **Affected versions**: 8.11.0–8.19.18, 9.0.0–9.3.7, and 9.4.0–9.4.3. On-prem Exchange only.

    **Fix**: [elastic/connectors#4123](https://github.com/elastic/connectors/pull/4123), shipped in 8.19.19, 9.3.8, 9.4.4, and 9.5.0.


* **Outlook connector aborts the sync when the Contacts folder contains a distribution list**

    The Contacts folder returns both `Contact` and `DistributionList` items, but the formatter assumed every item was a `Contact`, raising `'DistributionList' object has no attribute 'email_addresses'` and aborting the sync. Shared and resource mailboxes that lack Calendar or Tasks folders hit the same abort path.

    **Affected versions**: 8.11.0–8.19.18, 9.0.0–9.3.7, and 9.4.0–9.4.3. On-prem Exchange only.

    **Fix**: [elastic/connectors#4147](https://github.com/elastic/connectors/pull/4147), shipped in 8.19.19, 9.3.8, 9.4.4, 9.5.0, and 9.6.0.


* **MongoDB connector syncs fail on out-of-range BSON datetimes**

    Documents with dates outside the Python `datetime` range (years 1–9999) cause pymongo to raise `InvalidBSON` (for example `year 643385 is out of range`) and abort the sync. The default `datetime_conversion` value remains `DATETIME` (raise).

    **Affected versions**: All versions that use the default `DATETIME` conversion, including after the mitigation below.

    **Workaround**: In advanced configuration, set `datetime_conversion` to `DATETIME_CLAMP` so out-of-range values are clamped to valid dates and the sync can continue. See the [MongoDB connector reference](/reference/search-connectors/es-connectors-mongodb.md).

    **Fix**: Mitigation added in [elastic/connectors#4148](https://github.com/elastic/connectors/pull/4148), shipped in 8.19.19, 9.3.8, 9.4.4, 9.5.0, and 9.6.0.


* **Outlook connector aborts the sync on unexpected Exchange item types or folder errors**

    Folders could contain stray item types (for example a `CalendarItem` in a mail folder → `'CalendarItem' object has no attribute 'sender'`), or raise `ErrorManagedFolderNotFound` / `ErrorAccessDenied`. Any of these aborted the sync instead of skipping the bad item, folder, or account.

    **Affected versions**: 8.11.0–8.19.19, 9.0.0–9.3.8, and 9.4.0–9.4.4. On-prem Exchange only.

    **Fix**: [elastic/connectors#4158](https://github.com/elastic/connectors/pull/4158), shipped in 8.19.20, 9.3.9, 9.4.5, 9.5.0, and 9.6.0.


* **Outlook connector aborts calendar sync on unrecognised EWS elements**

    Some Exchange servers return elements such as `EndTimeZone` as siblings of calendar items. exchangelib raises `ValueError: Item type …EndTimeZone was unexpected in a BaseFolder folder` while loading the folder, which aborted the sync before per-item handling ran.

    **Affected versions**: 8.11.0–8.19.19, 9.0.0–9.3.8, and 9.4.0–9.4.4. On-prem Exchange only.

    **Fix**: [elastic/connectors#4287](https://github.com/elastic/connectors/pull/4287), shipped in 8.19.20, 9.3.9, 9.4.5, 9.5.0, and 9.6.0.


* **Outlook connector DLS hides mailbox content from its owner**

    With document-level security enabled, access control documents grant prefixed identities (`email:user@example.com`) while content documents stored the raw SMTP address. The DLS `terms` query intersection is empty, so mailbox owners see none of their own documents.

    **Affected versions**: All versions with Outlook DLS enabled, through 8.19.19, 9.3.8, and 9.4.4.

    **Fix**: [elastic/connectors#4291](https://github.com/elastic/connectors/pull/4291), shipped in 9.3.9, 9.4.5, 9.5.0, and 9.6.0. After upgrading, run a **full content sync** so `_allow_access_control` is rewritten on existing documents; an access control sync alone is not enough.


* **Confluence connector DLS over-grants access on pages with inherited restrictions**

    When a page inherits view restrictions from ancestors (or must satisfy both its own and parent restrictions), the connector ignored or incompletely applied the ancestor chain and fell back to broad space permissions. Users could see pages in Elasticsearch that they cannot view in Confluence.

    **Affected versions**: All versions with Confluence DLS enabled, through 8.19.19, 9.3.8, and 9.4.4. Cloud, Server, and Data Center.

    **Fix**: [elastic/connectors#4297](https://github.com/elastic/connectors/pull/4297), shipped in 8.19.20, 9.3.9, 9.4.5, 9.5.0, and 9.6.0. After upgrading, run a **full content sync** to rewrite `_allow_access_control`.


* **SharePoint Online syncs abort on the system list `SharePointHomeCacheList`**

    Microsoft Graph can return the system list `SharePointHomeCacheList`. Fetching its attachments via SharePoint REST returns Unauthorized and aborts the whole sync. Sync-rule exclusions cannot prevent this because they apply after the list is fetched.

    **Affected versions**: 8.9.0–8.19.19, 9.0.0–9.3.8, and 9.4.0–9.4.4.

    **Fix**: [elastic/connectors#4306](https://github.com/elastic/connectors/pull/4306), shipped in 8.19.20, 9.3.9, 9.4.5, 9.5.0, and 9.6.0.


## Individual connector known issues [es-connectors-known-issues-specific]

Individual connectors may have additional known issues. Refer to [each connector’s reference documentation](/reference/search-connectors/index.md) for connector-specific known issues.
