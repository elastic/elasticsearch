---
navigation_title: "Auditing settings"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/auditing-settings.html
applies_to:
  deployment:
    ess:
    self:
---

# Auditing security settings [auditing-settings]


$$$auditing-settings-description$$$
You can use [audit logging](docs-content://deploy-manage/security/logging-configuration/enabling-audit-logs.md) to record security-related events, such as authentication failures, refused connections, and data-access events. In addition, changes via the APIs to the security configuration, such as creating, updating and removing [native](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/native.md) and [built-in](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/built-in-users.md) users, [roles](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-put-role), [role mappings](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-put-role-mapping) and [API keys](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-create-api-key) are also recorded.

::::{tip}
Audit logs are only available on certain subscription levels. For more information, see [{{stack}} subscriptions](https://www.elastic.co/subscriptions).
::::

If configured, auditing settings must be set on every node in the cluster. Static settings, such as `xpack.security.audit.enabled`, must be configured in `elasticsearch.yml` on each node. For dynamic auditing settings, use the [cluster update settings API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings) to ensure the setting is the same on all nodes.

## General Auditing Settings [general-audit-settings]

$$$xpack-security-audit-enabled$$$

`xpack.security.audit.enabled` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Set to `true` to enable auditing on the node. The default value is `false`. This puts the auditing events in a dedicated file named `<clustername>_audit.json` on each node.

    If enabled, this setting must be configured in `elasticsearch.yml` on all nodes in the cluster.



## Audited Event Settings [event-audit-settings]

The events and some other information about what gets logged can be controlled by using the following settings:

$$$xpack-sa-lf-events-include$$$

`xpack.security.audit.logfile.events.include` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Specifies the [kind of events](/reference/elasticsearch/elasticsearch-audit-events.md) to print in the auditing output. In addition, `_all` can be used to exhaustively audit all the events, but this is usually discouraged since it will get very verbose. The default list value contains: `access_denied, access_granted, anonymous_access_denied, authentication_failed, connection_denied, tampered_request, run_as_denied, run_as_granted, security_config_change`.

$$$xpack-sa-lf-events-exclude$$$

`xpack.security.audit.logfile.events.exclude` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Excludes the specified [kind of events](/reference/elasticsearch/elasticsearch-audit-events.md) from the include list. This is useful in the case where the `events.include` setting contains the special value `_all`. The default is the empty list.

$$$xpack-sa-lf-events-emit-request$$$

`xpack.security.audit.logfile.events.emit_request_body` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Specifies whether to include the full request body from REST requests as an attribute of certain kinds of audit events. This setting can be used to [audit search queries](docs-content://deploy-manage/security/logging-configuration/auditing-search-queries.md).

    The default value is `false`, so request bodies are not printed.

    ::::{important}
    Be advised that sensitive data may be audited in plain text when including the request body in audit events, even though all the security APIs, such as those that change the user’s password, have the credentials filtered out when audited.
    ::::



## Local Node Info Settings [node-audit-settings]

$$$xpack-sa-lf-emit-node-name$$$

`xpack.security.audit.logfile.emit_node_name` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Specifies whether to include the [node name](docs-content://deploy-manage/deploy/self-managed/important-settings-configuration.md#node-name) as a field in each audit event. The default value is `false`.

$$$xpack-sa-lf-emit-node-host-address$$$

`xpack.security.audit.logfile.emit_node_host_address` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Specifies whether to include the node’s IP address as a field in each audit event. The default value is `false`.

$$$xpack-sa-lf-emit-node-host-name$$$

`xpack.security.audit.logfile.emit_node_host_name` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Specifies whether to include the node’s host name as a field in each audit event. The default value is `false`.

$$$xpack-sa-lf-emit-node-id$$$

`xpack.security.audit.logfile.emit_node_id` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Specifies whether to include the node id as a field in each audit event. Unlike [node name](docs-content://deploy-manage/deploy/self-managed/important-settings-configuration.md#node-name), whose value might change if the administrator changes the setting in the config file, the node id will persist across cluster restarts and the administrator cannot change it. The default value is `true`.


## Audit Logfile Event Ignore Policies [audit-event-ignore-policies]

The following settings affect the [ignore policies](docs-content://deploy-manage/security/logging-configuration/logfile-audit-events-ignore-policies.md) that enable fine-grained control over which audit events are printed to the log file. All of the settings with the same policy name combine to form a single policy. If an event matches all the conditions of any policy, it is ignored and not printed. Most audit events are subject to the ignore policies. The sole exception are events of the `security_config_change` type, which cannot be filtered out, unless [excluded](#xpack-sa-lf-events-exclude) altogether.

$$$xpack-sa-lf-events-ignore-users$$$

`xpack.security.audit.logfile.events.ignore_filters.<policy_name>.users` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) A list of user names or wildcards. The specified policy will not print audit events for users matching these values.

$$$xpack-sa-lf-events-ignore-realms$$$

`xpack.security.audit.logfile.events.ignore_filters.<policy_name>.realms` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) A list of authentication realm names or wildcards. The specified policy will not print audit events for users in these realms.

$$$xpack-sa-lf-events-ignore-actions$$$

`xpack.security.audit.logfile.events.ignore_filters.<policy_name>.actions` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) A list of action names or wildcards. Action name can be found in the `action` field of the audit event. The specified policy will not print audit events for actions matching these values.

$$$xpack-sa-lf-events-ignore-roles$$$

`xpack.security.audit.logfile.events.ignore_filters.<policy_name>.roles` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) A list of role names or wildcards. The specified policy will not print audit events for users that have these roles. If the user has several roles, some of which are **not** covered by the policy, the policy will **not** cover this event.

$$$xpack-sa-lf-events-ignore-indices$$$

`xpack.security.audit.logfile.events.ignore_filters.<policy_name>.indices` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) A list of index names or wildcards. The specified policy will not print audit events when all the indices in the event match these values. If the event concerns several indices, some of which are **not** covered by the policy, the policy will **not** cover this event.


