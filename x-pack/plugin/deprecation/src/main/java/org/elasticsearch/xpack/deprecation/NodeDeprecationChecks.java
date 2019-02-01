/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;


import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_HOSTS_PROVIDER_SETTING;
import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_TYPE_SETTING;
import static org.elasticsearch.discovery.zen.SettingsBasedHostsProvider.DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING;

/**
 * Node-specific deprecation checks
 */
public class NodeDeprecationChecks {

    static DeprecationIssue httpEnabledSettingRemoved(Settings nodeSettings, PluginsAndModules plugins) {
        if (nodeSettings.hasValue(NetworkModule.HTTP_ENABLED.getKey())) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "HTTP Enabled setting removed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#remove-http-enabled",
                "the HTTP Enabled setting has been removed");
        }
        return null;
    }

    static DeprecationIssue auditLogPrefixSettingsCheck(Settings nodeSettings, PluginsAndModules plugins) {
        if (nodeSettings.getByPrefix("xpack.security.audit.logfile.prefix").isEmpty() == false) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Audit log node info settings renamed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#audit-logfile-local-node-info",
                "the audit log is now structured JSON");
        }
        return null;
    }

    static DeprecationIssue auditIndexSettingsCheck(Settings nodeSettings, PluginsAndModules plugins) {
        if (nodeSettings.getByPrefix("xpack.security.audit.outputs").isEmpty() == false
            || nodeSettings.getByPrefix("xpack.security.audit.index").isEmpty() == false) {

            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Audit index output type removed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#remove-audit-index-output",
                "recommended replacement is the logfile audit output type");
        }
        return null;
    }

    static DeprecationIssue indexThreadPoolCheck(Settings nodeSettings, PluginsAndModules plugins) {
        if (nodeSettings.getByPrefix("thread_pool.index.").isEmpty() == false) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Index thread pool removed in favor of combined write thread pool",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#_index_thread_pool",
                "the write threadpool is now used for all writes");
        }
        return null;
    }

    static DeprecationIssue bulkThreadPoolCheck(Settings nodeSettings, PluginsAndModules plugins) {
        if (nodeSettings.getByPrefix("thread_pool.bulk.").isEmpty() == false) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Bulk thread pool renamed to write thread pool",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#write-thread-pool-fallback",
                "the write threadpool is now used for all writes");
        }
        return null;
    }

    static DeprecationIssue tribeNodeCheck(Settings nodeSettings, PluginsAndModules plugins) {
        if (nodeSettings.getByPrefix("tribe.").isEmpty() == false) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Tribe Node removed in favor of Cross Cluster Search",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#_tribe_node_removed",
                "tribe node functionality has been removed in favor of cross-cluster search");
        }
        return null;
    }

    static DeprecationIssue authRealmsTypeCheck(Settings nodeSettings, PluginsAndModules plugins) {
        if (nodeSettings.getGroups("xpack.security.authc.realms").size() > 0) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Security realm settings structure changed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#include-realm-type-in-setting",
                "these settings must be updated to the new format while the node is offline during the upgrade to 7.0");
        }
        return null;
    }

    static DeprecationIssue httpPipeliningCheck(Settings nodeSettings, PluginsAndModules plugins) {
        if (nodeSettings.hasValue(HttpTransportSettings.SETTING_PIPELINING.getKey())) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "HTTP pipelining setting removed as pipelining is now mandatory",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#remove-http-pipelining-setting",
                "");
        }
        return null;
    }

    static DeprecationIssue discoveryConfigurationCheck(Settings nodeSettings, PluginsAndModules plugins) {
        // These checks only apply in Zen2, which is the new default in 7.0 and can't be used in 6.x, so only apply the checks if this
        // node does not have a discovery type explicitly set
        if (nodeSettings.hasValue(DISCOVERY_TYPE_SETTING.getKey()) == false
            // This only checks for `ping.unicast.hosts` and `hosts_provider` because `cluster.initial_master_nodes` does not exist in 6.x
            && nodeSettings.hasValue(DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING.getKey()) == false
            && nodeSettings.hasValue(DISCOVERY_HOSTS_PROVIDER_SETTING.getKey()) == false) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Discovery configuration is required in production mode",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#_discovery_configuration_is_required_in_production",
                "");
        }
        return null;
    }

    static DeprecationIssue watcherNotificationsSecureSettingsCheck(Settings nodeSettings, PluginsAndModules plugins) {
        if (false == nodeSettings.getByPrefix("xpack.notification.email.account.").filter(s -> s.endsWith(".smtp.password")).isEmpty()
            || false == nodeSettings.getByPrefix("xpack.notification.hipchat.account.").filter(s -> s.endsWith(".auth_token")).isEmpty()
            || false == nodeSettings.getByPrefix("xpack.notification.jira.account.")
                .filter(s -> s.endsWith(".url") || s.endsWith(".user") || s.endsWith(".password")).isEmpty()
            || false == nodeSettings.getByPrefix("xpack.notification.pagerduty.account.")
                .filter(s -> s.endsWith(".service_api_key")).isEmpty()
            || false == nodeSettings.getByPrefix("xpack.notification.slack.account.").filter(s -> s.endsWith(".url")).isEmpty()) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Watcher notification accounts' authentication settings must be defined securely",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#watcher-notifications-account-settings",
                "account authentication settings must use the keystore");
        }
        return null;
    }

    static DeprecationIssue azureRepositoryChanges(Settings nodeSettings, PluginsAndModules plugins) {
        if (plugins.getPluginInfos().stream().anyMatch(pluginInfo -> "repository-azure".equals(pluginInfo.getName()))) {
            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "Azure Repository settings changed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#_azure_repository_plugin",
                "see breaking changes list for details");
        }
        return null;
    }

    static DeprecationIssue gcsRepositoryChanges(Settings nodeSettings, PluginsAndModules plugins) {
        if (plugins.getPluginInfos().stream().anyMatch(pluginInfo -> "repository-gcs".equals(pluginInfo.getName()))) {

            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "GCS Repository settings changed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#_google_cloud_storage_repository_plugin",
                "see breaking changes list for details");
        }
        return null;
    }

    static DeprecationIssue fileDiscoveryPluginRemoved(Settings nodeSettings, PluginsAndModules plugins) {
        if (plugins.getPluginInfos().stream().anyMatch(pluginInfo -> "discovery-file".equals(pluginInfo.getName()))) {
            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "File-based discovery is no longer a plugin and uses a different path",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#_file_based_discovery_plugin",
                "the location of the hosts file used for file-based discovery has changed to $ES_PATH_CONF/unicast_hosts.txt " +
                    "(from $ES_PATH_CONF/file-discovery/unicast_hosts.txt)");
        }
        return null;
    }

    static DeprecationIssue defaultSSLSettingsRemoved(Settings nodeSettings, PluginsAndModules plugins) {
        if (nodeSettings.getByPrefix("xpack.ssl").isEmpty() == false) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Default TLS/SSL settings have been removed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#tls-setting-fallback",
                "each component must have TLS/SSL configured explicitly");
        }
        return null;
    }
}
