/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;


import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.SessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_HOSTS_PROVIDER_SETTING;
import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_TYPE_SETTING;
import static org.elasticsearch.discovery.DiscoverySettings.NO_MASTER_BLOCK_SETTING;
import static org.elasticsearch.discovery.zen.SettingsBasedHostsProvider.DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING;
import static org.elasticsearch.xpack.core.XPackSettings.SECURITY_ENABLED;
import static org.elasticsearch.xpack.core.XPackSettings.TRANSPORT_SSL_ENABLED;

/**
 * Node-specific deprecation checks
 */
public class NodeDeprecationChecks {

    static DeprecationIssue httpEnabledSettingRemoved(Settings nodeSettings, PluginsAndModules plugins) {
        if (nodeSettings.hasValue(NetworkModule.HTTP_ENABLED.getKey())) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "HTTP Enabled setting removed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#remove-http-enabled",
                "the HTTP Enabled setting has been removed");
        }
        return null;
    }

    static DeprecationIssue noMasterBlockRenamed(Settings nodeSettings, PluginsAndModules plugins) {
        if (nodeSettings.hasValue(NO_MASTER_BLOCK_SETTING.getKey())) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Master block setting renamed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#_new_name_for_literal_no_master_block_literal_setting",
                "The setting discovery.zen.no_master_block will be renamed to cluster.no_master_block in 7.0. " +
                    "Please unset discovery.zen.no_master_block and set cluster.no_master_block after upgrading to 7.0.");
        }
        return null;
    }

    static DeprecationIssue auditLogPrefixSettingsCheck(Settings nodeSettings, PluginsAndModules plugins) {
        if (nodeSettings.getByPrefix("xpack.security.audit.logfile.prefix").isEmpty() == false) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Audit log node info settings renamed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
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
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#remove-audit-index-output",
                "recommended replacement is the logfile audit output type");
        }
        return null;
    }

    static DeprecationIssue indexThreadPoolCheck(Settings nodeSettings, PluginsAndModules plugins) {
        if (nodeSettings.getByPrefix("thread_pool.index.").isEmpty() == false) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Index thread pool removed in favor of combined write thread pool",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#_index_thread_pool",
                "the write threadpool is now used for all writes");
        }
        return null;
    }

    static DeprecationIssue bulkThreadPoolCheck(Settings nodeSettings, PluginsAndModules plugins) {
        if (nodeSettings.getByPrefix("thread_pool.bulk.").isEmpty() == false) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Bulk thread pool renamed to write thread pool",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#write-thread-pool-fallback",
                "the write threadpool is now used for all writes");
        }
        return null;
    }

    static DeprecationIssue tribeNodeCheck(Settings nodeSettings, PluginsAndModules plugins) {
        if (nodeSettings.getByPrefix("tribe.").isEmpty() == false) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Tribe Node removed in favor of Cross Cluster Search",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#_tribe_node_removed",
                "tribe node functionality has been removed in favor of cross-cluster search");
        }
        return null;
    }

    static DeprecationIssue authRealmsTypeCheck(Settings nodeSettings, PluginsAndModules plugins) {
        if (nodeSettings.getGroups("xpack.security.authc.realms").size() > 0) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Security realm settings structure changed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#include-realm-type-in-setting",
                "these settings must be updated to the new format while the node is offline during the upgrade to 7.0");
        }
        return null;
    }

    static DeprecationIssue httpPipeliningCheck(Settings nodeSettings, PluginsAndModules plugins) {
        if (nodeSettings.hasValue(HttpTransportSettings.SETTING_PIPELINING.getKey())) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "HTTP pipelining setting removed as pipelining is now mandatory",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
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
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#_discovery_configuration_is_required_in_production",
                "");
        }
        return null;
    }

    static DeprecationIssue watcherNotificationsSecureSettingsCheck(Settings nodeSettings, PluginsAndModules plugins) {
        if (false == nodeSettings.getByPrefix("xpack.notification.email.account.").filter(s -> s.endsWith(".smtp.password")).isEmpty()
            || false == nodeSettings.getByPrefix("xpack.notification.jira.account.")
                .filter(s -> s.endsWith(".url") || s.endsWith(".user") || s.endsWith(".password")).isEmpty()
            || false == nodeSettings.getByPrefix("xpack.notification.pagerduty.account.")
                .filter(s -> s.endsWith(".service_api_key")).isEmpty()
            || false == nodeSettings.getByPrefix("xpack.notification.slack.account.").filter(s -> s.endsWith(".url")).isEmpty()) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Watcher notification accounts' authentication settings must be defined securely",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#watcher-notifications-account-settings",
                "account authentication settings must use the keystore");
        }
        return null;
    }

    static DeprecationIssue watcherHipchatNotificationSettingsCheck(Settings nodeSettings, PluginsAndModules plugins) {
        if (nodeSettings.getByPrefix("xpack.notification.hipchat.").size() > 0) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Watcher Hipchat notifications will be removed in the next major release",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#watcher-notifications-account-settings",
                "[hipchat] actions are deprecated and should be removed from watch definitions");
        }
        return null;
    }

    static DeprecationIssue azureRepositoryChanges(Settings nodeSettings, PluginsAndModules plugins) {
        if (plugins.getPluginInfos().stream().anyMatch(pluginInfo -> "repository-azure".equals(pluginInfo.getName()))) {
            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "Azure Repository settings changed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#_azure_repository_plugin",
                "see breaking changes list for details");
        }
        return null;
    }

    static DeprecationIssue gcsRepositoryChanges(Settings nodeSettings, PluginsAndModules plugins) {
        if (plugins.getPluginInfos().stream().anyMatch(pluginInfo -> "repository-gcs".equals(pluginInfo.getName()))) {

            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "GCS Repository settings changed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#_google_cloud_storage_repository_plugin",
                "see breaking changes list for details");
        }
        return null;
    }

    static DeprecationIssue fileDiscoveryPluginRemoved(Settings nodeSettings, PluginsAndModules plugins) {
        if (plugins.getPluginInfos().stream().anyMatch(pluginInfo -> "discovery-file".equals(pluginInfo.getName()))) {
            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "File-based discovery is no longer a plugin and uses a different path",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
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
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#tls-setting-fallback",
                "each component must have TLS/SSL configured explicitly");
        }
        return null;
    }

    static DeprecationIssue tlsv1ProtocolDisabled(Settings nodeSettings, PluginsAndModules plugins) {
        final Set<String> contexts = new TreeSet<>();

        nodeSettings.keySet().stream()
            .filter(key -> key.contains(".ssl."))
            .map(key -> key.replaceAll("\\.ssl\\..*$", ".ssl"))
            .distinct()
            .filter(sslPrefix -> nodeSettings.hasValue(sslPrefix + ".supported_protocols") == false)
            .forEach(contexts::add);
        final Map<String, Settings> realms = RealmSettings.getRealmSettings(nodeSettings);
        realms.forEach((name, realmSettings) -> {
            final String type = realmSettings.get("type");
            final String sslPrefix = RealmSettings.PREFIX + name + ".ssl";
            if (LdapRealmSettings.LDAP_TYPE.equals(type) || LdapRealmSettings.AD_TYPE.equals(type)) {
                final List<String> urls = realmSettings.getAsList(SessionFactorySettings.URLS_SETTING);
                if (urls != null && urls.stream().anyMatch(u -> u.startsWith("ldaps://"))) {
                    if (nodeSettings.hasValue(sslPrefix + ".supported_protocols") == false) {
                        contexts.add(sslPrefix);
                    }
                }
            } else if (SamlRealmSettings.TYPE.equals(type)) {
                final String path = SamlRealmSettings.IDP_METADATA_PATH.get(realmSettings);
                if (Strings.hasText(path) && path.startsWith("https://")) {
                    if (nodeSettings.hasValue(sslPrefix + ".supported_protocols") == false) {
                        contexts.add(sslPrefix);
                    }
                }
            }
        });

        final Map<String, Settings> mon = nodeSettings.getGroups("xpack.monitoring.exporters");
        for (Map.Entry<String, Settings> entry : mon.entrySet()) {
            final List<String> hosts = entry.getValue().getAsList("host");
            if (hosts != null && hosts.stream().anyMatch(h -> h.startsWith("https://"))) {
                String sslPrefix = "xpack.monitoring.exporters." + entry.getKey() + ".ssl";
                if (nodeSettings.hasValue(sslPrefix + ".supported_protocols") == false) {
                    contexts.add(sslPrefix);
                }
            }
        }

        if (contexts.size() > 0) {
            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "TLS v1.0 has been removed from default TLS/SSL protocols",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#tls-v1-removed",
                "These ssl contexts rely on the default TLS/SSL protocols: " + contexts);
        }
        return null;
    }

    static DeprecationIssue transportSslEnabledWithoutSecurityEnabled(Settings nodeSettings, PluginsAndModules plugins) {
        if (TRANSPORT_SSL_ENABLED.get(nodeSettings) && nodeSettings.hasValue(SECURITY_ENABLED.getKey()) == false) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "TLS/SSL in use, but security not explicitly enabled",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#trial-explicit-security",
                "security should be explicitly enabled (with [" + SECURITY_ENABLED.getKey() +
                    "]), it will no longer be automatically enabled when transport SSL is enabled ([" +
                    TRANSPORT_SSL_ENABLED.getKey() + "])");
        }
        return null;
    }
}
