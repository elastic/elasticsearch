/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING;
import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_HOSTS_PROVIDER_SETTING;
import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_TYPE_SETTING;
import static org.elasticsearch.discovery.DiscoverySettings.NO_MASTER_BLOCK_SETTING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.NODE_SETTINGS_CHECKS;

public class NodeDeprecationChecksTests extends ESTestCase {
    private DiscoveryNode discoveryNode;
    private FsInfo.Path[] paths;
    private OsInfo osInfo;
    private PluginsAndModules pluginsAndModules;

    @Before
    public void setupDefaults() {
        discoveryNode = DiscoveryNode.createLocal(Settings.builder().put("node.name", "node_check").build(),
            new TransportAddress(TransportAddress.META_ADDRESS, 9200), "test");
        paths = new FsInfo.Path[] {};
        osInfo = new OsInfo(0L, 1, 1, randomAlphaOfLength(10),
            "foo-64", randomAlphaOfLength(10), randomAlphaOfLength(10));
        pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
    }

    private void assertSettingsAndIssue(String key, String value, DeprecationIssue expected) {
        assertSettingsAndIssues(Settings.builder().put(key, value).build(), expected);
    }

    private void assertSettingsAndIssues(Settings nodeSettings, DeprecationIssue... expected) {
        Settings settings = Settings.builder()
            .put(CLUSTER_NAME_SETTING.getKey(), "elasticsearch")
            .put(NODE_NAME_SETTING.getKey(), "node_check")
            .put(DISCOVERY_TYPE_SETTING.getKey(), "single-node") // Needed due to NodeDeprecationChecks#discoveryConfigurationCheck
            .put(nodeSettings)
            .build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(NODE_SETTINGS_CHECKS, c -> c.apply(settings, pluginsAndModules));
        assertThat(issues, Matchers.containsInAnyOrder(expected));
    }

    private void assertNoIssue(Settings settings) {
        Settings nodeSettings = Settings.builder()
            .put(settings)
            .put(CLUSTER_NAME_SETTING.getKey(), "elasticsearch")
            .put(NODE_NAME_SETTING.getKey(), "node_check")
            .put(DISCOVERY_TYPE_SETTING.getKey(), "single-node") // Needed due to NodeDeprecationChecks#discoveryConfigurationCheck
            .build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(NODE_SETTINGS_CHECKS, c -> c.apply(nodeSettings, pluginsAndModules));
        assertThat(issues, Matchers.empty());
    }

    public void testHttpEnabledCheck() {
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
            "HTTP Enabled setting removed",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#remove-http-enabled",
            "the HTTP Enabled setting has been removed");
        assertSettingsAndIssue("http.enabled", Boolean.toString(randomBoolean()), expected);
    }

    public void testNoMasterBlockRenamed() {
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
            "Master block setting renamed",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#_new_name_for_literal_no_master_block_literal_setting",
            "The setting discovery.zen.no_master_block will be renamed to cluster.no_master_block in 7.0. " +
                "Please unset discovery.zen.no_master_block and set cluster.no_master_block after upgrading to 7.0.");

        assertSettingsAndIssue(NO_MASTER_BLOCK_SETTING.getKey(), randomFrom("all", "write"), expected);
    }

    public void testAuditLoggingPrefixSettingsCheck() {
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
            "Audit log node info settings renamed",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#audit-logfile-local-node-info",
            "the audit log is now structured JSON");
        assertSettingsAndIssue("xpack.security.audit.logfile.prefix.emit_node_host_address",
            Boolean.toString(randomBoolean()), expected);
        assertSettingsAndIssue("xpack.security.audit.logfile.prefix.emit_node_host_name", Boolean.toString(randomBoolean()), expected);
        assertSettingsAndIssue("xpack.security.audit.logfile.prefix.emit_node_name", Boolean.toString(randomBoolean()), expected);
    }

    public void testAuditIndexSettingsCheck() {
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL, "Audit index output type removed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html"
                    + "#remove-audit-index-output",
                "recommended replacement is the logfile audit output type");
        assertSettingsAndIssue("xpack.security.audit.outputs", randomFrom("[index]", "[\"index\", \"logfile\"]"), expected);
        assertSettingsAndIssue("xpack.security.audit.index.events.emit_request_body", Boolean.toString(randomBoolean()), expected);

        Settings goodAuditSslSettings = Settings.builder()
            .put("xpack.security.audit.index.client.xpack.security.transport.ssl.enabled", true)
            .put("xpack.security.audit.index.client.xpack.security.transport.ssl.supported_protocols", "TLSv1.2,TLSv1.1")
            .build();
        assertSettingsAndIssues(goodAuditSslSettings, expected);
        assertSettingsAndIssue("xpack.security.audit.index.client.cluster.name", randomAlphaOfLength(4), expected);
        assertSettingsAndIssue("xpack.security.audit.index.settings.index.number_of_shards", Integer.toString(randomInt()), expected);
        assertSettingsAndIssue("xpack.security.audit.index.events.include",
                randomFrom("anonymous_access_denied", "authentication_failed", "realm_authentication_failed"), expected);
        assertSettingsAndIssue("xpack.security.audit.index.events.exclude",
                randomFrom("anonymous_access_denied", "authentication_failed", "realm_authentication_failed"), expected);
    }

    public void testIndexThreadPoolCheck() {
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
            "Index thread pool removed in favor of combined write thread pool",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#_index_thread_pool",
            "the write threadpool is now used for all writes");
        assertSettingsAndIssue("thread_pool.index.size", Integer.toString(randomIntBetween(1, 20000)), expected);
        assertSettingsAndIssue("thread_pool.index.queue_size", Integer.toString(randomIntBetween(1, 20000)), expected);
    }

    public void testBulkThreadPoolCheck() {
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
            "Bulk thread pool renamed to write thread pool",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#write-thread-pool-fallback",
            "the write threadpool is now used for all writes");
        assertSettingsAndIssue("thread_pool.bulk.size", Integer.toString(randomIntBetween(1, 20000)), expected);
        assertSettingsAndIssue("thread_pool.bulk.queue_size", Integer.toString(randomIntBetween(1, 20000)), expected);
    }

    public void testWatcherNotificationsSecureSettings() {
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Watcher notification accounts' authentication settings must be defined securely",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html"
                        + "#watcher-notifications-account-settings",
                "account authentication settings must use the keystore");
        assertSettingsAndIssue("xpack.notification.email.account." + randomAlphaOfLength(4) + ".smtp.password", randomAlphaOfLength(4),
                expected);
        expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Watcher notification accounts' authentication settings must be defined securely",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html"
                        + "#watcher-notifications-account-settings",
                "account authentication settings must use the keystore");
        assertSettingsAndIssue("xpack.notification.jira.account." + randomAlphaOfLength(4) + ".url", randomAlphaOfLength(4), expected);
        assertSettingsAndIssue("xpack.notification.jira.account." + randomAlphaOfLength(4) + ".user", randomAlphaOfLength(4), expected);
        assertSettingsAndIssue("xpack.notification.jira.account." + randomAlphaOfLength(4) + ".password",
            randomAlphaOfLength(4), expected);
        expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Watcher notification accounts' authentication settings must be defined securely",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html"
                        + "#watcher-notifications-account-settings",
                "account authentication settings must use the keystore");
        assertSettingsAndIssue("xpack.notification.pagerduty.account." + randomAlphaOfLength(4) + ".service_api_key",
                randomAlphaOfLength(4), expected);
        expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Watcher notification accounts' authentication settings must be defined securely",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html"
                        + "#watcher-notifications-account-settings",
                "account authentication settings must use the keystore");
        assertSettingsAndIssue("xpack.notification.slack.account." + randomAlphaOfLength(4) + ".url", randomAlphaOfLength(4), expected);
    }

    public void testWatcherHipchatSettings() {
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
            "Watcher Hipchat notifications will be removed in the next major release",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#watcher-notifications-account-settings",
            "[hipchat] actions are deprecated and should be removed from watch definitions");
        assertSettingsAndIssues(Settings.builder().put("xpack.notification.hipchat.account.profile", randomAlphaOfLength(4)).build(),
            expected);
    }

    public void testTribeNodeCheck() {
        String tribeSetting = "tribe." + randomAlphaOfLengthBetween(1, 20) + ".cluster.name";
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
            "Tribe Node removed in favor of Cross Cluster Search",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#_tribe_node_removed",
            "tribe node functionality has been removed in favor of cross-cluster search");
        assertSettingsAndIssue(tribeSetting, randomAlphaOfLength(5), expected);
    }

    public void testAuthenticationRealmTypeCheck() {
        String realm = randomAlphaOfLengthBetween(1, 20);
        String authRealmType = "xpack.security.authc.realms." + realm + ".type";
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
            "Security realm settings structure changed",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#include-realm-type-in-setting",
            "these settings must be updated to the new format while the node is offline during the upgrade to 7.0");
        assertSettingsAndIssue(authRealmType, randomAlphaOfLength(5), expected);
    }

    public void testHttpPipeliningCheck() {
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
            "HTTP pipelining setting removed as pipelining is now mandatory",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#remove-http-pipelining-setting",
            "");
        assertSettingsAndIssue("http.pipelining", Boolean.toString(randomBoolean()), expected);
    }

    public void testDiscoveryConfigurationCheck() {
        List<NodeStats> nodeStats = Collections.singletonList(new NodeStats(discoveryNode, 0L, null,
            null, null, null, null, new FsInfo(0L, null, paths), null, null, null,
            null, null, null, null));
        Settings baseSettings = Settings.builder()
            .put(CLUSTER_NAME_SETTING.getKey(), "elasticsearch")
            .put(NODE_NAME_SETTING.getKey(), "node_check")
            .build();

        {
            Settings hostsProviderSettings = Settings.builder().put(baseSettings)
                .put(DISCOVERY_HOSTS_PROVIDER_SETTING.getKey(), "file")
                .build();
            List<NodeInfo> nodeInfos = Collections.singletonList(new NodeInfo(Version.CURRENT, Build.CURRENT,
                discoveryNode, hostsProviderSettings, osInfo, null, null,
                null, null, null, pluginsAndModules, null, null));

            List<DeprecationIssue> issues = DeprecationChecks.filterChecks(NODE_SETTINGS_CHECKS,
                c -> c.apply(hostsProviderSettings, pluginsAndModules));
            assertTrue(issues.isEmpty());
        }

        {
            Settings hostsProviderSettings = Settings.builder().put(baseSettings)
                .put("discovery.zen.ping.unicast.hosts", "[1.2.3.4, 4.5.6.7]")
                .build();
            List<NodeInfo> nodeInfos = Collections.singletonList(new NodeInfo(Version.CURRENT, Build.CURRENT,
                discoveryNode, hostsProviderSettings, osInfo, null, null,
                null, null, null, pluginsAndModules, null, null));

            List<DeprecationIssue> issues = DeprecationChecks.filterChecks(NODE_SETTINGS_CHECKS,
                c -> c.apply(hostsProviderSettings, pluginsAndModules));
            assertTrue(issues.isEmpty());
        }

        {
            Settings hostsProviderSettings = Settings.builder().put(baseSettings)
                .build();
            List<NodeInfo> nodeInfos = Collections.singletonList(new NodeInfo(Version.CURRENT, Build.CURRENT,
                discoveryNode, hostsProviderSettings, osInfo, null, null,
                null, null, null, pluginsAndModules, null, null));

            DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Discovery configuration is required in production mode",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#_discovery_configuration_is_required_in_production",
                "");
            List<DeprecationIssue> issues = DeprecationChecks.filterChecks(NODE_SETTINGS_CHECKS,
                c -> c.apply(hostsProviderSettings, pluginsAndModules));
            assertEquals(singletonList(expected), issues);
        }

    }

    public void testAzurePluginCheck() {
        Version esVersion = VersionUtils.randomVersionBetween(random(), Version.V_6_0_0, Version.CURRENT);
        PluginInfo deprecatedPlugin = new PluginInfo(
            "repository-azure", "dummy plugin description", "dummy_plugin_version", esVersion,
            "javaVersion", "DummyPluginName", Collections.emptyList(), false);
        pluginsAndModules = new PluginsAndModules(Collections.singletonList(deprecatedPlugin), Collections.emptyList());

        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.WARNING,
            "Azure Repository settings changed",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#_azure_repository_plugin",
            "see breaking changes list for details");
        assertSettingsAndIssue("foo", "bar", expected);
    }

    public void testGCSPluginCheck() {
        Version esVersion = VersionUtils.randomVersionBetween(random(), Version.V_6_0_0, Version.CURRENT);
        PluginInfo deprecatedPlugin = new PluginInfo(
            "repository-gcs", "dummy plugin description", "dummy_plugin_version", esVersion,
            "javaVersion", "DummyPluginName", Collections.emptyList(), false);
        pluginsAndModules = new PluginsAndModules(Collections.singletonList(deprecatedPlugin), Collections.emptyList());

        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.WARNING,
            "GCS Repository settings changed",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#_google_cloud_storage_repository_plugin",
            "see breaking changes list for details");
        assertSettingsAndIssue("foo", "bar", expected);
    }

    public void testFileDiscoveryPluginCheck() {
        Version esVersion = VersionUtils.randomVersionBetween(random(), Version.V_6_0_0, Version.CURRENT);
        PluginInfo deprecatedPlugin = new PluginInfo(
            "discovery-file", "dummy plugin description", "dummy_plugin_version", esVersion,
            "javaVersion", "DummyPluginName", Collections.emptyList(), false);
        pluginsAndModules = new PluginsAndModules(Collections.singletonList(deprecatedPlugin), Collections.emptyList());

        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.WARNING,
            "File-based discovery is no longer a plugin and uses a different path",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#_file_based_discovery_plugin",
            "the location of the hosts file used for file-based discovery has changed to $ES_PATH_CONF/unicast_hosts.txt " +
                "(from $ES_PATH_CONF/file-discovery/unicast_hosts.txt)");
        assertSettingsAndIssue("foo", "bar", expected);
    }

    public void testDefaultSSLSettingsCheck() {
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
            "Default TLS/SSL settings have been removed",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#tls-setting-fallback",
            "each component must have TLS/SSL configured explicitly");
        assertSettingsAndIssues(Settings.builder()
            .put("xpack.ssl.keystore.path", randomAlphaOfLength(8))
            .putList("xpack.ssl.supported_protocols", randomAlphaOfLength(8))
            .build(), expected);
        assertSettingsAndIssues(Settings.builder()
            .put("xpack.ssl.truststore.password", randomAlphaOfLengthBetween(2, 12))
            .putList("xpack.ssl.supported_protocols", randomAlphaOfLength(8))
            .build(), expected);
        assertSettingsAndIssues(Settings.builder()
            .put("xpack.ssl.certificate_authorities",
                Strings.arrayToCommaDelimitedString(randomArray(1, 4, String[]::new, () -> randomAlphaOfLengthBetween(4, 16))))
            .putList("xpack.ssl.supported_protocols", randomAlphaOfLength(8))
            .build(),
            expected);
    }

    public void testTlsv1ProtocolDisabled() {
        DeprecationIssue[] expected = {
            new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "TLS v1.0 has been removed from default TLS/SSL protocols",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html#tls-v1-removed",
                "These ssl contexts rely on the default TLS/SSL protocols: [" +
                    "xpack.http.ssl, " +
                    "xpack.monitoring.exporters.ems.ssl, " +
                    "xpack.monitoring.exporters.foo.ssl, " +
                    "xpack.security.authc.realms.ldap1.ssl]"),
            new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Security realm settings structure changed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#include-realm-type-in-setting",
                "these settings must be updated to the new format while the node is offline during the upgrade to 7.0")
        };

        Settings settings = Settings.builder()
            .put("xpack.security.enabled", true)
            .put("xpack.http.ssl.verification_mode", "none")
            .put("xpack.security.http.ssl.keystore.path", "/path/to/keystore.p12")
            .putList("xpack.security.http.ssl.supported_protocols", "TLSv1.2", "TLSv1.1", "TLSv1")
            .put("xpack.security.transport.ssl.enabled", true)
            .putList("xpack.security.transport.ssl.supported_protocols", randomAlphaOfLengthBetween(3,6))
            .putList("xpack.monitoring.exporters.ems.ssl.certificate_authorities", "/path/to/ca.pem")
            .putList("xpack.monitoring.exporters.bar.host", "https://foo.example.net/")
            .putList("xpack.monitoring.exporters.bar.ssl.supported_protocols", randomAlphaOfLengthBetween(3,6))
            .putList("xpack.monitoring.exporters.foo.host", "https://foo.example.net/")
            .put("xpack.security.authc.realms.ldap1.type", "ldap")
            .putList("xpack.security.authc.realms.ldap1.url", "ldaps://my.ldap.example.net/")
            .put("xpack.security.authc.realms.ldap2.type", "ldap")
            .putList("xpack.security.authc.realms.ldap2.url", "ldaps://your.ldap.example.net/")
            .putList("xpack.security.authc.realms.ldap2.ssl.supported_protocols", generateRandomStringArray(3, 5, false, false))
            .build();
        assertSettingsAndIssues(settings, expected);
    }

    public void testTransportSslEnabledWithoutSecurityEnabled() {
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
            "TLS/SSL in use, but security not explicitly enabled",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#trial-explicit-security",
            "security should be explicitly enabled (with [xpack.security.enabled])," +
                " it will no longer be automatically enabled when transport SSL is enabled ([xpack.security.transport.ssl.enabled])");
        assertSettingsAndIssues(Settings.builder()
            .put("xpack.security.transport.ssl.enabled", true)
            .putList("xpack.security.transport.ssl.supported_protocols", "TLS1.2", "TLS1.0")
            .build(), expected);
        assertNoIssue(Settings.builder()
            .put("xpack.security.enabled", randomBoolean())
            .put("xpack.security.transport.ssl.enabled", randomBoolean())
            .putList("xpack.security.transport.ssl.supported_protocols", "TLS1.2", "TLS1.0")
            .build());
    }
}
