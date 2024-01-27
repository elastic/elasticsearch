/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.sniff.Sniffer;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.monitoring.MonitoringTemplateRegistry;
import org.elasticsearch.xpack.monitoring.exporter.ClusterAlertsUtil;
import org.elasticsearch.xpack.monitoring.exporter.ExportBulk;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.exporter.Exporter.Config;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringMigrationCoordinator;
import org.junit.Before;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests {@link HttpExporter}.
 */
public class HttpExporterTests extends ESTestCase {

    private final ClusterService clusterService = mock(ClusterService.class);
    private final XPackLicenseState licenseState = mock(XPackLicenseState.class);
    private final Metadata metadata = mock(Metadata.class);

    private final SSLService sslService = mock(SSLService.class);
    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

    @Before
    public void setupClusterState() {
        final ClusterState clusterState = mock(ClusterState.class);
        final DiscoveryNodes nodes = mock(DiscoveryNodes.class);

        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(clusterState.nodes()).thenReturn(nodes);
        // always let the watcher resources run for these tests; HttpExporterResourceTests tests it flipping on/off
        when(nodes.isLocalNodeElectedMaster()).thenReturn(true);
    }

    public void testEmptyHostListDefault() {
        runTestEmptyHostList(true);
    }

    public void testEmptyHostListExplicit() {
        runTestEmptyHostList(false);
    }

    private void runTestEmptyHostList(final boolean useDefault) {
        final String prefix = "xpack.monitoring.exporters.example";
        final Settings.Builder builder = Settings.builder().put(prefix + ".type", "http");
        List<String> expectedWarnings = new ArrayList<>();
        if (useDefault == false) {
            builder.putList(prefix + ".host", Collections.emptyList());
            expectedWarnings.add(
                "[xpack.monitoring.exporters.example.host] setting was deprecated in Elasticsearch and will be removed "
                    + "in a future release."
            );
        }
        final Settings settings = builder.build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HttpExporter.HOST_SETTING.getConcreteSetting(prefix + ".host").get(settings)
        );
        assertThat(e, hasToString(containsString("Failed to parse value [[]] for setting [" + prefix + ".host]")));
        assertThat(e.getCause(), instanceOf(SettingsException.class));
        assertThat(e.getCause(), hasToString(containsString("host list for [" + prefix + ".host] is empty")));
        expectedWarnings.add(
            "[xpack.monitoring.exporters.example.type] setting was deprecated in Elasticsearch and will be removed "
                + "in a future release."
        );
        assertWarnings(expectedWarnings.toArray(new String[0]));
    }

    public void testEmptyHostListOkayIfTypeNotSetDefault() {
        runTestEmptyHostListOkayIfTypeNotSet(true);
    }

    public void testEmptyHostListOkayIfTypeNotSetExplicit() {
        runTestEmptyHostListOkayIfTypeNotSet(true);
    }

    private void runTestEmptyHostListOkayIfTypeNotSet(final boolean useDefault) {
        final String prefix = "xpack.monitoring.exporters.example";
        final Settings.Builder builder = Settings.builder();
        if (useDefault == false) {
            builder.put(prefix + ".type", Exporter.TYPE_SETTING.getConcreteSettingForNamespace("example").get(Settings.EMPTY));
        }
        builder.putList(prefix + ".host", Collections.emptyList());
        final Settings settings = builder.build();
        HttpExporter.HOST_SETTING.getConcreteSetting(prefix + ".host").get(settings);
        assertWarnings(
            "[xpack.monitoring.exporters.example.host] setting was deprecated in Elasticsearch and will be removed in a "
                + "future release."
        );
    }

    public void testHostListIsRejectedIfTypeIsNotHttp() {
        final String prefix = "xpack.monitoring.exporters.example";
        final Settings.Builder builder = Settings.builder().put(prefix + ".type", "local");
        builder.putList(prefix + ".host", List.of("https://example.com:443"));
        final Settings settings = builder.build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, Set.of(HttpExporter.HOST_SETTING, Exporter.TYPE_SETTING));
        final SettingsException e = expectThrows(SettingsException.class, () -> clusterSettings.validate(settings, true));
        assertThat(e, hasToString(containsString("[" + prefix + ".host] is set but type is [local]")));
        assertWarnings(
            "[xpack.monitoring.exporters.example.type] setting was deprecated in Elasticsearch and will be removed in a future release.",
            "[xpack.monitoring.exporters.example.host] setting was deprecated in Elasticsearch and will be removed in a future release."
        );
    }

    public void testSecurePasswordIsRejectedIfTypeIsNotHttp() {
        final String prefix = "xpack.monitoring.exporters.example";
        final Settings.Builder builder = Settings.builder().put(prefix + ".type", "local");

        final String settingName = ".auth.secure_password";
        final String settingValue = "securePassword";
        MockSecureSettings mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString(prefix + settingName, settingValue);

        builder.setSecureSettings(mockSecureSettings);

        final Settings settings = builder.build();
        final ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Set.of(HttpExporter.AUTH_SECURE_PASSWORD_SETTING, Exporter.TYPE_SETTING)
        );
        final SettingsException e = expectThrows(SettingsException.class, () -> clusterSettings.validate(settings, true));
        assertThat(e, hasToString(containsString("[" + prefix + settingName + "] is set but type is [local]")));
        assertWarnings(
            "[xpack.monitoring.exporters.example.type] setting was deprecated in Elasticsearch and will be removed in a future release.",
            "[xpack.monitoring.exporters.example.auth.secure_password] setting was deprecated in Elasticsearch and will be removed in a "
                + "future release."
        );
    }

    public void testInvalidHost() {
        final String prefix = "xpack.monitoring.exporters.example";
        final String host = "https://example.com:443/";
        final Settings settings = Settings.builder().put(prefix + ".type", "http").put(prefix + ".host", host).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HttpExporter.HOST_SETTING.getConcreteSetting(prefix + ".host").get(settings)
        );
        assertThat(e, hasToString(containsString("Failed to parse value [[\"" + host + "\"]] for setting [" + prefix + ".host]")));
        assertThat(e.getCause(), instanceOf(SettingsException.class));
        assertThat(e.getCause(), hasToString(containsString("[" + prefix + ".host] invalid host: [" + host + "]")));
        assertThat(e.getCause().getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(e.getCause().getCause(), hasToString(containsString("HttpHosts do not use paths [/].")));
        assertWarnings(
            "[xpack.monitoring.exporters.example.host] setting was deprecated in Elasticsearch and will be removed in a future release.",
            "[xpack.monitoring.exporters.example.type] setting was deprecated in Elasticsearch and will be removed in a future release."
        );
    }

    public void testMixedSchemes() {
        final String prefix = "xpack.monitoring.exporters.example";
        final String httpHost = "http://example.com:443";
        final String httpsHost = "https://example.com:443";
        final Settings settings = Settings.builder()
            .put(prefix + ".type", "http")
            .putList(prefix + ".host", List.of(httpHost, httpsHost))
            .build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HttpExporter.HOST_SETTING.getConcreteSetting(prefix + ".host").get(settings)
        );
        assertThat(
            e,
            hasToString(
                containsString("Failed to parse value [[\"" + httpHost + "\",\"" + httpsHost + "\"]] for setting [" + prefix + ".host]")
            )
        );
        assertThat(e.getCause(), instanceOf(SettingsException.class));
        assertThat(e.getCause(), hasToString(containsString("[" + prefix + ".host] must use a consistent scheme: http or https")));
        assertWarnings(
            "[xpack.monitoring.exporters.example.host] setting was deprecated in Elasticsearch and will be removed in a future release.",
            "[xpack.monitoring.exporters.example.type] setting was deprecated in Elasticsearch and will be removed in a future release."
        );
    }

    public void testExporterWithBlacklistedHeaders() {
        final String blacklistedHeader = randomFrom(HttpExporter.BLACKLISTED_HEADERS);
        final String expected = "header cannot be overwritten via [xpack.monitoring.exporters._http.headers." + blacklistedHeader + "]";
        final Settings.Builder builder = Settings.builder()
            .put("xpack.monitoring.exporters._http.type", HttpExporter.TYPE)
            .put("xpack.monitoring.exporters._http.host", "http://localhost:9200")
            .put("xpack.monitoring.exporters._http.headers.abc", "xyz")
            .put("xpack.monitoring.exporters._http.headers." + blacklistedHeader, "value should not matter");

        if (randomBoolean()) {
            builder.put("xpack.monitoring.exporters._http.headers.xyz", "abc");
        }

        final Config config = createConfig(builder.build());
        final MonitoringMigrationCoordinator coordinator = new MonitoringMigrationCoordinator();

        final SettingsException exception = expectThrows(
            SettingsException.class,
            () -> new HttpExporter(config, sslService, threadContext, coordinator)
        );

        assertThat(exception.getMessage(), equalTo(expected));

        assertWarnings(
            "[xpack.monitoring.exporters._http.host] setting was deprecated in Elasticsearch and will be removed in a future release.",
            "[xpack.monitoring.exporters._http.type] setting was deprecated in Elasticsearch and will be removed in a future release."
        );
    }

    public void testExporterWithEmptyHeaders() {
        final String name = randomFrom("abc", "ABC", "X-Flag");
        final String expected = "headers must have values, missing for setting [xpack.monitoring.exporters._http.headers." + name + "]";
        final Settings.Builder builder = Settings.builder()
            .put("xpack.monitoring.exporters._http.type", HttpExporter.TYPE)
            .put("xpack.monitoring.exporters._http.host", "localhost:9200")
            .put("xpack.monitoring.exporters._http.headers." + name, "");

        if (randomBoolean()) {
            builder.put("xpack.monitoring.exporters._http.headers.xyz", "abc");
        }

        final Config config = createConfig(builder.build());
        final MonitoringMigrationCoordinator coordinator = new MonitoringMigrationCoordinator();

        final SettingsException exception = expectThrows(
            SettingsException.class,
            () -> new HttpExporter(config, sslService, threadContext, coordinator)
        );

        assertThat(exception.getMessage(), equalTo(expected));
        assertWarnings(
            "[xpack.monitoring.exporters._http.host] setting was deprecated in Elasticsearch and will be removed in a future release.",
            "[xpack.monitoring.exporters._http.type] setting was deprecated in Elasticsearch and will be removed in a future release."
        );
    }

    public void testExporterWithUnknownBlacklistedClusterAlerts() {
        final SSLIOSessionStrategy sslStrategy = mock(SSLIOSessionStrategy.class);
        when(sslService.sslIOSessionStrategy(any(Settings.class))).thenReturn(sslStrategy);

        final List<String> blacklist = new ArrayList<>();
        blacklist.add("does_not_exist");

        if (randomBoolean()) {
            // a valid ID
            blacklist.add(randomFrom(ClusterAlertsUtil.WATCH_IDS));
            Collections.shuffle(blacklist, random());
        }

        final Settings.Builder builder = Settings.builder()
            .put("xpack.monitoring.exporters._http.type", HttpExporter.TYPE)
            .put("xpack.monitoring.exporters._http.host", "http://localhost:9200")
            .put("xpack.monitoring.exporters._http.cluster_alerts.management.enabled", true)
            .putList("xpack.monitoring.exporters._http.cluster_alerts.management.blacklist", blacklist);

        final Config config = createConfig(builder.build());
        final MonitoringMigrationCoordinator coordinator = new MonitoringMigrationCoordinator();

        final SettingsException exception = expectThrows(
            SettingsException.class,
            () -> new HttpExporter(config, sslService, threadContext, coordinator)
        );

        assertThat(
            exception.getMessage(),
            equalTo(
                "[xpack.monitoring.exporters._http.cluster_alerts.management.blacklist] contains unrecognized Cluster "
                    + "Alert IDs [does_not_exist]"
            )
        );
        assertWarnings(
            "[xpack.monitoring.exporters._http.cluster_alerts.management.blacklist] setting was deprecated in Elasticsearch"
                + " and will be removed in a future release.",
            "[xpack.monitoring.exporters._http.cluster_alerts.management.enabled] setting was deprecated in Elasticsearch"
                + " and will be removed in a future release."
        );
    }

    public void testExporterWithHostOnly() throws Exception {
        final SSLIOSessionStrategy sslStrategy = mock(SSLIOSessionStrategy.class);
        when(sslService.sslIOSessionStrategy(any(Settings.class))).thenReturn(sslStrategy);

        final Settings.Builder builder = Settings.builder()
            .put("xpack.monitoring.exporters._http.type", "http")
            .put("xpack.monitoring.exporters._http.host", "http://localhost:9200");

        final Config config = createConfig(builder.build());
        final MonitoringMigrationCoordinator coordinator = new MonitoringMigrationCoordinator();

        new HttpExporter(config, sslService, threadContext, coordinator).close();
        assertWarnings(
            "[xpack.monitoring.exporters._http.host] setting was deprecated in Elasticsearch and will be removed in a future release.",
            "[xpack.monitoring.exporters._http.type] setting was deprecated in Elasticsearch and will be removed in a future release."
        );
    }

    public void testExporterWithInvalidProxyBasePath() throws Exception {
        final String prefix = "xpack.monitoring.exporters._http";
        final String settingName = ".proxy.base_path";
        final String settingValue = "z//";
        final String expected = "[" + prefix + settingName + "] is malformed [" + settingValue + "]";
        final Settings settings = Settings.builder()
            .put(prefix + ".type", HttpExporter.TYPE)
            .put(prefix + ".host", "localhost:9200")
            .put(prefix + settingName, settingValue)
            .build();

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HttpExporter.PROXY_BASE_PATH_SETTING.getConcreteSetting(prefix + settingName).get(settings)
        );
        assertThat(
            e,
            hasToString(containsString("Failed to parse value [" + settingValue + "] for setting [" + prefix + settingName + "]"))
        );

        assertThat(e.getCause(), instanceOf(SettingsException.class));
        assertThat(e.getCause(), hasToString(containsString(expected)));
        assertWarnings(
            "[xpack.monitoring.exporters._http.proxy.base_path] setting was deprecated in Elasticsearch and will be removed "
                + "in a future release."
        );
    }

    public void testCreateRestClient() throws IOException {
        final SSLIOSessionStrategy sslStrategy = mock(SSLIOSessionStrategy.class);

        when(sslService.sslIOSessionStrategy(any(Settings.class))).thenReturn(sslStrategy);
        List<String> expectedWarnings = new ArrayList<>();

        final Settings.Builder builder = Settings.builder()
            .put("xpack.monitoring.exporters._http.type", "http")
            .put("xpack.monitoring.exporters._http.host", "http://localhost:9200");
        expectedWarnings.add(
            "[xpack.monitoring.exporters._http.host] setting was deprecated in Elasticsearch and will be removed in a future release."
        );
        expectedWarnings.add(
            "[xpack.monitoring.exporters._http.type] setting was deprecated in Elasticsearch and will be removed in a future release."
        );

        // use basic auth
        final boolean useBasicAuth = randomBoolean();
        if (useBasicAuth) {
            builder.put("xpack.monitoring.exporters._http.auth.username", "_user");
            MockSecureSettings mockSecureSettings = new MockSecureSettings();
            mockSecureSettings.setString("xpack.monitoring.exporters._http.auth.secure_password", "securePassword");
            builder.setSecureSettings(mockSecureSettings);
            expectedWarnings.add(
                "[xpack.monitoring.exporters._http.auth.username] setting was deprecated in Elasticsearch and will be "
                    + "removed in a future release."
            );
        }

        // use headers
        if (randomBoolean()) {
            builder.put("xpack.monitoring.exporters._http.headers.abc", "xyz");
        }

        final Config config = createConfig(builder.build());
        final NodeFailureListener listener = mock(NodeFailureListener.class);

        // doesn't explode
        HttpExporter.createRestClient(config, sslService, listener).close();

        if (expectedWarnings.size() > 0) {
            assertWarnings(expectedWarnings.toArray(new String[0]));
        }
    }

    public void testCreateCredentialsProviderWithoutSecurity() {
        final Settings.Builder builder = Settings.builder()
            .put("xpack.monitoring.exporters._http.type", "http")
            .put("xpack.monitoring.exporters._http.host", "http://localhost:9200");

        final Config config = createConfig(builder.build());
        CredentialsProvider provider = HttpExporter.createCredentialsProvider(config);

        assertNull(provider);

        assertWarnings(
            "[xpack.monitoring.exporters._http.type] setting was deprecated in Elasticsearch and will be removed in a future release."
        );
    }

    public void testCreateSnifferDisabledByDefault() {
        final Config config = createConfig(Settings.EMPTY);
        final RestClient client = mock(RestClient.class);
        final NodeFailureListener listener = mock(NodeFailureListener.class);

        assertThat(HttpExporter.createSniffer(config, client, listener), nullValue());

        verifyNoMoreInteractions(client, listener);
    }

    public void testCreateSniffer() throws IOException {
        final Settings.Builder builder = Settings.builder()
            .put("xpack.monitoring.exporters._http.type", "http")
            // it's a simple check: does it start with "https"?
            .put("xpack.monitoring.exporters._http.host", randomFrom("neither", "http", "https"))
            .put("xpack.monitoring.exporters._http.sniff.enabled", true);

        final Config config = createConfig(builder.build());
        final RestClient client = mock(RestClient.class);
        final NodeFailureListener listener = mock(NodeFailureListener.class);
        final Response response = mock(Response.class);
        final StringEntity entity = new StringEntity("{}", ContentType.APPLICATION_JSON);

        when(response.getEntity()).thenReturn(entity);
        when(client.performRequest(any(Request.class))).thenReturn(response);

        try (Sniffer sniffer = HttpExporter.createSniffer(config, client, listener)) {
            assertThat(sniffer, not(nullValue()));

            verify(listener).setSniffer(sniffer);
        }

        // it's a race whether it triggers this at all
        verify(client, atMost(1)).performRequest(any(Request.class));

        verifyNoMoreInteractions(client, listener);

        assertWarnings(
            "[xpack.monitoring.exporters._http.sniff.enabled] setting was deprecated in Elasticsearch and will be removed "
                + "in a future release.",
            "[xpack.monitoring.exporters._http.host] setting was deprecated in Elasticsearch and will be removed in a future release.",
            "[xpack.monitoring.exporters._http.type] setting was deprecated in Elasticsearch and will be removed in a future release."
        );
    }

    public void testCreateResources() {
        final boolean clusterAlertManagement = randomBoolean();
        final TimeValue templateTimeout = randomFrom(TimeValue.timeValueSeconds(30), null);

        final Settings.Builder builder = Settings.builder().put("xpack.monitoring.exporters._http.type", "http");
        List<String> warningsExpected = new ArrayList<>();

        if (clusterAlertManagement) {
            builder.put("xpack.monitoring.exporters._http.cluster_alerts.management.enabled", true);
            warningsExpected.add(
                "[xpack.monitoring.exporters._http.cluster_alerts.management.enabled] setting was deprecated in "
                    + "Elasticsearch and will be removed in a future release."
            );
        }

        if (templateTimeout != null) {
            builder.put("xpack.monitoring.exporters._http.index.template.master_timeout", templateTimeout.getStringRep());
            warningsExpected.add(
                "[xpack.monitoring.exporters._http.index.template.master_timeout] setting was deprecated in "
                    + "Elasticsearch and will be removed in a future release."
            );
        }

        final Config config = createConfig(builder.build());

        final MultiHttpResource multiResource = HttpExporter.createResources(config).allResources;

        final List<HttpResource> resources = multiResource.getResources();
        final int version = (int) resources.stream().filter((resource) -> resource instanceof VersionHttpResource).count();
        final List<TemplateHttpResource> templates = resources.stream()
            .filter((resource) -> resource instanceof TemplateHttpResource)
            .map(TemplateHttpResource.class::cast)
            .collect(Collectors.toList());
        final List<WatcherExistsHttpResource> watcherCheck = resources.stream()
            .filter((resource) -> resource instanceof WatcherExistsHttpResource)
            .map(WatcherExistsHttpResource.class::cast)
            .collect(Collectors.toList());
        final List<ClusterAlertHttpResource> watches;
        if (watcherCheck.isEmpty()) {
            watches = Collections.emptyList();
        } else {
            watches = watcherCheck.get(0)
                .getWatches()
                .getResources()
                .stream()
                .filter((resource) -> resource instanceof ClusterAlertHttpResource)
                .map(ClusterAlertHttpResource.class::cast)
                .collect(Collectors.toList());
        }

        // expected number of resources
        assertThat(multiResource.getResources().size(), equalTo(version + templates.size() + watcherCheck.size()));
        assertThat(version, equalTo(1));
        assertThat(templates, hasSize(MonitoringTemplateRegistry.TEMPLATE_NAMES.length));
        assertThat(watcherCheck, hasSize(clusterAlertManagement ? 1 : 0));
        assertThat(watches, hasSize(clusterAlertManagement ? ClusterAlertsUtil.WATCH_IDS.length : 0));

        // timeouts
        assertMasterTimeoutSet(templates, templateTimeout);

        // logging owner names
        final List<String> uniqueOwners = resources.stream()
            .map(HttpResource::getResourceOwnerName)
            .distinct()
            .collect(Collectors.toList());

        assertThat(uniqueOwners, hasSize(1));
        assertThat(uniqueOwners.get(0), equalTo("xpack.monitoring.exporters._http"));
        if (warningsExpected.size() > 0) {
            assertWarnings(warningsExpected.toArray(new String[0]));
        }
    }

    public void testCreateDefaultParams() {
        final TimeValue bulkTimeout = randomFrom(TimeValue.timeValueSeconds(30), null);

        final Settings.Builder builder = Settings.builder().put("xpack.monitoring.exporters._http.type", "http");

        if (bulkTimeout != null) {
            builder.put("xpack.monitoring.exporters._http.bulk.timeout", bulkTimeout.toString());
        }

        final Config config = createConfig(builder.build());

        final Map<String, String> parameters = new HashMap<>(HttpExporter.createDefaultParams(config));

        assertThat(parameters.remove("filter_path"), equalTo("errors,items.*.error"));

        if (bulkTimeout != null) {
            assertThat(parameters.remove("timeout"), equalTo(bulkTimeout.toString()));
            assertWarnings(
                "[xpack.monitoring.exporters._http.bulk.timeout] setting was deprecated in Elasticsearch and will be removed "
                    + "in a future release."
            );
        } else {
            assertNull(parameters.remove("timeout"));
        }

        // should have removed everything
        assertThat(parameters.size(), equalTo(0));
    }

    public void testHttpExporterMigrationInProgressBlock() throws Exception {
        final Config config = createConfig(Settings.EMPTY);
        final RestClient client = mock(RestClient.class);
        final Sniffer sniffer = randomFrom(mock(Sniffer.class), null);
        final NodeFailureListener listener = mock(NodeFailureListener.class);
        // this is configured to throw an error when the resource is checked
        final HttpResource resource = new MockHttpResource(exporterName(), true, null, false);
        final HttpResource alertsResource = new MockHttpResource(exporterName(), false, null, false);
        final MonitoringMigrationCoordinator migrationCoordinator = new MonitoringMigrationCoordinator();
        assertTrue(migrationCoordinator.tryBlockInstallationTasks());

        try (
            HttpExporter exporter = new HttpExporter(
                config,
                client,
                sniffer,
                threadContext,
                migrationCoordinator,
                listener,
                resource,
                alertsResource
            )
        ) {
            verify(listener).setResource(resource);

            final CountDownLatch awaitResponseAndClose = new CountDownLatch(1);
            final ActionListener<ExportBulk> bulkListener = ActionTestUtils.assertNoFailureListener(bulk -> {
                assertNull("should have been invoked with null value to denote migration in progress", bulk);
                awaitResponseAndClose.countDown();
            });

            exporter.openBulk(bulkListener);

            // wait for it to actually respond
            assertTrue(awaitResponseAndClose.await(15, TimeUnit.SECONDS));
        }
    }

    public void testHttpExporterDirtyResourcesBlock() throws Exception {
        final Config config = createConfig(Settings.EMPTY);
        final RestClient client = mock(RestClient.class);
        final Sniffer sniffer = randomFrom(mock(Sniffer.class), null);
        final NodeFailureListener listener = mock(NodeFailureListener.class);
        // this is configured to throw an error when the resource is checked
        final HttpResource resource = new MockHttpResource(exporterName(), true, null, false);
        final HttpResource alertsResource = new MockHttpResource(exporterName(), false, null, false);
        final MonitoringMigrationCoordinator migrationCoordinator = new MonitoringMigrationCoordinator();

        try (
            HttpExporter exporter = new HttpExporter(
                config,
                client,
                sniffer,
                threadContext,
                migrationCoordinator,
                listener,
                resource,
                alertsResource
            )
        ) {
            verify(listener).setResource(resource);

            final CountDownLatch awaitResponseAndClose = new CountDownLatch(1);
            final ActionListener<ExportBulk> bulkListener = ActionListener.wrap(
                bulk -> fail("[onFailure] should have been invoked by failed resource check"),
                e -> awaitResponseAndClose.countDown()
            );

            exporter.openBulk(bulkListener);

            // wait for it to actually respond
            assertTrue(awaitResponseAndClose.await(15, TimeUnit.SECONDS));
        }
    }

    public void testHttpExporterReturnsNullForOpenBulkIfNotReady() throws Exception {
        final Config config = createConfig(Settings.EMPTY);
        final RestClient client = mock(RestClient.class);
        final Sniffer sniffer = randomFrom(mock(Sniffer.class), null);
        final NodeFailureListener listener = mock(NodeFailureListener.class);
        // always has to check, and never succeeds checks but it does not throw an exception (e.g., version check fails)
        final HttpResource resource = new MockHttpResource(exporterName(), true, false, false);
        final HttpResource alertsResource = new MockHttpResource(exporterName(), false, null, false);
        final MonitoringMigrationCoordinator migrationCoordinator = new MonitoringMigrationCoordinator();

        try (
            HttpExporter exporter = new HttpExporter(
                config,
                client,
                sniffer,
                threadContext,
                migrationCoordinator,
                listener,
                resource,
                alertsResource
            )
        ) {
            verify(listener).setResource(resource);

            final CountDownLatch awaitResponseAndClose = new CountDownLatch(1);
            final ActionListener<ExportBulk> bulkListener = ActionTestUtils.assertNoFailureListener(bulk -> {
                assertThat(bulk, nullValue());
                awaitResponseAndClose.countDown();
            });

            exporter.openBulk(bulkListener);

            // wait for it to actually respond
            assertTrue(awaitResponseAndClose.await(15, TimeUnit.SECONDS));
        }
    }

    public void testHttpExporter() throws Exception {
        final Config config = createConfig(Settings.EMPTY);
        final RestClient client = mock(RestClient.class);
        final Sniffer sniffer = randomFrom(mock(Sniffer.class), null);
        final NodeFailureListener listener = mock(NodeFailureListener.class);
        // sometimes dirty to start with and sometimes not; but always succeeds on checkAndPublish
        final HttpResource resource = new MockHttpResource(exporterName(), randomBoolean());
        final HttpResource alertsResource = new MockHttpResource(exporterName(), false, null, false);
        final MonitoringMigrationCoordinator migrationCoordinator = new MonitoringMigrationCoordinator();

        try (
            HttpExporter exporter = new HttpExporter(
                config,
                client,
                sniffer,
                threadContext,
                migrationCoordinator,
                listener,
                resource,
                alertsResource
            )
        ) {
            verify(listener).setResource(resource);

            final CountDownLatch awaitResponseAndClose = new CountDownLatch(1);
            final ActionListener<ExportBulk> bulkListener = ActionTestUtils.assertNoFailureListener(bulk -> {
                assertThat(bulk.getName(), equalTo(exporterName()));
                awaitResponseAndClose.countDown();
            });

            exporter.openBulk(bulkListener);

            // wait for it to actually respond
            assertTrue(awaitResponseAndClose.await(15, TimeUnit.SECONDS));
        }
    }

    public void testHttpExporterShutdown() throws Exception {
        final Config config = createConfig(Settings.EMPTY);
        final RestClient client = mock(RestClient.class);
        final Sniffer sniffer = randomFrom(mock(Sniffer.class), null);
        final NodeFailureListener listener = mock(NodeFailureListener.class);
        final MultiHttpResource resource = mock(MultiHttpResource.class);
        final HttpResource alertsResource = mock(MultiHttpResource.class);
        final MonitoringMigrationCoordinator migrationCoordinator = new MonitoringMigrationCoordinator();

        if (sniffer != null && rarely()) {
            doThrow(new RuntimeException("expected")).when(sniffer).close();
        }

        if (rarely()) {
            doThrow(randomFrom(new IOException("expected"), new RuntimeException("expected"))).when(client).close();
        }

        new HttpExporter(config, client, sniffer, threadContext, migrationCoordinator, listener, resource, alertsResource).close();

        // order matters; sniffer must close first
        if (sniffer != null) {
            final InOrder inOrder = inOrder(sniffer, client);

            inOrder.verify(sniffer).close();
            inOrder.verify(client).close();
        } else {
            verify(client).close();
        }
    }

    private void assertMasterTimeoutSet(final List<? extends HttpResource> resources, final TimeValue timeout) {
        if (timeout != null) {
            for (final HttpResource resource : resources) {
                if (resource instanceof PublishableHttpResource) {
                    assertEquals(timeout.getStringRep(), ((PublishableHttpResource) resource).getDefaultParameters().get("master_timeout"));
                }
            }
        }
    }

    /**
     * Create the {@link Config} named "_http" and select those settings from {@code settings}.
     *
     * @param settings The settings to select the exporter's settings from
     * @return Never {@code null}.
     */
    private Config createConfig(final Settings settings) {
        return new Config("_http", HttpExporter.TYPE, settings, clusterService, licenseState);
    }

    private static String exporterName() {
        return "xpack.monitoring.exporters._http";
    }

}
