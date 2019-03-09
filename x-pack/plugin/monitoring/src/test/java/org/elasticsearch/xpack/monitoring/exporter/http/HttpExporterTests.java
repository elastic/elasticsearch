/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.sniff.Sniffer;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.monitoring.exporter.ClusterAlertsUtil;
import org.elasticsearch.xpack.monitoring.exporter.ExportBulk;
import org.elasticsearch.xpack.monitoring.exporter.Exporter.Config;
import org.junit.Before;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.OLD_TEMPLATE_IDS;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.PIPELINE_IDS;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.TEMPLATE_IDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests {@link HttpExporter}.
 */
public class HttpExporterTests extends ESTestCase {

    private final ClusterService clusterService = mock(ClusterService.class);
    private final XPackLicenseState licenseState = mock(XPackLicenseState.class);
    private final MetaData metaData = mock(MetaData.class);

    private final SSLService sslService = mock(SSLService.class);
    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

    @Before
    public void setupClusterState() {
        final ClusterState clusterState = mock(ClusterState.class);
        final DiscoveryNodes nodes = mock(DiscoveryNodes.class);

        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metaData()).thenReturn(metaData);
        when(clusterState.nodes()).thenReturn(nodes);
        // always let the watcher resources run for these tests; HttpExporterResourceTests tests it flipping on/off
        when(nodes.isLocalNodeElectedMaster()).thenReturn(true);
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

        final SettingsException exception =
                expectThrows(SettingsException.class, () -> new HttpExporter(config, sslService, threadContext));

        assertThat(exception.getMessage(), equalTo(expected));
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

        final SettingsException exception =
                expectThrows(SettingsException.class, () -> new HttpExporter(config, sslService, threadContext));

        assertThat(exception.getMessage(), equalTo(expected));
    }

    public void testExporterWithPasswordButNoUsername() {
        final String expected =
                "[xpack.monitoring.exporters._http.auth.password] without [xpack.monitoring.exporters._http.auth.username]";
        final Settings.Builder builder = Settings.builder()
                .put("xpack.monitoring.exporters._http.type", HttpExporter.TYPE)
                .put("xpack.monitoring.exporters._http.host", "localhost:9200")
                .put("xpack.monitoring.exporters._http.auth.password", "_pass");

        final Config config = createConfig(builder.build());

        final SettingsException exception = expectThrows(SettingsException.class,
                () -> new HttpExporter(config, sslService, threadContext));

        assertThat(exception.getMessage(), equalTo(expected));
    }

    public void testExporterWithMissingHost() {
        // forgot host!
        final Settings.Builder builder = Settings.builder()
                .put("xpack.monitoring.exporters._http.type", HttpExporter.TYPE);

        if (randomBoolean()) {
            builder.put("xpack.monitoring.exporters._http.host", "");
        } else if (randomBoolean()) {
            builder.putList("xpack.monitoring.exporters._http.host");
        } else if (randomBoolean()) {
            builder.putNull("xpack.monitoring.exporters._http.host");
        }

        final Config config = createConfig(builder.build());

        final SettingsException exception =
                expectThrows(SettingsException.class, () -> new HttpExporter(config, sslService, threadContext));

        assertThat(exception.getMessage(), equalTo("missing required setting [xpack.monitoring.exporters._http.host]"));
    }

    public void testExporterWithInconsistentSchemes() {
        final Settings.Builder builder = Settings.builder()
                .put("xpack.monitoring.exporters._http.type", HttpExporter.TYPE)
                .putList("xpack.monitoring.exporters._http.host", "http://localhost:9200", "https://localhost:9201");

        final Config config = createConfig(builder.build());

        final SettingsException exception =
                expectThrows(SettingsException.class, () -> new HttpExporter(config, sslService, threadContext));

        assertThat(exception.getMessage(),
                   equalTo("[xpack.monitoring.exporters._http.host] must use a consistent scheme: http or https"));
    }

    public void testExporterWithInvalidHost() {
        final String invalidHost = randomFrom("://localhost:9200", "gopher!://xyz.my.com");

        final Settings.Builder builder = Settings.builder()
                .put("xpack.monitoring.exporters._http.type", HttpExporter.TYPE);

        // sometimes add a valid URL with it
        if (randomBoolean()) {
            if (randomBoolean()) {
                builder.putList("xpack.monitoring.exporters._http.host", "localhost:9200", invalidHost);
            } else {
                builder.putList("xpack.monitoring.exporters._http.host", invalidHost, "localhost:9200");
            }
        } else {
            builder.put("xpack.monitoring.exporters._http.host", invalidHost);
        }

        final Config config = createConfig(builder.build());

        final SettingsException exception =
                expectThrows(SettingsException.class, () -> new HttpExporter(config, sslService, threadContext));

        assertThat(exception.getMessage(), equalTo("[xpack.monitoring.exporters._http.host] invalid host: [" + invalidHost + "]"));
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
                .putList("xpack.monitoring.exporters._http.cluster_alerts.management.blacklist", blacklist);

        final Config config = createConfig(builder.build());

        final SettingsException exception =
                expectThrows(SettingsException.class, () -> new HttpExporter(config, sslService, threadContext));

        assertThat(exception.getMessage(),
                   equalTo("[xpack.monitoring.exporters._http.cluster_alerts.management.blacklist] contains unrecognized Cluster " +
                           "Alert IDs [does_not_exist]"));
    }

    public void testExporterWithHostOnly() throws Exception {
        final SSLIOSessionStrategy sslStrategy = mock(SSLIOSessionStrategy.class);
        when(sslService.sslIOSessionStrategy(any(Settings.class))).thenReturn(sslStrategy);

        final Settings.Builder builder = Settings.builder()
                .put("xpack.monitoring.exporters._http.type", "http")
                .put("xpack.monitoring.exporters._http.host", "http://localhost:9200");

        final Config config = createConfig(builder.build());

        new HttpExporter(config, sslService, threadContext).close();
    }

    public void testCreateRestClient() throws IOException {
        final SSLIOSessionStrategy sslStrategy = mock(SSLIOSessionStrategy.class);

        when(sslService.sslIOSessionStrategy(any(Settings.class))).thenReturn(sslStrategy);

        final Settings.Builder builder = Settings.builder()
                .put("xpack.monitoring.exporters._http.type", "http")
                .put("xpack.monitoring.exporters._http.host", "http://localhost:9200");

        // use basic auth
        if (randomBoolean()) {
            builder.put("xpack.monitoring.exporters._http.auth.username", "_user")
                   .put("xpack.monitoring.exporters._http.auth.password", "_pass");
        }

        // use headers
        if (randomBoolean()) {
            builder.put("xpack.monitoring.exporters._http.headers.abc", "xyz");
        }

        final Config config = createConfig(builder.build());
        final NodeFailureListener listener = mock(NodeFailureListener.class);

        // doesn't explode
        HttpExporter.createRestClient(config, sslService, listener).close();
    }

    public void testCreateSnifferDisabledByDefault() {
        final Config config = createConfig(Settings.EMPTY);
        final RestClient client = mock(RestClient.class);
        final NodeFailureListener listener = mock(NodeFailureListener.class);

        assertThat(HttpExporter.createSniffer(config, client, listener), nullValue());

        verifyZeroInteractions(client, listener);
    }

    public void testCreateSnifferWithoutHosts() {
        final Settings.Builder builder = Settings.builder()
                .put("xpack.monitoring.exporters._http.type", "http")
                .put("xpack.monitoring.exporters._http.sniff.enabled", true);

        final Config config = createConfig(builder.build());
        final RestClient client = mock(RestClient.class);
        final NodeFailureListener listener = mock(NodeFailureListener.class);

        expectThrows(IndexOutOfBoundsException.class, () -> HttpExporter.createSniffer(config, client, listener));
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
    }

    public void testCreateResources() {
        final boolean useIngest = randomBoolean();
        final boolean clusterAlertManagement = randomBoolean();
        final boolean createOldTemplates = randomBoolean();
        final TimeValue templateTimeout = randomFrom(TimeValue.timeValueSeconds(30), null);
        final TimeValue pipelineTimeout = randomFrom(TimeValue.timeValueSeconds(30), null);

        final Settings.Builder builder = Settings.builder()
                .put("xpack.monitoring.exporters._http.type", "http");

        if (useIngest == false) {
            builder.put("xpack.monitoring.exporters._http.use_ingest", false);
        }

        if (clusterAlertManagement == false) {
            builder.put("xpack.monitoring.exporters._http.cluster_alerts.management.enabled", false);
        }

        if (createOldTemplates == false) {
            builder.put("xpack.monitoring.exporters._http.index.template.create_legacy_templates", false);
        }

        if (templateTimeout != null) {
            builder.put("xpack.monitoring.exporters._http.index.template.master_timeout", templateTimeout.getStringRep());
        }

        // note: this shouldn't get used with useIngest == false, but it doesn't hurt to try to cause issues
        if (pipelineTimeout != null) {
            builder.put("xpack.monitoring.exporters._http.index.pipeline.master_timeout", pipelineTimeout.getStringRep());
        }

        final Config config = createConfig(builder.build());

        final MultiHttpResource multiResource = HttpExporter.createResources(config);

        final List<HttpResource> resources = multiResource.getResources();
        final int version = (int)resources.stream().filter((resource) -> resource instanceof VersionHttpResource).count();
        final List<TemplateHttpResource> templates =
                resources.stream().filter((resource) -> resource instanceof TemplateHttpResource)
                                  .map(TemplateHttpResource.class::cast)
                                  .collect(Collectors.toList());
        final List<PipelineHttpResource> pipelines =
                resources.stream().filter((resource) -> resource instanceof PipelineHttpResource)
                                  .map(PipelineHttpResource.class::cast)
                                  .collect(Collectors.toList());
        final List<WatcherExistsHttpResource> watcherCheck =
                resources.stream().filter((resource) -> resource instanceof WatcherExistsHttpResource)
                                  .map(WatcherExistsHttpResource.class::cast)
                                  .collect(Collectors.toList());
        final List<ClusterAlertHttpResource> watches;
        if (watcherCheck.isEmpty()) {
            watches = Collections.emptyList();
        } else {
            watches = watcherCheck.get(0).getWatches().getResources()
                                                      .stream().filter((resource) -> resource instanceof ClusterAlertHttpResource)
                                                      .map(ClusterAlertHttpResource.class::cast)
                                                      .collect(Collectors.toList());
        }

        // expected number of resources
        assertThat(multiResource.getResources().size(),
                   equalTo(version + templates.size() + pipelines.size() + watcherCheck.size()));
        assertThat(version, equalTo(1));
        assertThat(templates, hasSize(createOldTemplates ? TEMPLATE_IDS.length + OLD_TEMPLATE_IDS.length : TEMPLATE_IDS.length));
        assertThat(pipelines, hasSize(useIngest ? PIPELINE_IDS.length : 0));
        assertThat(watcherCheck, hasSize(clusterAlertManagement ? 1 : 0));
        assertThat(watches, hasSize(clusterAlertManagement ? ClusterAlertsUtil.WATCH_IDS.length : 0));

        // timeouts
        assertMasterTimeoutSet(templates, templateTimeout);
        assertMasterTimeoutSet(pipelines, pipelineTimeout);

        // logging owner names
        final List<String> uniqueOwners =
                resources.stream().map(HttpResource::getResourceOwnerName).distinct().collect(Collectors.toList());

        assertThat(uniqueOwners, hasSize(1));
        assertThat(uniqueOwners.get(0), equalTo("xpack.monitoring.exporters._http"));
    }

    public void testCreateDefaultParams() {
        final TimeValue bulkTimeout = randomFrom(TimeValue.timeValueSeconds(30), null);
        final boolean useIngest = randomBoolean();

        final Settings.Builder builder = Settings.builder()
                .put("xpack.monitoring.exporters._http.type", "http");

        if (bulkTimeout != null) {
            builder.put("xpack.monitoring.exporters._http.bulk.timeout", bulkTimeout.toString());
        }

        if (useIngest == false) {
            builder.put("xpack.monitoring.exporters._http.use_ingest", false);
        }

        final Config config = createConfig(builder.build());

        final Map<String, String> parameters = new HashMap<>(HttpExporter.createDefaultParams(config));

        assertThat(parameters.remove("filter_path"), equalTo("errors,items.*.error"));

        if (bulkTimeout != null) {
            assertThat(parameters.remove("timeout"), equalTo(bulkTimeout.toString()));
        } else {
            assertNull(parameters.remove("timeout"));
        }

        if (useIngest) {
            assertThat(parameters.remove("pipeline"),
                       equalTo(MonitoringTemplateUtils.pipelineName(MonitoringTemplateUtils.TEMPLATE_VERSION)));
        }

        // should have removed everything
        assertThat(parameters.size(), equalTo(0));
    }

    public void testHttpExporterDirtyResourcesBlock() throws Exception {
        final Config config = createConfig(Settings.EMPTY);
        final RestClient client = mock(RestClient.class);
        final Sniffer sniffer = randomFrom(mock(Sniffer.class), null);
        final NodeFailureListener listener = mock(NodeFailureListener.class);
        // this is configured to throw an error when the resource is checked
        final HttpResource resource = new MockHttpResource(exporterName(), true, null, false);

        try (HttpExporter exporter = new HttpExporter(config, client, sniffer, threadContext, listener, resource)) {
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

        try (HttpExporter exporter = new HttpExporter(config, client, sniffer, threadContext, listener, resource)) {
            verify(listener).setResource(resource);

            final CountDownLatch awaitResponseAndClose = new CountDownLatch(1);
            final ActionListener<ExportBulk> bulkListener = ActionListener.wrap(
                bulk -> {
                    assertThat(bulk, nullValue());

                    awaitResponseAndClose.countDown();
                },
                e -> fail(e.getMessage())
            );

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

        try (HttpExporter exporter = new HttpExporter(config, client, sniffer, threadContext, listener, resource)) {
            verify(listener).setResource(resource);

            final CountDownLatch awaitResponseAndClose = new CountDownLatch(1);
            final ActionListener<ExportBulk> bulkListener = ActionListener.wrap(
                bulk -> {
                    assertThat(bulk.getName(), equalTo(exporterName()));

                    awaitResponseAndClose.countDown();
                },
                e -> fail(e.getMessage())
            );

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

        if (sniffer != null && rarely()) {
            doThrow(new RuntimeException("expected")).when(sniffer).close();
        }

        if (rarely()) {
            doThrow(randomFrom(new IOException("expected"), new RuntimeException("expected"))).when(client).close();
        }

        new HttpExporter(config, client, sniffer, threadContext, listener, resource).close();

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
