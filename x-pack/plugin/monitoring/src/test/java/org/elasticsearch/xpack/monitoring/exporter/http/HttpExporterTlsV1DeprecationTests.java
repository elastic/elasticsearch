/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.TLSv1DeprecationHandler;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HttpExporterTlsV1DeprecationTests extends ESTestCase {

    private final ClusterService clusterService = mock(ClusterService.class);
    private final XPackLicenseState licenseState = mock(XPackLicenseState.class);
    private final SSLService sslService = mock(SSLService.class);
    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

    @Before
    public void setupMocks() {
        final ClusterState clusterState = mock(ClusterState.class);
        final DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        final MetaData metaData = mock(MetaData.class);

        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metaData()).thenReturn(metaData);
        when(clusterState.nodes()).thenReturn(nodes);
        // always let the watcher resources run for these tests; HttpExporterResourceTests tests it flipping on/off
        when(nodes.isLocalNodeElectedMaster()).thenReturn(true);

        DeprecationLogger.setThreadContext(threadContext);
    }

    @After
    public void clearThreadContext() {
        DeprecationLogger.removeThreadContext(threadContext);
    }

    public void testDeprecationWarningEnabledWhenUsingDefaultProtocols() throws IOException {
        final Settings.Builder builder = Settings.builder()
            .put("xpack.monitoring.exporters._http.type", "http")
            .put("xpack.monitoring.exporters._http.host", "https://localhost:12345")
            .put("xpack.monitoring.exporters._http.ssl.enabled", true);

        final boolean useSecureSettings = randomBoolean();
        if (useSecureSettings) {
            // The HTTP Exporter behaves slightly differently if secure settings are in use (because they cannot be dynamically reloaded)
            final MockSecureSettings secureSettings = new MockSecureSettings();
            secureSettings.setString("xpack.monitoring.exporters._http.ssl.truststore.secure_password", "random");
            builder.setSecureSettings(secureSettings);
        }

        final TLSv1DeprecationHandler deprecationHandler = buildExporterAndGetDeprecationHandler(builder, useSecureSettings);
        assertThat(deprecationHandler.shouldLogWarnings(), is(true));

        final SSLSession sslSession = mock(SSLSession.class);
        when(sslSession.getProtocol()).thenReturn("TLSv1");
        deprecationHandler.checkAndLog(sslSession, () -> "http exporter test");
        assertThat(threadContext.getResponseHeaders(), hasKey("Warning"));
        threadContext.stashContext();

        when(sslSession.getProtocol()).thenReturn(randomFrom("TLSv1.1", "TLSv1.2"));
        deprecationHandler.checkAndLog(sslSession, () -> "http exporter test");
        assertThat(threadContext.getResponseHeaders(), not(hasKey("Warning")));

        assertWarnings("a TLS v1.0 session was used for [http exporter test]," +
            " this protocol will be disabled by default in a future version." +
            " The [xpack.monitoring.exporters._http.ssl.supported_protocols] setting can be used to control this.");
    }

    public void testNoDeprecationWarningWhenUsingExplicitProtocols() throws IOException {
        final Settings.Builder builder = Settings.builder()
            .put("xpack.monitoring.exporters._http.type", "http")
            .put("xpack.monitoring.exporters._http.host", "https://localhost:12345")
            .put("xpack.monitoring.exporters._http.ssl.enabled", true)
            .putList("xpack.monitoring.exporters._http.ssl.supported_protocols", "TLSv1.2", "TLSv1.1", "TLSv1");

        final boolean useSecureSettings = randomBoolean();
        if (useSecureSettings) {
            // The HTTP Exporter behaves slightly differently if secure settings are in use (because they cannot be dynamically reloaded)
            final MockSecureSettings secureSettings = new MockSecureSettings();
            secureSettings.setString("xpack.monitoring.exporters._http.ssl.truststore.secure_password", "not user");
            builder.setSecureSettings(secureSettings);
        }

        final TLSv1DeprecationHandler deprecationHandler = buildExporterAndGetDeprecationHandler(builder, useSecureSettings);
        assertThat(deprecationHandler.shouldLogWarnings(), is(false));

        final SSLSession sslSession = mock(SSLSession.class);
        when(sslSession.getProtocol()).thenReturn("TLSv1");
        deprecationHandler.checkAndLog(sslSession, () -> "http exporter test");
        assertThat(threadContext.getResponseHeaders(), not(hasKey("Warning")));
    }

    private TLSv1DeprecationHandler buildExporterAndGetDeprecationHandler(Settings.Builder builder, boolean withSecureSettings)
        throws IOException {

        final AtomicReference<TLSv1DeprecationHandler> deprecationHandlerRef = new AtomicReference<>();
        final Answer<Object> answer = inv -> {
            assert inv.getArguments().length == 2;
            final TLSv1DeprecationHandler handler = (TLSv1DeprecationHandler) inv.getArguments()[1];
            deprecationHandlerRef.set(handler);
            return SSLIOSessionStrategy.getDefaultStrategy();
        };
        if (withSecureSettings) {
            Mockito.when(sslService.sslIOSessionStrategy(any(SSLConfiguration.class), any(TLSv1DeprecationHandler.class))).then(answer);
        } else {
            Mockito.when(sslService.sslIOSessionStrategy(any(Settings.class), any(TLSv1DeprecationHandler.class))).then(answer);
        }

        final Exporter.Config config = createConfig(builder.build());
        final NodeFailureListener listener = mock(NodeFailureListener.class);
        try (RestClient client = HttpExporter.createRestClient(config, sslService, listener)) {
            assertThat(client, notNullValue());
        }
        final TLSv1DeprecationHandler deprecationHandler = deprecationHandlerRef.get();
        assertThat(deprecationHandler, notNullValue());
        return deprecationHandler;
    }

    private Exporter.Config createConfig(final Settings settings) {
        return new Exporter.Config("_http", HttpExporter.TYPE, settings, clusterService, licenseState);
    }

}
