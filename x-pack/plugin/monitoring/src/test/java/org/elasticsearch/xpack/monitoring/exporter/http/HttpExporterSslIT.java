/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import com.sun.net.httpserver.HttpsServer;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.bootstrap.JavaVersion;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ssl.TestsSSLService;
import org.elasticsearch.xpack.core.ssl.VerificationMode;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.exporter.Exporters;
import org.elasticsearch.xpack.monitoring.test.MonitoringIntegTestCase;
import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.Before;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = Scope.SUITE,
    numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class HttpExporterSslIT extends MonitoringIntegTestCase {

    private final Settings globalSettings = Settings.builder().put("path.home", createTempDir()).build();
    private final Environment environment = TestEnvironment.newEnvironment(globalSettings);

    private static MockWebServer webServer;
    private MockSecureSettings secureSettings;


    @AfterClass
    public static void cleanUpStatics() {
        if (webServer != null) {
            webServer.close();
            webServer = null;
        }
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final Path truststore = getDataPath("/org/elasticsearch/xpack/monitoring/exporter/http/testnode.jks");
        assertThat(Files.exists(truststore), CoreMatchers.is(true));

        if (webServer == null) {
            try {
                webServer = buildWebServer();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        final String address = "https://" + webServer.getHostName() + ":" + webServer.getPort();
        final Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("xpack.monitoring.exporters.plaintext.type", "http")
            .put("xpack.monitoring.exporters.plaintext.enabled", true)
            .put("xpack.monitoring.exporters.plaintext.host", address)
            .put("xpack.monitoring.exporters.plaintext.ssl.truststore.path", truststore)
            .put("xpack.monitoring.exporters.plaintext.ssl.truststore.password", "testnode")
            .put("xpack.monitoring.exporters.secure.type", "http")
            .put("xpack.monitoring.exporters.secure.enabled", true)
            .put("xpack.monitoring.exporters.secure.host", address)
            .put("xpack.monitoring.exporters.secure.ssl.truststore.path", truststore);

        secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.monitoring.exporters.secure.ssl.truststore.secure_password", "testnode");
        builder.setSecureSettings(secureSettings);

        return builder.build();
    }

    private MockWebServer buildWebServer() throws IOException {
        final Path cert = getDataPath("/org/elasticsearch/xpack/monitoring/exporter/http/testnode.crt");
        final Path key = getDataPath("/org/elasticsearch/xpack/monitoring/exporter/http/testnode.pem");

        final Settings sslSettings = Settings.builder()
            .put("xpack.transport.security.ssl.certificate", cert)
            .put("xpack.transport.security.ssl.key", key)
            .put("xpack.transport.security.ssl.key_passphrase", "testnode")
            .putList("xpack.transport.security.ssl.supported_protocols", getProtocols())
            .put(globalSettings)
            .build();

        TestsSSLService sslService = new TestsSSLService(sslSettings, environment);
        final SSLContext sslContext = sslService.sslContext("xpack.security.transport.ssl");
        MockWebServer server = new MockWebServer(sslContext, false);
        server.start();
        return server;
    }

    @Before
    // Force the exporters to be built from closed secure settings (as they would be in a production environment)
    public void closeSecureSettings() throws IOException {
        if (secureSettings != null) {
            secureSettings.close();
        }
    }

    public void testCannotUpdateSslSettingsWithSecureSettings() throws Exception {
        // Verify that it was created even though it has a secure setting
        assertExporterExists("secure");

        // Verify that we cannot modify the SSL settings
        final ActionFuture<ClusterUpdateSettingsResponse> future = setVerificationMode("secure", VerificationMode.CERTIFICATE);
        final IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(iae.getCause(), instanceOf(IllegalStateException.class));
        assertThat(iae.getCause().getMessage(), containsString("secure_password"));
    }

    public void testCanUpdateSslSettingsWithNoSecureSettings() {
        final ActionFuture<ClusterUpdateSettingsResponse> future = setVerificationMode("plaintext", VerificationMode.CERTIFICATE);
        final ClusterUpdateSettingsResponse response = future.actionGet();
        assertThat(response, notNullValue());
        clearTransientSettings("plaintext");
    }

    public void testCanAddNewExporterWithSsl() {
        Path truststore = getDataPath("/org/elasticsearch/xpack/monitoring/exporter/http/testnode.jks");
        assertThat(Files.exists(truststore), CoreMatchers.is(true));

        final ClusterUpdateSettingsRequest updateSettings = new ClusterUpdateSettingsRequest();
        final Settings settings = Settings.builder()
            .put("xpack.monitoring.exporters._new.type", "http")
            .put("xpack.monitoring.exporters._new.host", "https://" + webServer.getHostName() + ":" + webServer.getPort())
            .put("xpack.monitoring.exporters._new.ssl.truststore.path", truststore)
            .put("xpack.monitoring.exporters._new.ssl.truststore.password", "testnode")
            .put("xpack.monitoring.exporters._new.ssl.verification_mode", VerificationMode.CERTIFICATE.name())
            .build();
        updateSettings.transientSettings(settings);
        final ActionFuture<ClusterUpdateSettingsResponse> future = client().admin().cluster().updateSettings(updateSettings);
        final ClusterUpdateSettingsResponse response = future.actionGet();
        assertThat(response, notNullValue());

        assertExporterExists("_new");
        clearTransientSettings("_new");
    }

    private void assertExporterExists(String secure) {
        final Exporter httpExporter = getExporter(secure);
        assertThat(httpExporter, notNullValue());
        assertThat(httpExporter, instanceOf(HttpExporter.class));
    }

    private Exporter getExporter(String name) {
        final Exporters exporters = internalCluster().getInstance(Exporters.class);
        assertThat(exporters, notNullValue());
        return exporters.getExporter(name);
    }

    private ActionFuture<ClusterUpdateSettingsResponse> setVerificationMode(String name, VerificationMode mode) {
        final ClusterUpdateSettingsRequest updateSettings = new ClusterUpdateSettingsRequest();
        final Settings settings = Settings.builder()
            .put("xpack.monitoring.exporters." + name + ".ssl.verification_mode", mode.name())
            .build();
        updateSettings.transientSettings(settings);
        return client().admin().cluster().updateSettings(updateSettings);
    }

    private void clearTransientSettings(String... names) {
        final ClusterUpdateSettingsRequest updateSettings = new ClusterUpdateSettingsRequest();
        final Settings.Builder builder = Settings.builder();
        for (String name : names) {
            builder.put("xpack.monitoring.exporters." + name + ".*", (String) null);
        }
        updateSettings.transientSettings(builder.build());
        client().admin().cluster().updateSettings(updateSettings).actionGet();
    }


    /**
     * The {@link HttpsServer} in the JDK has issues with TLSv1.3 when running in a JDK prior to
     * 12.0.1 so we pin to TLSv1.2 when running on an earlier JDK
     */
    private static List<String> getProtocols() {
        if (JavaVersion.current().compareTo(JavaVersion.parse("12")) < 0) {
            return List.of("TLSv1.2");
        } else {
            JavaVersion full =
                AccessController.doPrivileged(
                    (PrivilegedAction<JavaVersion>) () -> JavaVersion.parse(System.getProperty("java.version")));
            if (full.compareTo(JavaVersion.parse("12.0.1")) < 0) {
                return List.of("TLSv1.2");
            }
        }
        return XPackSettings.DEFAULT_SUPPORTED_PROTOCOLS;
    }
}
