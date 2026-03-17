/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SingleSpSamlRealmSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.TestsSSLService;
import org.junit.Before;
import org.opensaml.saml.common.xml.SAMLConstants;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.saml2.metadata.IDPSSODescriptor;
import org.opensaml.saml.saml2.metadata.SingleSignOnService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivilegedActionException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link SamlMetadataResolver}.
 */
public class SamlMetadataResolverTests extends SamlTestCase {

    private static final String REALM_NAME = "my-saml";
    private static final String REALM_SETTINGS_PREFIX = "xpack.security.authc.realms.saml." + REALM_NAME;
    private static final String TEST_IDP_ENTITY_ID = "http://demo_josso_1.josso.dev.docker:8081/IDBUS/JOSSO-TUTORIAL/IDP1/SAML2/MD";
    private static final TimeValue METADATA_REFRESH = TimeValue.timeValueMillis(3000);

    private Settings globalSettings;
    private TestThreadPool threadPool;

    @Before
    public void setupEnv() throws PrivilegedActionException {
        SamlUtils.initialize(logger);
        globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        threadPool = new TestThreadPool("saml-metadata-resolver-tests", globalSettings);
    }

    @org.junit.After
    public void shutdownThreadPool() {
        if (threadPool != null) {
            threadPool.shutdown();
        }
    }

    public void testReadIdpMetadataFromFile() throws Exception {
        final Path path = getDataPath("idp1.xml");
        Tuple<RealmConfig, SSLService> config = buildConfig(path.toString());
        final ResourceWatcherService watcherService = mock(ResourceWatcherService.class);
        try (SamlMetadataResolver resolver = SamlMetadataResolver.create(logger, config.v1(), config.v2(), watcherService, threadPool)) {
            assertIdp1MetadataParsedCorrectly(resolver.get());
        }
    }

    public void testReadIdpMetadataFromHttps() throws Exception {
        final Path path = getDataPath("idp1.xml");
        final String body = Files.readString(path);
        TestsSSLService sslService = buildTestSslService();
        try (MockWebServer proxyServer = new MockWebServer(sslService.sslContext("xpack.security.http.ssl"), false)) {
            proxyServer.start();
            proxyServer.enqueue(new MockResponse().setResponseCode(200).setBody(body).addHeader("Content-Type", "application/xml"));
            proxyServer.enqueue(new MockResponse().setResponseCode(200).setBody(body).addHeader("Content-Type", "application/xml"));
            assertEquals(0, proxyServer.requests().size());

            Tuple<RealmConfig, SSLService> config = buildConfig("https://localhost:" + proxyServer.getPort());
            logger.info("Settings\n{}", config.v1().settings().toDelimitedString('\n'));
            final ResourceWatcherService watcherService = mock(ResourceWatcherService.class);
            try (
                SamlMetadataResolver resolver = SamlMetadataResolver.create(logger, config.v1(), config.v2(), watcherService, threadPool)
            ) {
                assertThat(proxyServer.requests().size(), greaterThanOrEqualTo(1));
                assertIdp1MetadataParsedCorrectly(resolver.get());
            }
        }
    }

    public void testFailOnErrorForInvalidHttpsMetadata() throws Exception {
        TestsSSLService sslService = buildTestSslService();
        try (MockWebServer webServer = new MockWebServer(sslService.sslContext("xpack.security.http.ssl"), false)) {
            webServer.start();
            webServer.enqueue(
                new MockResponse().setResponseCode(randomIntBetween(400, 405))
                    .setBody("No metadata available")
                    .addHeader("Content-Type", "text/plain")
            );
            assertEquals(0, webServer.requests().size());

            var metadataPath = "https://localhost:" + webServer.getPort();
            final Tuple<RealmConfig, SSLService> config = buildConfig(
                metadataPath,
                settings -> settings.put(
                    SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_HTTP_FAIL_ON_ERROR),
                    true
                )
            );
            final ResourceWatcherService watcherService = mock(ResourceWatcherService.class);
            var exception = expectThrows(
                ElasticsearchSecurityException.class,
                () -> SamlMetadataResolver.create(logger, config.v1(), config.v2(), watcherService, threadPool)
            );
            assertThat(exception, throwableWithMessage("cannot load SAML metadata from [" + metadataPath + "]"));
        }
    }

    public void testRetryFailedHttpsMetadata() throws Exception {
        final Path path = getDataPath("idp1.xml");
        final String body = Files.readString(path);
        TestsSSLService sslService = buildTestSslService();
        doTestReloadFailedHttpsMetadata(body, sslService, true);
        doTestReloadFailedHttpsMetadata(body, sslService, false);
    }

    /**
     * @param testBackgroundRefresh If {@code true}, the test asserts that the metadata is automatically loaded in the background.
     *                              If {@code false}, the test triggers activity on the realm to force a reload of the metadata.
     */
    private void doTestReloadFailedHttpsMetadata(String metadataBody, TestsSSLService sslService, boolean testBackgroundRefresh)
        throws Exception {
        try (MockWebServer webServer = new MockWebServer(sslService.sslContext("xpack.security.http.ssl"), false)) {
            webServer.start();
            webServer.enqueue(
                new MockResponse().setResponseCode(randomIntBetween(400, 405))
                    .setBody("No metadata available")
                    .addHeader("Content-Type", "text/plain")
            );
            assertEquals(0, webServer.requests().size());

            // Even with a long refresh we should automatically retry metadata that fails
            final TimeValue defaultRefreshTime = TimeValue.timeValueHours(24);
            // OpenSAML (4.0) has a bug that can attempt to set negative duration timers if the refresh is too short.
            // Don't set this too small or we may hit "java.lang.IllegalArgumentException: Negative delay."
            final TimeValue minimumRefreshTime = testBackgroundRefresh ? TimeValue.timeValueMillis(500) : defaultRefreshTime;

            final Tuple<RealmConfig, SSLService> config = buildConfig("https://localhost:" + webServer.getPort(), builder -> {
                builder.put(
                    SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_HTTP_REFRESH),
                    defaultRefreshTime.getStringRep()
                );
                builder.put(
                    SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_HTTP_MIN_REFRESH),
                    minimumRefreshTime.getStringRep()
                );
                if (randomBoolean()) {
                    builder.put(
                        SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_HTTP_FAIL_ON_ERROR),
                        false
                    );
                }
            });
            logger.info("Settings\n{}", config.v1().settings().toDelimitedString('\n'));
            final ResourceWatcherService watcherService = mock(ResourceWatcherService.class);
            try (
                SamlMetadataResolver resolver = SamlMetadataResolver.create(logger, config.v1(), config.v2(), watcherService, threadPool)
            ) {
                final int firstRequestCount = webServer.requests().size();
                assertThat(firstRequestCount, greaterThanOrEqualTo(1));
                assertThat(resolver.get(), instanceOf(UnresolvedEntity.class));

                webServer.enqueue(
                    new MockResponse().setResponseCode(200).setBody(metadataBody).addHeader("Content-Type", "application/xml")
                );

                // This would typically run in the background (via a scheduled task every 60s)
                // but we don't want the test to wait that long, so we force it
                resolver.asyncUpdateCachedDescriptor();

                assertBusy(() -> assertIdp1MetadataParsedCorrectly(resolver.get()), 3, TimeUnit.SECONDS);
            }
        }
    }

    public void testHttpMetadataWithCustomTimeouts() throws Exception {
        final Path path = getDataPath("idp1.xml");
        final String body = Files.readString(path);
        TestsSSLService sslService = buildTestSslService();
        try (MockWebServer proxyServer = new MockWebServer(sslService.sslContext("xpack.security.http.ssl"), false)) {
            proxyServer.start();
            proxyServer.enqueue(new MockResponse().setResponseCode(200).setBody(body).addHeader("Content-Type", "application/xml"));

            final TimeValue customConnectTimeout = TimeValue.timeValueSeconds(3);
            final TimeValue customReadTimeout = TimeValue.timeValueSeconds(15);

            Tuple<RealmConfig, SSLService> config = buildConfig("https://localhost:" + proxyServer.getPort(), builder -> {
                builder.put(
                    SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_HTTP_CONNECT_TIMEOUT),
                    customConnectTimeout.getStringRep()
                );
                builder.put(
                    SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_HTTP_READ_TIMEOUT),
                    customReadTimeout.getStringRep()
                );
            });

            // Verify settings are correctly configured
            assertThat(config.v1().getSetting(SamlRealmSettings.IDP_METADATA_HTTP_CONNECT_TIMEOUT), equalTo(customConnectTimeout));
            assertThat(config.v1().getSetting(SamlRealmSettings.IDP_METADATA_HTTP_READ_TIMEOUT), equalTo(customReadTimeout));

            final ResourceWatcherService watcherService = mock(ResourceWatcherService.class);
            try (
                SamlMetadataResolver resolver = SamlMetadataResolver.create(logger, config.v1(), config.v2(), watcherService, threadPool)
            ) {
                assertThat(proxyServer.requests().size(), greaterThanOrEqualTo(1));
                assertIdp1MetadataParsedCorrectly(resolver.get());
            }
        }
    }

    public void testHttpMetadataWithDefaultTimeouts() throws Exception {
        final Path path = getDataPath("idp1.xml");
        final String body = Files.readString(path);
        TestsSSLService sslService = buildTestSslService();
        try (MockWebServer proxyServer = new MockWebServer(sslService.sslContext("xpack.security.http.ssl"), false)) {
            proxyServer.start();
            proxyServer.enqueue(new MockResponse().setResponseCode(200).setBody(body).addHeader("Content-Type", "application/xml"));

            Tuple<RealmConfig, SSLService> config = buildConfig("https://localhost:" + proxyServer.getPort());

            // Verify default timeout values are used
            assertThat(config.v1().getSetting(SamlRealmSettings.IDP_METADATA_HTTP_CONNECT_TIMEOUT), equalTo(TimeValue.timeValueSeconds(5)));
            assertThat(config.v1().getSetting(SamlRealmSettings.IDP_METADATA_HTTP_READ_TIMEOUT), equalTo(TimeValue.timeValueSeconds(10)));

            final ResourceWatcherService watcherService = mock(ResourceWatcherService.class);
            try (
                SamlMetadataResolver resolver = SamlMetadataResolver.create(logger, config.v1(), config.v2(), watcherService, threadPool)
            ) {
                assertThat(proxyServer.requests().size(), greaterThanOrEqualTo(1));
                assertIdp1MetadataParsedCorrectly(resolver.get());
            }
        }
    }

    public void testHttpMetadataConnectionTimeout() throws Exception {
        // Use a non-routable IP address to simulate connection timeout
        // 192.0.2.1 is reserved for documentation and will not be routable
        final String unreachableUrl = "https://192.0.2.1:9999/metadata.xml";
        final TimeValue shortConnectTimeout = TimeValue.timeValueMillis(100);

        Tuple<RealmConfig, SSLService> config = buildConfig(unreachableUrl, builder -> {
            builder.put(
                SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_HTTP_CONNECT_TIMEOUT),
                shortConnectTimeout.getStringRep()
            );
            builder.put(SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_HTTP_FAIL_ON_ERROR), false);
        });

        final ResourceWatcherService watcherService = mock(ResourceWatcherService.class);

        // initialization should complete even though the connection fails
        try (SamlMetadataResolver resolver = SamlMetadataResolver.create(logger, config.v1(), config.v2(), watcherService, threadPool)) {
            EntityDescriptor descriptor = resolver.get();
            assertThat(descriptor, instanceOf(UnresolvedEntity.class));
        }
    }

    private Tuple<RealmConfig, SSLService> buildConfig(String idpMetadataPath) {
        return buildConfig(idpMetadataPath, ignore -> {});
    }

    private Tuple<RealmConfig, SSLService> buildConfig(String idpMetadataPath, Consumer<Settings.Builder> additionalSettings) {
        var settings = buildSettings(idpMetadataPath);
        additionalSettings.accept(settings);
        final RealmConfig config = realmConfigFromGlobalSettings(settings.build());
        final SSLService sslService = new SSLService(config.env());
        return new Tuple<>(config, sslService);
    }

    private Settings.Builder buildSettings(String idpMetadataPath) {
        MockSecureSettings secureSettings = new MockSecureSettings();
        final String realmType = SingleSpSamlRealmSettings.TYPE;
        secureSettings.setString(REALM_SETTINGS_PREFIX + ".ssl.secure_key_passphrase", "testnode");
        return Settings.builder()
            .put(REALM_SETTINGS_PREFIX + ".ssl.verification_mode", "certificate")
            .put(
                REALM_SETTINGS_PREFIX + ".ssl.key",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem")
            )
            .put(
                REALM_SETTINGS_PREFIX + ".ssl.certificate",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt")
            )
            .put(
                REALM_SETTINGS_PREFIX + ".ssl.certificate_authorities",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt")
            )
            .put(SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_PATH), idpMetadataPath)
            .put(SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_ENTITY_ID), TEST_IDP_ENTITY_ID)
            .put(
                SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_HTTP_REFRESH),
                METADATA_REFRESH.getStringRep()
            )
            .put(RealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.PRINCIPAL_ATTRIBUTE.apply(realmType).getAttribute()), "uid")
            .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
            .put("path.home", createTempDir())
            .setSecureSettings(secureSettings);
    }

    private RealmConfig realmConfigFromGlobalSettings(Settings globalSettings) {
        final Environment env = TestEnvironment.newEnvironment(globalSettings);
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("saml", REALM_NAME);
        return new RealmConfig(
            realmIdentifier,
            Settings.builder()
                .put(globalSettings)
                .put(RealmSettings.getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0)
                .build(),
            env,
            new org.elasticsearch.common.util.concurrent.ThreadContext(globalSettings)
        );
    }

    private TestsSSLService buildTestSslService() throws IOException {
        final MockSecureSettings mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString("xpack.security.http.ssl.secure_key_passphrase", "testnode");
        final Settings settings = Settings.builder()
            .put("xpack.security.http.ssl.enabled", true)
            .put("xpack.security.http.ssl.key", getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
            .put(
                "xpack.security.http.ssl.certificate",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt")
            )
            .put(
                "xpack.security.http.ssl.certificate_authorities",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt")
            )
            .putList("xpack.security.http.ssl.supported_protocols", XPackSettings.DEFAULT_SUPPORTED_PROTOCOLS)
            .put("path.home", createTempDir())
            .setSecureSettings(mockSecureSettings)
            .build();
        return new TestsSSLService(TestEnvironment.newEnvironment(settings));
    }

    private void assertIdp1MetadataParsedCorrectly(EntityDescriptor descriptor) {
        try {
            IDPSSODescriptor idpssoDescriptor = descriptor.getIDPSSODescriptor(SAMLConstants.SAML20P_NS);
            assertNotNull(idpssoDescriptor);
            List<SingleSignOnService> ssoServices = idpssoDescriptor.getSingleSignOnServices();
            assertEquals(2, ssoServices.size());
            assertEquals(SAMLConstants.SAML2_POST_BINDING_URI, ssoServices.get(0).getBinding());
            assertEquals(SAMLConstants.SAML2_REDIRECT_BINDING_URI, ssoServices.get(1).getBinding());
        } catch (ElasticsearchSecurityException e) {
            // Convert a SAML exception into an assertion failure so that we can `assertBusy` on these checks
            throw new AssertionError("Failed to retrieve IdP metadata", e);
        }
    }
}
