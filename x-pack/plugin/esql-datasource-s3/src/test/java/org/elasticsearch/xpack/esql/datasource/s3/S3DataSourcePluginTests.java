/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.core.SdkSystemSetting;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceTelemetryVocabulary.Type;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderServices;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_WEB_IDENTITY_TOKEN_FILE;

/**
 * Lifecycle tests for {@link S3DataSourcePlugin}: storage providers are wired through the
 * {@link StorageProviderServices} SPI (not the removed static bridge), workload-identity sources are
 * built lazily on first use, and {@link S3DataSourcePlugin#close()} is safe whether or not
 * {@code storageProviders} ran first.
 *
 * <p>These tests assume the EKS workload-identity environment variables are unset in the test JVM so
 * that the IRSA provider stays inactive (no file watcher, no STS client) and the Pod Identity sysprop
 * redirect is a no-op; the env-var/symlink activation matrix is covered by
 * {@link CustomWebIdentityTokenCredentialsProviderTests} and the QA integration tests.
 */
public class S3DataSourcePluginTests extends ESTestCase {

    private Environment environment;

    @Before
    public void initEnvironment() {
        assumeTrue(
            "EKS IRSA env var must be unset for this test",
            System.getenv(AWS_WEB_IDENTITY_TOKEN_FILE.environmentVariable()) == null
        );
        assumeTrue(
            "EKS Pod Identity env var must be unset for this test",
            System.getenv(AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.environmentVariable()) == null
        );
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        environment = TestEnvironment.newEnvironment(settings);
    }

    private StorageProviderServices services() {
        // A mock watcher keeps the test robust even on a host that happens to export the EKS env vars:
        // it removes the only NPE the inactive-provider path could hit (ResourceWatcherService.add).
        return new StorageProviderServices(
            Settings.EMPTY,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            environment,
            mock(ResourceWatcherService.class)
        );
    }

    public void testStorageProvidersRegistersS3Schemes() throws IOException {
        try (S3DataSourcePlugin plugin = new S3DataSourcePlugin()) {
            Map<String, StorageProviderFactory> providers = plugin.storageProviders(services());
            assertTrue("should register s3 scheme", providers.containsKey("s3"));
            assertTrue("should register s3a scheme", providers.containsKey("s3a"));
            assertTrue("should register s3n scheme", providers.containsKey("s3n"));
            assertEquals("should register exactly 3 schemes", 3, providers.size());
        }
    }

    public void testSupportedSchemes() throws IOException {
        try (S3DataSourcePlugin plugin = new S3DataSourcePlugin()) {
            assertEquals(Set.of("s3", "s3a", "s3n"), plugin.supportedSchemes());
        }
    }

    public void testSchemeFoldAgreesWithTypeId() throws IOException {
        try (S3DataSourcePlugin plugin = new S3DataSourcePlugin()) {
            String typeId = plugin.datasourceValidators(Settings.EMPTY).keySet().iterator().next();
            for (String scheme : plugin.supportedSchemes()) {
                assertSame(Type.fromTypeId(typeId), Type.fromScheme(scheme));
            }
        }
    }

    public void testRegisteredValidatorConstrainsTheEndpoint() throws IOException {
        // S3DataSourceValidatorTests builds its own validator, so it cannot notice the plugin dropping the
        // endpoint constraint from the one it actually registers. This asserts on the registered instance.
        try (S3DataSourcePlugin plugin = new S3DataSourcePlugin()) {
            DataSourceValidator validator = plugin.datasourceValidators(Settings.EMPTY).get("s3");
            var e = expectThrows(
                ValidationException.class,
                () -> validator.validateDatasource(Map.of("endpoint", "https://minio.example.com:9000", "auth", "anonymous"))
            );
            assertThat(e.getMessage(), containsString("not a supported AWS S3 endpoint"));
            var accepted = validator.validateDatasource(Map.of("endpoint", "https://s3.us-east-1.amazonaws.com", "auth", "anonymous"));
            assertEquals("https://s3.us-east-1.amazonaws.com", accepted.get("endpoint").nonSecretValue());
        }
    }

    /**
     * The operator allow-list, driven through the automaton the plugin actually builds rather than a
     * hand-written predicate: {@code S3EndpointCheckTests} supplies its own {@code Predicate<String>}, so it
     * cannot notice the glob compilation or the {@code host:port} spelling the plugin matches against.
     *
     * <p>Two of the entries name a port rather than a wildcard, and that is deliberate: a URL carrying no
     * port has to acquire its scheme's default before {@code host:port} can match one of them, so these are
     * the cases that hold the default-port inference. A wildcard entry would match whatever number the
     * inference produced and prove nothing about it.
     */
    public void testAllowlistedHostIsAcceptedOverPlainHttp() throws IOException {
        Settings settings = Settings.builder()
            .putList("esql.external.allowed_endpoint_hosts", "127.0.0.1:9000", "localhost:80", "127.0.0.1:443")
            .build();
        try (S3DataSourcePlugin plugin = new S3DataSourcePlugin()) {
            DataSourceValidator validator = plugin.datasourceValidators(settings).get("s3");
            for (String endpoint : List.of(
                "http://127.0.0.1:9000", // the named port, matched as written
                "http://localhost",      // only matches localhost:80 if http's default port is inferred
                "https://127.0.0.1"      // and this one only if https infers 443
            )) {
                var accepted = validator.validateDatasource(Map.of("endpoint", endpoint, "auth", "anonymous"));
                assertEquals(endpoint, accepted.get("endpoint").nonSecretValue());
            }
            // The list is the enable: a host it does not name gets no waiver, on either the port or the name.
            for (String endpoint : List.of("http://127.0.0.1:9001", "http://127.0.0.2:9000")) {
                var e = expectThrows(
                    ValidationException.class,
                    () -> validator.validateDatasource(Map.of("endpoint", endpoint, "auth", "anonymous"))
                );
                assertThat(e.getMessage(), containsString("must use https"));
            }
            // An https host the list does not name falls through to the AWS host rule, not to a waiver.
            var e = expectThrows(
                ValidationException.class,
                () -> validator.validateDatasource(Map.of("endpoint", "https://127.0.0.1:8443", "auth", "anonymous"))
            );
            assertThat(e.getMessage(), containsString("not a supported AWS S3 endpoint"));
        }
    }

    /** The production default is an empty list, which must waive neither of the two rules it can waive. */
    public void testEmptyAllowlistWaivesNothing() throws IOException {
        try (S3DataSourcePlugin plugin = new S3DataSourcePlugin()) {
            DataSourceValidator validator = plugin.datasourceValidators(Settings.EMPTY).get("s3");
            var overHttp = expectThrows(
                ValidationException.class,
                () -> validator.validateDatasource(Map.of("endpoint", "http://127.0.0.1:9000", "auth", "anonymous"))
            );
            assertThat(overHttp.getMessage(), containsString("must use https"));
            // The same host over https clears the scheme rule and must still be refused by the host rule.
            var overHttps = expectThrows(
                ValidationException.class,
                () -> validator.validateDatasource(Map.of("endpoint", "https://127.0.0.1:9000", "auth", "anonymous"))
            );
            assertThat(overHttps.getMessage(), containsString("not a supported AWS S3 endpoint"));
        }
    }

    public void testS3SchemesShareSameFactory() throws IOException {
        try (S3DataSourcePlugin plugin = new S3DataSourcePlugin()) {
            Map<String, StorageProviderFactory> providers = plugin.storageProviders(services());
            StorageProviderFactory s3 = providers.get("s3");
            assertNotNull(s3);
            assertSame(s3, providers.get("s3a"));
            assertSame(s3, providers.get("s3n"));
        }
    }

    public void testCloseIsSafeBeforeStorageProviders() throws IOException {
        new S3DataSourcePlugin().close();
    }

    public void testCloseIsSafeAfterStorageProviders() throws IOException {
        S3DataSourcePlugin plugin = new S3DataSourcePlugin();
        plugin.storageProviders(services());
        plugin.close();
        // A second close must be a no-op: the provider object exists but is inactive (holds no STS
        // client/watcher to release), and the Pod Identity sysprop was never set.
        plugin.close();
    }

    public void testPodIdentitySyspropNotSetWhenEnvVarUnset() throws IOException {
        String before = System.getProperty(SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.property());
        try (S3DataSourcePlugin plugin = new S3DataSourcePlugin()) {
            plugin.storageProviders(services());
            assertEquals(
                "sysprop must be untouched when the Pod Identity env var is unset",
                before,
                System.getProperty(SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.property())
            );
        }
    }
}
