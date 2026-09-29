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
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceTelemetryVocabulary.Type;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderServices;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
 * that the IRSA and Pod Identity providers stay inactive (no file watcher, no credentials client);
 * the env-var/symlink activation matrix is covered by
 * {@link CustomWebIdentityTokenCredentialsProviderTests}, {@link EsqlContainerCredentialsProviderTests},
 * and the QA integration tests.
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

    /** Host names are case-insensitive, so neither the entry's case nor the URL's decides the match. */
    public void testAllowlistIgnoresCase() throws IOException {
        Settings settings = Settings.builder().putList(ExternalSourceSettings.ALLOWED_ENDPOINT_HOSTS_KEY, "MinIO.Internal:9000").build();
        try (S3DataSourcePlugin plugin = new S3DataSourcePlugin()) {
            DataSourceValidator validator = plugin.datasourceValidators(settings).get("s3");
            for (String endpoint : List.of("http://minio.internal:9000", "http://MINIO.INTERNAL:9000")) {
                var accepted = validator.validateDatasource(Map.of("endpoint", endpoint, "auth", "anonymous"));
                assertEquals(endpoint, accepted.get("endpoint").nonSecretValue());
            }
        }
    }

    /** Drives the automaton the plugin builds; the port-bearing entries pin default-port inference. */
    public void testAllowlistedHostIsAcceptedOverPlainHttp() throws IOException {
        Settings settings = Settings.builder()
            .putList(ExternalSourceSettings.ALLOWED_ENDPOINT_HOSTS_KEY, "127.0.0.1:9000", "localhost:80", "127.0.0.1:443")
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
        // A second close must be a no-op: the provider objects exist but are inactive (hold no
        // STS client/watcher/credentials cache to release).
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

    public void testBuildingStorageProvidersDoesNotTouchTheJvmWideTokenProperty() throws IOException {
        // Pod Identity env is set (via the test seam) and the entitled symlink exists. Building the
        // sources must leave the JVM-wide property alone.
        Path tokenFile = environment.configDir().resolve(EsqlContainerCredentialsProvider.POD_IDENTITY_TOKEN_FILE_LOCATION);
        Files.createDirectories(tokenFile.getParent());
        Files.writeString(tokenFile, "unit-test-token");

        String before = System.getProperty(SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.property());
        Map<String, String> env = Map.of(
            AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.environmentVariable(),
            "/var/run/secrets/pods.eks.amazonaws.com/serviceaccount/token",
            SdkSystemSetting.AWS_CONTAINER_CREDENTIALS_FULL_URI.environmentVariable(),
            "http://127.0.0.1:1/creds"
        );
        try (S3DataSourcePlugin plugin = new S3DataSourcePlugin()) {
            plugin.initializeWorkloadIdentityForTesting(environment, mock(ResourceWatcherService.class), env::get);
            plugin.storageProviders(services());
            assertEquals(
                "sysprop must stay untouched when Pod Identity env is set",
                before,
                System.getProperty(SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.property())
            );
        }
    }
}
