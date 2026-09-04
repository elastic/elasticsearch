/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.core.SdkSystemSetting;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderServices;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

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
