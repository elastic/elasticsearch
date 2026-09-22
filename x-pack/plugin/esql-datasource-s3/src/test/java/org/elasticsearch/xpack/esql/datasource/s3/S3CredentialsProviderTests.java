/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.core.SdkSystemSetting;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceConfiguration.AuthMode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

/**
 * Tests the mapping from a parsed {@link S3Configuration} to the AWS SDK credential: the mode resolution
 * ({@link S3Configuration#resolveAuthMode()}) and the static-credential assembly
 * ({@link S3StorageProvider#buildStaticCredentials(S3Configuration)}), including STS temporary credentials.
 */
public class S3CredentialsProviderTests extends ESTestCase {

    public void testSessionCredentials() {
        S3Configuration config = S3Configuration.fromFields("ak", "sk", "tok", null, null, null);
        assertEquals(AuthMode.STATIC_CREDENTIALS, config.resolveAuthMode());
        AwsCredentials creds = S3StorageProvider.buildStaticCredentials(config).resolveCredentials();
        assertThat(creds, instanceOf(AwsSessionCredentials.class));
        AwsSessionCredentials session = (AwsSessionCredentials) creds;
        assertEquals("ak", session.accessKeyId());
        assertEquals("sk", session.secretAccessKey());
        assertEquals("tok", session.sessionToken());
    }

    public void testBasicCredentialsWhenNoSessionToken() {
        S3Configuration config = S3Configuration.fromFields("ak", "sk", null, null);
        assertEquals(AuthMode.STATIC_CREDENTIALS, config.resolveAuthMode());
        AwsCredentials creds = S3StorageProvider.buildStaticCredentials(config).resolveCredentials();
        assertThat(creds, instanceOf(AwsBasicCredentials.class));
        assertEquals("ak", creds.accessKeyId());
        assertEquals("sk", creds.secretAccessKey());
    }

    public void testResolveExplicitModes() {
        assertEquals(
            AuthMode.ANONYMOUS,
            S3Configuration.fromFields(null, null, "http://endpoint", "us-east-1", "anonymous").resolveAuthMode()
        );
        assertEquals(
            AuthMode.MANAGED_IDENTITY,
            S3Configuration.fromFields(null, null, "http://endpoint", "us-east-1", "managed_identity").resolveAuthMode()
        );
        assertEquals(
            AuthMode.STATIC_CREDENTIALS,
            S3Configuration.fromFields("ak", "sk", null, "http://endpoint", "us-east-1", "static_credentials").resolveAuthMode()
        );
        assertEquals(
            AuthMode.FEDERATED_IDENTITY,
            S3Configuration.fromFederatedFields("arn", null, "aud", null, null, "http://endpoint", "us-east-1").resolveAuthMode()
        );
    }

    public void testEmptySessionTokenTreatedAsAbsent() {
        // An empty session token is treated as absent: with keys present, this falls back to basic credentials
        // rather than building STS credentials with an empty token (which would fail later at request signing).
        S3Configuration config = S3Configuration.fromMap(Map.of("access_key", "ak", "secret_key", "sk", "session_token", ""));
        assertEquals(AuthMode.STATIC_CREDENTIALS, config.resolveAuthMode());
        AwsCredentials creds = S3StorageProvider.buildStaticCredentials(config).resolveCredentials();
        assertThat(creds, instanceOf(AwsBasicCredentials.class));
    }

    public void testSessionTokenWithoutKeysRejectedAtCreate() {
        // auth=auto with only a session_token resolves to nothing (no complete static credential, no federated
        // settings), so it is rejected at create time rather than deferred to query time.
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> S3Configuration.fromFields(null, null, "tok", null, null, null)
        );
        assertTrue(e.getMessage().contains("S3 data source requires credentials"));
    }

    public void testNoCredentialsRejectedAtCreate() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> S3Configuration.fromFields(null, null, "http://endpoint", "us-east-1")
        );
        assertTrue(e.getMessage().contains("S3 data source requires credentials"));
    }

    /**
     * Without an IRSA / Pod Identity singleton wired through, the workload-identity chain is just
     * ContainerCredentialsProvider followed by InstanceProfileCredentialsProvider — the v1
     * shape. Behavior on every non-EKS deployment.
     */
    public void testManagedIdentityChainExcludesIrsaWhenSingletonAbsent() {
        List<AwsCredentialsProvider> providers = S3StorageProvider.forTesting(null, null).managedIdentityProviders();
        assertThat(providers, hasSize(2));
        assertThat(providers.get(0), instanceOf(ContainerCredentialsProvider.class));
        assertThat(providers.get(1), instanceOf(InstanceProfileCredentialsProvider.class));
    }

    /**
     * An inactive singleton (env var unset on the node) is treated the same as no singleton at
     * all: skipped from the chain so we don't attempt STS calls that have no chance of succeeding.
     */
    public void testManagedIdentityChainExcludesIrsaWhenSingletonInactive() {
        CustomWebIdentityTokenCredentialsProvider inactive = new CustomWebIdentityTokenCredentialsProvider(null, null, null, name -> null);
        assertFalse("test setup: provider must be inactive when env returns null for everything", inactive.isActive());
        List<AwsCredentialsProvider> providers = new S3StorageProvider(null, null, inactive).managedIdentityProviders();
        assertThat(providers, hasSize(2));
        assertThat(providers.get(0), instanceOf(ContainerCredentialsProvider.class));
        assertThat(providers.get(1), instanceOf(InstanceProfileCredentialsProvider.class));
    }

    /**
     * An active Pod Identity provider replaces the stock ContainerCredentialsProvider in the chain.
     */
    public void testManagedIdentityChainUsesEsqlContainerProviderWhenActive() throws IOException {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Environment environment = TestEnvironment.newEnvironment(settings);
        Path tokenFile = environment.configDir().resolve(EsqlContainerCredentialsProvider.POD_IDENTITY_TOKEN_FILE_LOCATION);
        Files.createDirectories(tokenFile.getParent());
        Files.writeString(tokenFile, "token");
        try (EsqlContainerCredentialsProvider active = new EsqlContainerCredentialsProvider(environment, null, name -> {
            if (name.equals(SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.environmentVariable())) {
                return "/var/run/secrets/token";
            }
            if (name.equals(SdkSystemSetting.AWS_CONTAINER_CREDENTIALS_FULL_URI.environmentVariable())) {
                return "http://127.0.0.1/creds";
            }
            return null;
        })) {
            assertTrue(active.isActive());
            List<AwsCredentialsProvider> providers = new S3StorageProvider(null, null, null, active).managedIdentityProviders();
            assertThat(providers, hasSize(2));
            assertThat(providers.get(0), instanceOf(ErrorLoggingCredentialsProvider.class));
            assertThat(providers.get(1), instanceOf(InstanceProfileCredentialsProvider.class));
            for (AwsCredentialsProvider provider : providers) {
                assertFalse(provider instanceof ContainerCredentialsProvider);
            }
        }
    }

    /**
     * A misconfigured Pod Identity provider (env set, entitled symlink missing) must fail loudly
     * rather than fall through to the stock ContainerCredentialsProvider — but only when no earlier
     * provider is already in the chain.
     */
    public void testManagedIdentityChainFailsWhenPodIdentityMisconfigured() {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Environment environment = TestEnvironment.newEnvironment(settings);
        EsqlContainerCredentialsProvider misconfigured = new EsqlContainerCredentialsProvider(environment, null, name -> {
            if (name.equals(SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.environmentVariable())) {
                return "/var/run/secrets/token";
            }
            if (name.equals(SdkSystemSetting.AWS_CONTAINER_CREDENTIALS_FULL_URI.environmentVariable())) {
                return "http://127.0.0.1/creds";
            }
            return null;
        });
        assertTrue(misconfigured.isMisconfigured());
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> new S3StorageProvider(null, null, null, misconfigured).managedIdentityProviders()
        );
        assertTrue(e.getMessage().contains(EsqlContainerCredentialsProvider.POD_IDENTITY_TOKEN_FILE_LOCATION));
    }

    /**
     * Active IRSA + misconfigured Pod Identity must keep IRSA in the chain and skip stock CCP —
     * a working IRSA source must not be aborted by a missing ES|QL Pod Identity symlink.
     */
    @SuppressForbidden(reason = "AWS SDK StsClient builder requires a region; pinning one for this test method")
    public void testManagedIdentityChainKeepsIrsaWhenPodIdentityMisconfigured() throws IOException {
        String previousAwsRegion = System.setProperty("aws.region", "us-east-1");
        try {
            Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
            Environment environment = TestEnvironment.newEnvironment(settings);
            Path irsaToken = environment.configDir().resolve(CustomWebIdentityTokenCredentialsProvider.WEB_IDENTITY_TOKEN_FILE_LOCATION);
            Files.createDirectories(irsaToken.getParent());
            Files.writeString(irsaToken, "fake-irsa-token");

            try (
                CustomWebIdentityTokenCredentialsProvider irsa = new CustomWebIdentityTokenCredentialsProvider(
                    environment,
                    Clock.systemUTC(),
                    mock(ResourceWatcherService.class),
                    name -> switch (name) {
                        case "AWS_WEB_IDENTITY_TOKEN_FILE" -> "/var/run/secrets/eks.amazonaws.com/serviceaccount/token";
                        case "AWS_ROLE_ARN" -> "arn:aws:iam::123456789012:role/test";
                        default -> null;
                    }
                )
            ) {
                assertTrue(irsa.isActive());
                EsqlContainerCredentialsProvider misconfigured = new EsqlContainerCredentialsProvider(environment, null, name -> {
                    if (name.equals(SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.environmentVariable())) {
                        return "/var/run/secrets/token";
                    }
                    if (name.equals(SdkSystemSetting.AWS_CONTAINER_CREDENTIALS_FULL_URI.environmentVariable())) {
                        return "http://127.0.0.1/creds";
                    }
                    return null;
                });
                assertTrue(misconfigured.isMisconfigured());

                List<AwsCredentialsProvider> providers = new S3StorageProvider(null, null, irsa, misconfigured).managedIdentityProviders();
                assertThat(providers, hasSize(2));
                assertThat(providers.get(0), instanceOf(ErrorLoggingCredentialsProvider.class));
                assertThat(providers.get(1), instanceOf(InstanceProfileCredentialsProvider.class));
                for (AwsCredentialsProvider provider : providers) {
                    assertFalse(provider instanceof ContainerCredentialsProvider);
                }
            }
        } finally {
            if (previousAwsRegion == null) {
                System.clearProperty("aws.region");
            } else {
                System.setProperty("aws.region", previousAwsRegion);
            }
        }
    }

    /**
     * Sanity check the v1 providers are listed in the documented order: container credentials
     * (ECS / Pod Identity) before EC2 instance profile.
     */
    public void testV1ChainOrder() {
        List<AwsCredentialsProvider> providers = S3StorageProvider.forTesting(null, null).managedIdentityProviders();
        assertThat(providers, hasSize(2));
        assertThat(providers.get(0), instanceOf(ContainerCredentialsProvider.class));
        assertThat(providers.get(1), instanceOf(InstanceProfileCredentialsProvider.class));
    }
}
