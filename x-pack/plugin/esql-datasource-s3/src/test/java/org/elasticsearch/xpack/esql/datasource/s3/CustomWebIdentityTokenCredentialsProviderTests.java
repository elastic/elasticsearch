/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Clock;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.esql.datasource.s3.CustomWebIdentityTokenCredentialsProvider.WEB_IDENTITY_TOKEN_FILE_LOCATION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Activation matrix for {@link CustomWebIdentityTokenCredentialsProvider}: the constructor turns
 * env vars + filesystem state into either an active provider or a no-op. These tests exercise
 * each branch by stubbing the env-var lookup and shaping {@code ${ES_PATH_CONF}} on disk.
 */
public class CustomWebIdentityTokenCredentialsProviderTests extends ESTestCase {

    private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/test";

    private Environment environment;
    private ResourceWatcherService resourceWatcherService;
    // The active-path test instantiates a real StsClient. The SDK's region resolution chain reads
    // AWS_REGION (env), aws.region (sysprop), then probes the EC2 metadata service. None of these
    // are available in unit tests, so we pin a dummy region via system property and clean up after.
    private static String previousAwsRegion;

    @BeforeClass
    @SuppressForbidden(reason = "AWS SDK's StsClient builder requires a region; pinning one for the duration of this test class")
    public static void setStsRegion() {
        previousAwsRegion = System.setProperty("aws.region", "us-east-1");
    }

    @AfterClass
    @SuppressForbidden(reason = "Restoring the aws.region property to its prior value after this test class completes")
    public static void restoreStsRegion() {
        if (previousAwsRegion == null) {
            System.clearProperty("aws.region");
        } else {
            System.setProperty("aws.region", previousAwsRegion);
        }
    }

    @Before
    public void initEnvironmentAndWatcher() throws IOException {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        environment = TestEnvironment.newEnvironment(settings);
        Files.createDirectories(environment.configDir().resolve("esql-datasource-s3"));
        resourceWatcherService = mock(ResourceWatcherService.class);
    }

    public void testInactiveWhenWebIdentityTokenFileEnvVarUnset() throws IOException {
        try (
            CustomWebIdentityTokenCredentialsProvider provider = new CustomWebIdentityTokenCredentialsProvider(
                environment,
                Clock.systemUTC(),
                resourceWatcherService,
                env(Map.of())
            )
        ) {
            assertFalse("missing AWS_WEB_IDENTITY_TOKEN_FILE must leave provider inactive", provider.isActive());
        }
    }

    public void testInactiveWhenWebIdentityTokenFileEnvVarBlank() throws IOException {
        try (
            CustomWebIdentityTokenCredentialsProvider provider = new CustomWebIdentityTokenCredentialsProvider(
                environment,
                Clock.systemUTC(),
                resourceWatcherService,
                env(Map.of("AWS_WEB_IDENTITY_TOKEN_FILE", "   ", "AWS_ROLE_ARN", ROLE_ARN))
            )
        ) {
            assertFalse("blank AWS_WEB_IDENTITY_TOKEN_FILE must leave provider inactive", provider.isActive());
        }
    }

    public void testInactiveWhenRoleArnBlank() throws IOException {
        Path tokenFile = environment.configDir().resolve(WEB_IDENTITY_TOKEN_FILE_LOCATION);
        Files.writeString(tokenFile, "fake-token");
        try (
            CustomWebIdentityTokenCredentialsProvider provider = new CustomWebIdentityTokenCredentialsProvider(
                environment,
                Clock.systemUTC(),
                resourceWatcherService,
                env(Map.of("AWS_WEB_IDENTITY_TOKEN_FILE", "/var/run/secrets/eks.amazonaws.com/serviceaccount/token", "AWS_ROLE_ARN", "   "))
            )
        ) {
            assertFalse("blank AWS_ROLE_ARN must leave provider inactive", provider.isActive());
        }
    }

    public void testInactiveWhenSymlinkAbsent() throws IOException {
        try (
            CustomWebIdentityTokenCredentialsProvider provider = new CustomWebIdentityTokenCredentialsProvider(
                environment,
                Clock.systemUTC(),
                resourceWatcherService,
                env(
                    Map.of(
                        "AWS_WEB_IDENTITY_TOKEN_FILE",
                        "/var/run/secrets/eks.amazonaws.com/serviceaccount/token",
                        "AWS_ROLE_ARN",
                        ROLE_ARN
                    )
                )
            )
        ) {
            assertFalse("AWS_WEB_IDENTITY_TOKEN_FILE set but entitled symlink absent must leave provider inactive", provider.isActive());
        }
    }

    public void testThrowsWhenSymlinkUnreadable() throws IOException {
        // POSIX permissions are required for this test; skip on filesystems that don't honor them
        // (e.g. native Windows). The plugin only ships on Linux/macOS in production.
        assumeTrue("requires POSIX file permissions", PathUtils.getDefaultFileSystem().supportedFileAttributeViews().contains("posix"));
        Path tokenFile = environment.configDir().resolve(WEB_IDENTITY_TOKEN_FILE_LOCATION);
        Files.writeString(tokenFile, "fake-token");
        try {
            Files.setPosixFilePermissions(tokenFile, EnumSet.noneOf(PosixFilePermission.class));
            // chmod 000 can still leave the file readable for the owning user on some filesystems
            // (e.g. macOS APFS when the test runs as root); skip if the permission did not stick.
            assumeTrue("filesystem honors chmod 000", Files.isReadable(tokenFile) == false);

            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> new CustomWebIdentityTokenCredentialsProvider(
                    environment,
                    Clock.systemUTC(),
                    resourceWatcherService,
                    env(
                        Map.of(
                            "AWS_WEB_IDENTITY_TOKEN_FILE",
                            "/var/run/secrets/eks.amazonaws.com/serviceaccount/token",
                            "AWS_ROLE_ARN",
                            ROLE_ARN
                        )
                    )
                )
            );
            assertTrue(e.getMessage().contains("not readable"));
        } finally {
            Files.setPosixFilePermissions(tokenFile, PosixFilePermissions.fromString("rw-------"));
        }
    }

    public void testInactiveWhenRoleArnMissing() throws IOException {
        Path tokenFile = environment.configDir().resolve(WEB_IDENTITY_TOKEN_FILE_LOCATION);
        Files.writeString(tokenFile, "fake-token");
        try (
            CustomWebIdentityTokenCredentialsProvider provider = new CustomWebIdentityTokenCredentialsProvider(
                environment,
                Clock.systemUTC(),
                resourceWatcherService,
                env(Map.of("AWS_WEB_IDENTITY_TOKEN_FILE", "/var/run/secrets/eks.amazonaws.com/serviceaccount/token"))
            )
        ) {
            assertFalse("AWS_ROLE_ARN missing must leave provider inactive", provider.isActive());
        }
    }

    public void testActiveWhenFullyConfiguredAndRegistersFileWatcher() throws IOException {
        Path tokenFile = environment.configDir().resolve(WEB_IDENTITY_TOKEN_FILE_LOCATION);
        Files.writeString(tokenFile, "fake-token");
        try (
            CustomWebIdentityTokenCredentialsProvider provider = new CustomWebIdentityTokenCredentialsProvider(
                environment,
                Clock.systemUTC(),
                resourceWatcherService,
                env(
                    Map.of(
                        "AWS_WEB_IDENTITY_TOKEN_FILE",
                        "/var/run/secrets/eks.amazonaws.com/serviceaccount/token",
                        "AWS_ROLE_ARN",
                        ROLE_ARN
                    )
                )
            )
        ) {
            assertTrue("fully configured provider must be active", provider.isActive());
            // The file watcher must be registered so credentials are refreshed when the symlinked
            // token rotates on disk; without this the STS provider would cache stale credentials
            // for the duration of their TTL after every rotation.
            verify(resourceWatcherService).add(any(), any());
        }
    }

    private static java.util.function.Function<String, String> env(Map<String, String> values) {
        Map<String, String> snapshot = new HashMap<>(values);
        return snapshot::get;
    }
}
