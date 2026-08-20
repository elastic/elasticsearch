/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.CredentialUnavailableException;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityRegistry;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;

/**
 * Unit tests for AzureStorageProvider.
 * Tests scheme validation, path extraction, and supported schemes.
 * Note: Tests that require BlobServiceClient are excluded as it is a final class
 * and cannot be mocked with standard Mockito.
 */
public class AzureStorageProviderTests extends ESTestCase {

    public void testSupportedSchemes() {
        AzureStorageProvider provider = new AzureStorageProvider(null, null, null);
        assertEquals(List.of("wasbs", "wasb"), provider.supportedSchemes());
    }

    public void testInvalidSchemeThrows() {
        AzureStorageProvider provider = new AzureStorageProvider(null, null, null);
        StoragePath s3Path = StoragePath.of("s3://my-bucket/path/to/file.parquet");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> provider.newObject(s3Path));
        assertTrue(e.getMessage().contains("AzureStorageProvider only supports wasbs:// and wasb:// schemes"));
    }

    public void testManagedIdentityDefersBuildWhenAccountOnlyFromPath() {
        // auth=managed_identity with the account supplied only in the query path (no account/endpoint in config) must
        // not fail at construction: the endpoint is derived per-query from the wasbs://<account>... path, which is
        // unavailable at construction, so the client build is deferred to first use. Regression guard for the eager-
        // build guard, which must exclude managed_identity for Azure.
        AzureConfiguration config = AzureConfiguration.fromFields(null, null, null, null, null, "managed_identity");
        AzureStorageProvider provider = new AzureStorageProvider(config, null, null);
        assertNotNull(provider);
    }

    public void testWasbsPathParsing() {
        StoragePath path = StoragePath.of("wasbs://myaccount.blob.core.windows.net/container/data/sales.parquet");
        assertEquals("wasbs", path.scheme());
        assertEquals("myaccount.blob.core.windows.net", path.host());
        assertEquals("/container/data/sales.parquet", path.path());
        assertEquals("sales.parquet", path.objectName());
    }

    public void testWasbPathParsing() {
        StoragePath path = StoragePath.of("wasb://myaccount.blob.core.windows.net/container/data/file.parquet");
        assertEquals("wasb", path.scheme());
        assertEquals("myaccount.blob.core.windows.net", path.host());
        assertEquals("/container/data/file.parquet", path.path());
    }

    public void testPathWithNestedDirectory() {
        StoragePath path = StoragePath.of("wasbs://account.blob.core.windows.net/warehouse/db/table/part-00000.parquet");
        assertEquals("wasbs", path.scheme());
        assertEquals("account.blob.core.windows.net", path.host());
        assertEquals("/warehouse/db/table/part-00000.parquet", path.path());
        assertEquals("part-00000.parquet", path.objectName());
    }

    public void testPathWithGlobPattern() {
        StoragePath path = StoragePath.of("wasbs://account.blob.core.windows.net/container/data/*.parquet");
        assertEquals("wasbs", path.scheme());
        assertTrue(path.isPattern());
        assertEquals("*.parquet", path.globPart());
    }

    public void testPathPatternPrefix() {
        StoragePath path = StoragePath.of("wasbs://account.blob.core.windows.net/container/data/2024/*.parquet");
        StoragePath prefix = path.patternPrefix();
        assertEquals("wasbs://account.blob.core.windows.net/container/data/2024/", prefix.toString());
    }

    // -- Hadoop/Spark canonical WASB(S) form: wasbs://<container>@<account>.host/<blob> --

    public void testParsePathHadoopForm() {
        StoragePath path = StoragePath.of("wasbs://nyctlc@azureopendatastorage.blob.core.windows.net/yellow/puYear=2019/file.parquet");
        AzureStorageProvider.ParsedPath parsed = AzureStorageProvider.parsePath(path);
        assertEquals("azureopendatastorage.blob.core.windows.net", parsed.host());
        assertEquals("nyctlc", parsed.container());
        assertEquals("yellow/puYear=2019/file.parquet", parsed.blobName());
    }

    public void testParsePathHadoopFormRootListing() {
        // Hadoop form with empty path is a valid root-of-container reference for listing.
        StoragePath path = StoragePath.of("wasbs://my-container@account.blob.core.windows.net/");
        AzureStorageProvider.ParsedPath parsed = AzureStorageProvider.parsePath(path);
        assertEquals("my-container", parsed.container());
        assertEquals("", parsed.blobName());
    }

    public void testParsePathPathStyleUnchanged() {
        StoragePath path = StoragePath.of("wasbs://account.blob.core.windows.net/my-container/data/file.parquet");
        AzureStorageProvider.ParsedPath parsed = AzureStorageProvider.parsePath(path);
        assertEquals("account.blob.core.windows.net", parsed.host());
        assertEquals("my-container", parsed.container());
        assertEquals("data/file.parquet", parsed.blobName());
    }

    public void testParsePathPathStyleEmptyRejected() {
        StoragePath path = StoragePath.of("wasbs://account.blob.core.windows.net/");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> AzureStorageProvider.parsePath(path));
        assertTrue(e.getMessage().contains("container and blob name required"));
    }

    // -- AzureStorageIterator URI-form preservation: emitted entries match input form --

    public void testIteratorEmitsPathStyleWhenInputIsPathStyle() throws Exception {
        StoragePath base = StoragePath.of("wasbs://account.blob.core.windows.net/c/data/");
        List<StoragePath> emitted = drain(base, "c", "data/file1.parquet", "data/sub/file2.parquet");
        assertEquals(
            List.of(
                StoragePath.of("wasbs://account.blob.core.windows.net/c/data/file1.parquet"),
                StoragePath.of("wasbs://account.blob.core.windows.net/c/data/sub/file2.parquet")
            ),
            emitted
        );
        for (StoragePath p : emitted) {
            assertNull("path-style entries must not carry userInfo", p.userInfo());
        }
    }

    public void testIteratorEmitsHadoopFormWhenInputIsHadoopForm() throws Exception {
        StoragePath base = StoragePath.of("wasbs://c@account.blob.core.windows.net/data/");
        List<StoragePath> emitted = drain(base, "c", "data/file1.parquet", "data/sub/file2.parquet");
        assertEquals(
            List.of(
                StoragePath.of("wasbs://c@account.blob.core.windows.net/data/file1.parquet"),
                StoragePath.of("wasbs://c@account.blob.core.windows.net/data/sub/file2.parquet")
            ),
            emitted
        );
        for (StoragePath p : emitted) {
            assertEquals("Hadoop-form entries must carry container in userInfo", "c", p.userInfo());
        }
    }

    public void testIteratorSkipsPrefixesAndDirectoryEntries() throws Exception {
        BlobItem prefix = new BlobItem().setName("data/sub/").setIsPrefix(Boolean.TRUE);
        BlobItem dir = new BlobItem().setName("data/dir/").setProperties(properties(0L));
        BlobItem file = new BlobItem().setName("data/file.parquet").setProperties(properties(123L));

        StoragePath base = StoragePath.of("wasbs://c@account.blob.core.windows.net/data/");
        AzureStorageProvider.AzureStorageIterator it = new AzureStorageProvider.AzureStorageIterator(List.of(prefix, dir, file), base, "c");

        List<StorageEntry> entries = drainEntries(it);
        assertEquals(1, entries.size());
        assertEquals(StoragePath.of("wasbs://c@account.blob.core.windows.net/data/file.parquet"), entries.get(0).path());
        assertEquals(123L, entries.get(0).length());
    }

    private static List<StoragePath> drain(StoragePath base, String container, String... blobNames) throws Exception {
        List<BlobItem> items = new ArrayList<>(blobNames.length);
        for (String name : blobNames) {
            items.add(new BlobItem().setName(name).setProperties(properties(0L)));
        }
        AzureStorageProvider.AzureStorageIterator it = new AzureStorageProvider.AzureStorageIterator(items, base, container);
        List<StoragePath> paths = new ArrayList<>();
        for (StorageEntry entry : drainEntries(it)) {
            paths.add(entry.path());
        }
        return paths;
    }

    private static List<StorageEntry> drainEntries(StorageIterator it) throws Exception {
        List<StorageEntry> entries = new ArrayList<>();
        try (it) {
            while (it.hasNext()) {
                entries.add(it.next());
            }
        }
        return entries;
    }

    private static BlobItemProperties properties(long contentLength) {
        return new BlobItemProperties().setContentLength(contentLength);
    }

    public void testIssueAssertionAsyncCompletesWithToken() throws Exception {
        WorkloadIdentityIssuerClient issuer = (request, listener) -> listener.onResponse(
            new WorkloadIdentityIssuerClient.IssueTokenResponse("header.payload.signature", Instant.now().plusSeconds(3600))
        );
        CompletableFuture<String> assertion = AzureStorageProvider.issueAssertionAsync(issuer, "api://AzureADTokenExchange");
        assertEquals("header.payload.signature", assertion.get());
    }

    public void testIssueAssertionAsyncCompletesExceptionallyOnFailure() {
        IOException failure = new IOException("issuer unavailable");
        WorkloadIdentityIssuerClient issuer = (request, listener) -> listener.onFailure(failure);
        CompletableFuture<String> assertion = AzureStorageProvider.issueAssertionAsync(issuer, "api://AzureADTokenExchange");
        ExecutionException e = expectThrows(ExecutionException.class, assertion::get);
        assertSame(failure, e.getCause());
    }

    public void testBuildClientAssertionCredentialThrowsWhenWorkloadIdentityDisabled() {
        WorkloadIdentityRegistry.setIssuerClient(new RecordingIssuerClient(false, false));
        try {
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> AzureStorageProvider.buildClientAssertionCredential(federatedConfig(), EsExecutors.DIRECT_EXECUTOR_SERVICE)
            );
            assertThat(e.getMessage(), containsString("workload-identity feature to be enabled"));
        } finally {
            WorkloadIdentityRegistry.reset();
        }
    }

    public void testBuildClientAssertionCredentialRequiresExecutor() {
        WorkloadIdentityRegistry.setIssuerClient(new RecordingIssuerClient(true, false));
        try {
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> AzureStorageProvider.buildClientAssertionCredential(federatedConfig(), null)
            );
            assertThat(e.getMessage(), containsString("non-null executor"));
        } finally {
            WorkloadIdentityRegistry.reset();
        }
    }

    public void testBuildClientAssertionCredentialRequestsConfiguredAudience() {
        RecordingIssuerClient issuer = new RecordingIssuerClient(true, false);
        WorkloadIdentityRegistry.setIssuerClient(issuer);
        try {
            TokenCredential credential = AzureStorageProvider.buildClientAssertionCredential(
                federatedConfig(),
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );
            // The issuer fails the assertion, so resolution short-circuits before any Azure AD token exchange.
            TokenRequestContext request = new TokenRequestContext().addScopes("https://storage.azure.com/.default");
            expectThrows(CredentialUnavailableException.class, () -> credential.getToken(request).block());
            assertEquals("overridden-audience", issuer.requestedAudience);
        } finally {
            WorkloadIdentityRegistry.reset();
        }
    }

    public void testBuildClientAssertionCredentialRequestsDefaultAudienceWhenOmitted() {
        RecordingIssuerClient issuer = new RecordingIssuerClient(true, false);
        WorkloadIdentityRegistry.setIssuerClient(issuer);
        try {
            TokenCredential credential = AzureStorageProvider.buildClientAssertionCredential(
                AzureConfiguration.fromMap(Map.of("tenant_id", "test-tenant-id", "client_id", "test-client-id")),
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );
            // The issuer fails the assertion, so resolution short-circuits before any Azure AD token exchange.
            TokenRequestContext request = new TokenRequestContext().addScopes("https://storage.azure.com/.default");
            expectThrows(CredentialUnavailableException.class, () -> credential.getToken(request).block());
            assertEquals("api://AzureADTokenExchange", issuer.requestedAudience);
        } finally {
            WorkloadIdentityRegistry.reset();
        }
    }

    private static AzureConfiguration federatedConfig() {
        return AzureConfiguration.fromMap(
            Map.of("tenant_id", "test-tenant-id", "client_id", "test-client-id", "jwt_audience", "overridden-audience")
        );
    }

    /** Stub issuer that records the requested audience and either fails the listener or returns a token. */
    private static final class RecordingIssuerClient implements WorkloadIdentityIssuerClient {
        private final boolean enabled;
        private final boolean succeed;
        private volatile String requestedAudience;

        RecordingIssuerClient(boolean enabled, boolean succeed) {
            this.enabled = enabled;
            this.succeed = succeed;
        }

        @Override
        public void issueToken(IssueTokenRequest request, ActionListener<IssueTokenResponse> listener) {
            requestedAudience = request.audience();
            if (succeed) {
                listener.onResponse(new IssueTokenResponse("header.payload.signature", Instant.now().plusSeconds(3600)));
            } else {
                listener.onFailure(new IOException("issuer unavailable"));
            }
        }

        @Override
        public boolean isEnabled() {
            return enabled;
        }
    }

    // -- AKS Workload Identity activation matrix -------------------------------------------------

    public void testAksWorkloadIdentityInactiveWithoutEnvironment() {
        AzureStorageProvider provider = new AzureStorageProvider((AzureConfiguration) null, null, null);
        assertNull(
            "without an Environment the workload-identity chain must fall back to ManagedIdentity-only",
            provider.maybeBuildAksWorkloadIdentityCredential(name -> "any")
        );
    }

    public void testAksWorkloadIdentityInactiveWhenEnvTripleMissing() throws IOException {
        Environment env = newTestEnvironment();
        Files.createDirectories(env.configDir().resolve("esql-datasource-azure"));
        Files.writeString(env.configDir().resolve(AzureStorageProvider.AKS_FEDERATED_TOKEN_FILE_LOCATION), "tok");
        AzureStorageProvider provider = new AzureStorageProvider((AzureConfiguration) null, env, null);
        assertNull(
            "missing AZURE_FEDERATED_TOKEN_FILE must keep the workload-identity chain at ManagedIdentity-only",
            provider.maybeBuildAksWorkloadIdentityCredential(env(Map.of("AZURE_CLIENT_ID", "cid", "AZURE_TENANT_ID", "tid")))
        );
    }

    public void testAksWorkloadIdentityThrowsWhenSymlinkAbsent() throws IOException {
        Environment env = newTestEnvironment();
        Files.createDirectories(env.configDir().resolve("esql-datasource-azure"));
        AzureStorageProvider provider = new AzureStorageProvider((AzureConfiguration) null, env, null);
        // The AKS env triple means the operator intended workload identity; a missing entitled
        // symlink is a misconfiguration that must fail loudly rather than silently degrade to a
        // ManagedIdentityCredential that re-enters the entitlement-blocked K8s token path.
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> provider.maybeBuildAksWorkloadIdentityCredential(
                env(
                    Map.of(
                        "AZURE_FEDERATED_TOKEN_FILE",
                        "/var/run/secrets/azure/tokens/azure-identity-token",
                        "AZURE_CLIENT_ID",
                        "cid",
                        "AZURE_TENANT_ID",
                        "tid"
                    )
                )
            )
        );
        assertThat(e.getMessage(), containsString("is missing"));
    }

    public void testAksWorkloadIdentityActiveWhenFullyConfigured() throws IOException {
        Environment env = newTestEnvironment();
        Path tokenFile = env.configDir().resolve(AzureStorageProvider.AKS_FEDERATED_TOKEN_FILE_LOCATION);
        Files.createDirectories(tokenFile.getParent());
        Files.writeString(tokenFile, "fake-federated-token");
        AzureStorageProvider provider = new AzureStorageProvider((AzureConfiguration) null, env, null);
        TokenCredential credential = provider.maybeBuildAksWorkloadIdentityCredential(
            env(
                Map.of(
                    "AZURE_FEDERATED_TOKEN_FILE",
                    "/var/run/secrets/azure/tokens/azure-identity-token",
                    "AZURE_CLIENT_ID",
                    "cid",
                    "AZURE_TENANT_ID",
                    "tid"
                )
            )
        );
        assertNotNull("fully configured AKS env triple must produce a TokenCredential", credential);
        assertThat(credential.getClass().getName(), org.hamcrest.Matchers.containsString("WorkloadIdentityCredential"));
    }

    public void testAksWorkloadIdentityThrowsWhenSymlinkUnreadable() throws IOException {
        // POSIX permissions are required for this test; skip on filesystems that don't honor them
        // (e.g. native Windows). The plugin only ships on Linux/macOS in production.
        assumeTrue("requires POSIX file permissions", PathUtils.getDefaultFileSystem().supportedFileAttributeViews().contains("posix"));
        Environment env = newTestEnvironment();
        Path tokenFile = env.configDir().resolve(AzureStorageProvider.AKS_FEDERATED_TOKEN_FILE_LOCATION);
        Files.createDirectories(tokenFile.getParent());
        Files.writeString(tokenFile, "fake-federated-token");
        try {
            Files.setPosixFilePermissions(tokenFile, EnumSet.noneOf(PosixFilePermission.class));
            // chmod 000 can still leave the file readable for the owning user on some filesystems
            // (e.g. macOS APFS when the test runs as root); skip if the permission did not stick.
            assumeTrue("filesystem honors chmod 000", Files.isReadable(tokenFile) == false);

            AzureStorageProvider provider = new AzureStorageProvider((AzureConfiguration) null, env, null);
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> provider.maybeBuildAksWorkloadIdentityCredential(
                    env(
                        Map.of(
                            "AZURE_FEDERATED_TOKEN_FILE",
                            "/var/run/secrets/azure/tokens/azure-identity-token",
                            "AZURE_CLIENT_ID",
                            "cid",
                            "AZURE_TENANT_ID",
                            "tid"
                        )
                    )
                )
            );
            assertThat(e.getMessage(), containsString("not readable"));
        } finally {
            Files.setPosixFilePermissions(tokenFile, PosixFilePermissions.fromString("rw-------"));
        }
    }

    private static Environment newTestEnvironment() {
        return TestEnvironment.newEnvironment(
            Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build()
        );
    }

    private static java.util.function.Function<String, String> env(Map<String, String> values) {
        Map<String, String> snapshot = new HashMap<>(values);
        return snapshot::get;
    }
}
