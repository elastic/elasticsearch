/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gcs;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityRegistry;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceConfiguration.AuthMode;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.After;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for GcsStorageProvider.
 * Tests scheme validation, path extraction, and supported schemes.
 */
public class GcsStorageProviderTests extends ESTestCase {

    private final Storage mockStorage = mock(Storage.class);

    @After
    public void resetWorkloadIdentityRegistry() throws Exception {
        WorkloadIdentityRegistry.reset();
    }

    public void testFederatedAuthFailsWhenWorkloadIdentityDisabled() {
        WorkloadIdentityRegistry.setIssuerClient(new WorkloadIdentityIssuerClient() {
            @Override
            public boolean isEnabled() {
                return false;
            }

            @Override
            public void issueToken(IssueTokenRequest request, ActionListener<IssueTokenResponse> listener) {
                throw new UnsupportedOperationException("not expected");
            }
        });
        GcsConfiguration config = federatedConfiguration();
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> new GcsStorageProvider(config));
        assertThat(e.getMessage(), containsString("workload-identity"));
    }

    public void testFederatedAuthBuildsWhenWorkloadIdentityEnabled() {
        WorkloadIdentityRegistry.setIssuerClient((request, listener) -> fail("token request is not expected during client construction"));
        assertNotNull(new GcsStorageProvider(federatedConfiguration()));
    }

    public void testFederatedAuthBuildsWithoutServiceAccountImpersonationUrl() {
        WorkloadIdentityRegistry.setIssuerClient((request, listener) -> fail("token request is not expected during client construction"));
        GcsConfiguration config = GcsConfiguration.fromFields(
            null,
            null,
            null,
            null,
            null,
            "jwt-audience",
            "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
            null
        );
        assertNotNull(new GcsStorageProvider(config));
    }

    public void testFederatedAuthBuildsWithDefaultJwtAudience() {
        WorkloadIdentityRegistry.setIssuerClient((request, listener) -> fail("token request is not expected during client construction"));
        GcsConfiguration config = GcsConfiguration.fromFields(
            null,
            null,
            null,
            null,
            null,
            null,
            "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
            null
        );
        assertNotNull(new GcsStorageProvider(config));
    }

    public void testFederatedAuthBuildsWithOverriddenJwtAudience() {
        WorkloadIdentityRegistry.setIssuerClient((request, listener) -> fail("token request is not expected during client construction"));
        GcsConfiguration config = federatedConfiguration();
        assertNotNull(new GcsStorageProvider(config));
    }

    private static GcsConfiguration federatedConfiguration() {
        return GcsConfiguration.fromFields(
            null,
            null,
            null,
            null,
            null,
            "jwt-audience",
            "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/sa@project.iam.gserviceaccount.com:generateAccessToken"
        );
    }

    public void testSupportedSchemes() {
        GcsStorageProvider provider = new GcsStorageProvider(mockStorage);
        assertEquals(List.of("gs"), provider.supportedSchemes());
    }

    public void testInvalidSchemeThrows() {
        GcsStorageProvider provider = new GcsStorageProvider(mockStorage);
        StoragePath s3Path = StoragePath.of("s3://my-bucket/path/to/file.parquet");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> provider.newObject(s3Path));
        assertTrue(e.getMessage().contains("GcsStorageProvider only supports gs:// scheme"));
    }

    public void testNewObjectWithValidGsPath() {
        GcsStorageProvider provider = new GcsStorageProvider(mockStorage);
        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        var obj = provider.newObject(path);
        assertNotNull(obj);
        assertEquals(path, obj.path());
    }

    public void testGsPathParsing() {
        StoragePath path = StoragePath.of("gs://my-bucket/data/sales.parquet");
        assertEquals("gs", path.scheme());
        assertEquals("my-bucket", path.host());
        assertEquals("/data/sales.parquet", path.path());
        assertEquals("sales.parquet", path.objectName());
    }

    public void testGsPathWithNestedDirectory() {
        StoragePath path = StoragePath.of("gs://my-bucket/warehouse/db/table/part-00000.parquet");
        assertEquals("gs", path.scheme());
        assertEquals("my-bucket", path.host());
        assertEquals("/warehouse/db/table/part-00000.parquet", path.path());
        assertEquals("part-00000.parquet", path.objectName());
    }

    public void testGsPathBucketOnly() {
        StoragePath path = StoragePath.of("gs://my-bucket/");
        assertEquals("gs", path.scheme());
        assertEquals("my-bucket", path.host());
        assertEquals("/", path.path());
    }

    public void testGsPathWithGlobPattern() {
        StoragePath path = StoragePath.of("gs://my-bucket/data/*.parquet");
        assertEquals("gs", path.scheme());
        assertTrue(path.isPattern());
        assertEquals("*.parquet", path.globPart());
    }

    public void testGsPathPatternPrefix() {
        StoragePath path = StoragePath.of("gs://my-bucket/data/2024/*.parquet");
        StoragePath prefix = path.patternPrefix();
        assertEquals("gs://my-bucket/data/2024/", prefix.toString());
    }

    public void testGsPathParentDirectory() {
        StoragePath path = StoragePath.of("gs://my-bucket/data/sales.parquet");
        StoragePath parent = path.parentDirectory();
        assertNotNull(parent);
        assertEquals("gs://my-bucket/data", parent.toString());
    }

    public void testGsPathAppendPath() {
        StoragePath base = StoragePath.of("gs://my-bucket/data");
        StoragePath appended = base.appendPath("sales.parquet");
        assertEquals("gs://my-bucket/data/sales.parquet", appended.toString());
    }

    public void testCredentialsFromAccessToken() throws Exception {
        GcsConfiguration config = GcsConfiguration.fromMap(Map.of("access_token", "ya29.token"));
        assertEquals(AuthMode.STATIC_CREDENTIALS, config.resolveAuthMode());
        GoogleCredentials creds = GcsStorageProvider.buildStaticCredentials(config);
        assertEquals("ya29.token", creds.getAccessToken().getTokenValue());
    }

    public void testCredentialsRequiresCredentialsRejectedAtCreate() {
        // auth=auto with no credentials, no federated settings resolves to nothing, so it is rejected at create time.
        ValidationException e = expectThrows(ValidationException.class, () -> GcsConfiguration.fromMap(Map.of("project_id", "my-project")));
        assertTrue(e.getMessage().contains("GCS data source requires credentials"));
    }

    public void testEmptyAccessTokenTreatedAsAbsent() {
        // An empty access token is treated as absent rather than building OAuth credentials with an empty token, so
        // auth=auto has nothing to resolve and is rejected at create time.
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> GcsConfiguration.fromMap(Map.of("access_token", "", "project_id", "my-project"))
        );
        assertTrue(e.getMessage().contains("GCS data source requires credentials"));
    }

    public void testWhitespaceServiceAccountTreatedAsAbsent() {
        // A whitespace-only service-account credentials blob is treated as absent (consistent with the
        // access_token path) rather than handed to ServiceAccountCredentials.fromStream as garbage JSON.
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> GcsConfiguration.fromMap(Map.of("credentials", "   ", "project_id", "my-project"))
        );
        assertTrue(e.getMessage().contains("GCS data source requires credentials"));
    }

    /**
     * auth=managed_identity resolves to MANAGED_IDENTITY and the switch arm builds {@link ComputeEngineCredentials}
     * from the production seam.
     */
    public void testManagedIdentityCredentialsReturnsComputeEngine() throws Exception {
        GcsConfiguration config = GcsConfiguration.fromMap(Map.of("auth", "managed_identity"));
        assertEquals(AuthMode.MANAGED_IDENTITY, config.resolveAuthMode());
        Credentials creds = new GcsStorageProvider(mockStorage).buildManagedIdentityCredentials();
        assertThat(creds, instanceOf(ComputeEngineCredentials.class));
    }

    /**
     * {@link GcsStorageProvider#buildManagedIdentityCredentials()} — the seam the MANAGED_IDENTITY switch arm calls —
     * is overridable, so tests can inject a credential backed by a mock HTTP transport instead of the GCE metadata
     * server. Verifies the override is honored; the arm→seam routing is exercised end-to-end by the ITs.
     */
    public void testManagedIdentityCredentialsSeamIsOverridable() throws Exception {
        GoogleCredentials injected = GoogleCredentials.create(new com.google.auth.oauth2.AccessToken("seam-token", null));
        GcsStorageProvider provider = new GcsStorageProvider(mockStorage) {
            @Override
            protected Credentials buildManagedIdentityCredentials() {
                return injected;
            }
        };
        assertSame(injected, provider.buildManagedIdentityCredentials());
    }
}
