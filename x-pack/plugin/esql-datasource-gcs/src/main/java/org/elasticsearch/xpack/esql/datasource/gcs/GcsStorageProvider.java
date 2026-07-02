/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gcs;

import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdentityPoolCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;

import org.elasticsearch.common.Strings;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityRegistry;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;

/**
 * StorageProvider implementation for Google Cloud Storage.
 * <p>
 * Supports the {@code gs://} URI scheme. The {@code auth} mode selects the credential; {@code auto} (the default)
 * infers it from the fields present:
 * <ul>
 *   <li>{@code auth=static_credentials} — a service account JSON key or a short-lived OAuth2 access token</li>
 *   <li>{@code auth=federated_identity} — workload identity federation via {@code jwt_audience}, {@code sts_audience},
 *       and {@code service_account_impersonation_url}</li>
 *   <li>{@code auth=anonymous} — anonymous access to public buckets</li>
 *   <li>{@code auth=managed_identity} — the node's GCE/GKE metadata-server credentials
 *       ({@link ComputeEngineCredentials}); requires the {@code esql.datasource.managed_identity.enabled}
 *       cluster setting</li>
 * </ul>
 * File-based ADC sources ({@code GOOGLE_APPLICATION_CREDENTIALS}, the well-known gcloud credential
 * file) are excluded because file reads are blocked by entitlements. GKE Workload Identity
 * Federation is handled separately via {@code auth=federated_identity}.
 * <p>
 * {@link GcsStorageObject} provides optimized I/O via GCS {@link com.google.cloud.ReadChannel}:
 * <ul>
 *   <li>{@code readBytes} reads directly into {@link java.nio.ByteBuffer} without intermediate copies</li>
 *   <li>{@code readBytesAsync} uses executor-based async with efficient ReadChannel reads</li>
 * </ul>
 * The async path blocks a worker thread (not truly non-blocking like HTTP sendAsync or S3AsyncClient)
 * but avoids the byte[] allocation overhead of the default InputStream-based wrappers.
 */
public class GcsStorageProvider implements StorageProvider {
    private volatile Storage storage;
    private final GcsConfiguration config;

    @SuppressWarnings("this-escape")
    public GcsStorageProvider(GcsConfiguration config) {
        this.config = config;
        // With a configuration present, build the client eagerly so misconfigurations are caught early (every
        // validated config resolves to a mode). When there is no configuration (config is null), defer client
        // creation to first use so the plugin can load; the missing-config error then surfaces only when a gs://
        // query is actually executed.
        if (config != null) {
            this.storage = buildStorageClient(config);
        }
    }

    /**
     * Constructor for testing with a pre-built Storage client.
     * Allows tests to inject a client configured with custom endpoints (e.g., fixture token URI).
     */
    public GcsStorageProvider(Storage storage) {
        this.config = null;
        this.storage = storage;
    }

    /**
     * Returns the Storage client, building it lazily on first access if needed.
     * This allows the plugin to load successfully even when no GCS credentials are configured;
     * the error is deferred to when a gs:// query is actually executed.
     */
    private Storage storage() {
        if (storage == null) {
            synchronized (this) {
                if (storage == null) {
                    storage = buildStorageClient(config);
                }
            }
        }
        return storage;
    }

    private Storage buildStorageClient(GcsConfiguration config) {
        // Connection pooling: unlike S3 (Netty maxConcurrency) and Azure (reactor-netty ConnectionProvider), the
        // google-cloud-storage client's default NetHttpTransport is backed by HttpURLConnection, whose
        // http.maxConnections (default 5) bounds only the idle keep-alive cache — NOT the number of concurrent
        // connections, which HttpURLConnection opens on demand. So GCS has no SDK-side pool cap. It does not need
        // one: GCS reads are blocking, so they run on the dedicated esql_external_blocking_io thread pool (sized by
        // esql.external.max_connections), which already bounds read concurrency. Sizing an SDK pool would mean
        // swapping in ApacheHttpTransport + a PoolingHttpClientConnectionManager — a production dependency that
        // neither this plugin nor repository-gcs carries (apache-httpclient is test-scope only there).
        if (config == null) {
            // The datasource layer always builds a provider from a validated configuration.
            throw new IllegalArgumentException("GCS data source requires a configuration");
        }
        try {
            // Select the client credential from the resolved auth mode. Anonymous uses the unauthenticated
            // StorageOptions; every other mode sets a Credentials on the standard builder.
            StorageOptions.Builder builder = switch (config.resolveAuthMode()) {
                case ANONYMOUS -> StorageOptions.getUnauthenticatedInstance().toBuilder();
                case STATIC_CREDENTIALS -> StorageOptions.newBuilder().setCredentials(buildStaticCredentials(config));
                case FEDERATED_IDENTITY -> StorageOptions.newBuilder().setCredentials(buildIdentityPoolCredentials(config));
                case MANAGED_IDENTITY -> StorageOptions.newBuilder().setCredentials(buildManagedIdentityCredentials());
            };
            if (config.projectId() != null) {
                builder.setProjectId(config.projectId());
            }
            if (config.endpoint() != null) {
                builder.setHost(config.endpoint());
            }
            return builder.build().getService();
        } catch (IOException e) {
            throw new IllegalStateException(
                "Failed to build Google Cloud Storage client. "
                    + "If no explicit credentials are configured, ensure Application Default Credentials (ADC) are available "
                    + "(e.g., GOOGLE_APPLICATION_CREDENTIALS environment variable or GCE metadata server): "
                    + e.getMessage(),
                e
            );
        }
    }

    /**
     * Builds the static Google credential for {@code STATIC_CREDENTIALS}: a service-account JSON key
     * ({@link ServiceAccountCredentials}) when present, otherwise a short-lived {@code access_token}. Validation
     * guarantees one of the two static forms is set for this mode, so this always yields a credential.
     */
    static GoogleCredentials buildStaticCredentials(GcsConfiguration config) throws IOException {
        if (Strings.hasText(config.serviceAccountCredentials())) {
            ServiceAccountCredentials credentials = ServiceAccountCredentials.fromStream(
                new ByteArrayInputStream(config.serviceAccountCredentials().getBytes(StandardCharsets.UTF_8))
            );
            // Override the token server URI if provided (used for testing with mock GCS fixtures)
            if (config.tokenUri() != null) {
                credentials = credentials.toBuilder().setTokenServerUri(URI.create(config.tokenUri())).build();
            }
            return credentials;
        }
        return GoogleCredentials.create(new AccessToken(config.accessToken(), null));
    }

    /**
     * Builds the credential used when {@code auth=managed_identity} is configured. Default returns
     * {@link ComputeEngineCredentials#create()}, which contacts the GCE/GKE metadata server.
     * <p>
     * {@code GoogleCredentials.getApplicationDefault()} is deliberately not used: it reads
     * {@code GOOGLE_APPLICATION_CREDENTIALS} and the well-known gcloud credential file, both
     * blocked by the entitlement system. {@code ComputeEngineCredentials} contacts only the
     * metadata server over the network (covered by the {@code outbound_network} entitlement).
     * GKE Workload Identity Federation is handled separately via {@code auth=federated_identity}.
     * <p>
     * Tests may override this method (anonymous subclass) to inject a credential built with a
     * mock {@link com.google.api.client.http.HttpTransport} — the same pattern used by
     * {@code GoogleCloudStorageAdcTests} in {@code repository-gcs}.
     */
    protected Credentials buildManagedIdentityCredentials() {
        return ComputeEngineCredentials.create();
    }

    private static GoogleCredentials buildIdentityPoolCredentials(GcsConfiguration config) throws IOException {
        WorkloadIdentityIssuerClient issuerClient = WorkloadIdentityRegistry.getSharedIssuerClient();
        if (issuerClient.isEnabled() == false) {
            throw new IllegalStateException("GCS keyless authentication requires the workload-identity feature to be enabled on this node");
        }

        IdentityPoolCredentials.Builder credentialsBuilder = IdentityPoolCredentials.newBuilder()
            .setAudience(config.stsAudience())
            .setSubjectTokenType(ExternalAccountCredentials.SubjectTokenTypes.JWT)
            .setSubjectTokenSupplier(new GcsWorkloadIdentitySubjectTokenSupplier(issuerClient, config.jwtAudience()));

        // Optional: when absent, the federated identity maps directly to a principal without impersonation.
        if (config.serviceAccountImpersonationUrl() != null) {
            credentialsBuilder.setServiceAccountImpersonationUrl(config.serviceAccountImpersonationUrl());
        }

        if (config.tokenUri() != null) {
            credentialsBuilder.setTokenUrl(config.tokenUri());
        }

        return credentialsBuilder.build();
    }

    @Override
    public StorageObject newObject(StoragePath path) {
        validateGcsScheme(path);
        String bucket = path.host();
        String objectName = extractObjectName(path);
        return new GcsStorageObject(storage(), bucket, objectName, path);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length) {
        validateGcsScheme(path);
        String bucket = path.host();
        String objectName = extractObjectName(path);
        return new GcsStorageObject(storage(), bucket, objectName, path, length);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
        validateGcsScheme(path);
        String bucket = path.host();
        String objectName = extractObjectName(path);
        return new GcsStorageObject(storage(), bucket, objectName, path, length, lastModified);
    }

    @Override
    public StorageIterator listObjects(StoragePath prefix, boolean recursive) throws IOException {
        validateGcsScheme(prefix);
        String bucket = prefix.host();
        String objectPrefix = extractObjectName(prefix);

        if (objectPrefix.isEmpty() == false && objectPrefix.endsWith(StoragePath.PATH_SEPARATOR) == false) {
            objectPrefix += StoragePath.PATH_SEPARATOR;
        }

        return new GcsStorageIterator(storage(), bucket, objectPrefix, prefix, recursive);
    }

    @Override
    public boolean exists(StoragePath path) throws IOException {
        validateGcsScheme(path);
        String bucket = path.host();
        String objectName = extractObjectName(path);

        try {
            Blob blob = storage().get(BlobId.of(bucket, objectName));
            return blob != null;
        } catch (StorageException e) {
            if (e.getCode() == 404) {
                return false;
            }
            if (e.getCode() == 403) {
                return existsViaRead(bucket, objectName, path);
            }
            throw new IOException("Failed to check existence of " + path + credentialHint(), e);
        }
    }

    private boolean existsViaRead(String bucket, String objectName, StoragePath path) throws IOException {
        try (ReadChannel reader = storage().reader(BlobId.of(bucket, objectName))) {
            return true;
        } catch (StorageException e) {
            if (e.getCode() == 404) {
                return false;
            }
            throw new IOException("Failed to check existence of " + path + " (metadata denied, read also failed)" + credentialHint(), e);
        }
    }

    private String credentialHint() {
        if (config == null || config.resolveAuthModeOrNull() == null) {
            return ". If accessing a public bucket, set auth=anonymous. "
                + "Otherwise, provide credentials via credentials or access_token, "
                + "or configure keyless authentication with jwt_audience and sts_audience";
        }
        return "";
    }

    @Override
    public List<String> supportedSchemes() {
        return List.of("gs");
    }

    @Override
    public void close() throws IOException {
        if (storage != null) {
            try {
                storage.close();
            } catch (Exception e) {
                throw new IOException("Failed to close GCS storage client", e);
            }
        }
    }

    private static void validateGcsScheme(StoragePath path) {
        String scheme = path.scheme().toLowerCase(Locale.ROOT);
        if (scheme.equals("gs") == false) {
            throw new IllegalArgumentException("GcsStorageProvider only supports gs:// scheme, got: " + scheme);
        }
    }

    private static String extractObjectName(StoragePath path) {
        String name = path.path();
        if (name.startsWith(StoragePath.PATH_SEPARATOR)) {
            name = name.substring(1);
        }
        return name;
    }

    GcsConfiguration config() {
        return config;
    }

    @Override
    public String toString() {
        return "GcsStorageProvider{config=" + config + "}";
    }

    /**
     * Iterator for GCS object listing with pagination support.
     * Uses the Google Cloud Storage client's built-in lazy pagination via {@link com.google.api.gax.paging.Page#iterateAll()}.
     * Note: {@code iterateAll()} fetches pages lazily as the iterator is consumed, but for very large listings
     * (millions of objects), the GCS client may buffer page results in memory.
     */
    private static final class GcsStorageIterator implements StorageIterator {
        private final Storage storage;
        private final String bucket;
        private final String prefix;
        private final StoragePath baseDirectory;
        private final boolean recursive;

        private Iterator<Blob> currentIterator;
        private StorageEntry nextEntry;
        private boolean initialized;

        GcsStorageIterator(Storage storage, String bucket, String prefix, StoragePath baseDirectory, boolean recursive) {
            this.storage = storage;
            this.bucket = bucket;
            this.prefix = prefix;
            this.baseDirectory = baseDirectory;
            this.recursive = recursive;
            this.initialized = false;
        }

        @Override
        public boolean hasNext() {
            if (initialized == false) {
                fetchObjects();
                initialized = true;
            }
            if (nextEntry != null) {
                return true;
            }
            nextEntry = advance();
            return nextEntry != null;
        }

        @Override
        public StorageEntry next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }

            StorageEntry entry = nextEntry;
            nextEntry = null;
            return entry;
        }

        @Override
        public void close() throws IOException {
            // No resources to close
        }

        private StorageEntry advance() {
            while (currentIterator != null && currentIterator.hasNext()) {
                Blob blob = currentIterator.next();
                // Filter out directory pseudo-objects (names ending with "/") that GCS returns
                // when using currentDirectory() option for non-recursive listings
                if (blob.getName().endsWith(StoragePath.PATH_SEPARATOR)) {
                    continue;
                }
                String fullPath = baseDirectory.scheme() + StoragePath.SCHEME_SEPARATOR + bucket + StoragePath.PATH_SEPARATOR + blob
                    .getName();
                StoragePath objectPath = StoragePath.of(fullPath);

                Instant lastModified = blob.getUpdateTimeOffsetDateTime() != null ? blob.getUpdateTimeOffsetDateTime().toInstant() : null;

                return new StorageEntry(objectPath, blob.getSize(), lastModified);
            }
            return null;
        }

        private void fetchObjects() {
            try {
                Storage.BlobListOption[] options;
                if (recursive) {
                    options = new Storage.BlobListOption[] { Storage.BlobListOption.prefix(prefix) };
                } else {
                    options = new Storage.BlobListOption[] {
                        Storage.BlobListOption.prefix(prefix),
                        Storage.BlobListOption.currentDirectory() };
                }

                com.google.api.gax.paging.Page<Blob> page = storage.list(bucket, options);
                currentIterator = page.iterateAll().iterator();
            } catch (Exception e) {
                String msg = (e instanceof StorageException se && se.getCode() == 403)
                    ? "Access denied listing objects in bucket ["
                        + bucket
                        + "] with prefix ["
                        + prefix
                        + "]. "
                        + "Verify that the configured credentials have storage.objects.list permission, "
                        + "or use exact file paths instead of glob patterns."
                    : "Failed to list objects in bucket [" + bucket + "] with prefix [" + prefix + "]";
                throw new UncheckedIOException(new IOException(msg, e));
            }
        }
    }
}
