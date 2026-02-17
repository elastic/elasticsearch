/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gcs;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;

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
 * Supports the {@code gs://} URI scheme. Authentication can be provided via:
 * <ul>
 *   <li>Explicit service account JSON credentials</li>
 *   <li>Application Default Credentials (ADC) — environment variable, metadata server, etc.</li>
 * </ul>
 * <p>
 * Note: this implementation does not currently provide native async support
 * ({@code readBytesAsync} / {@code supportsNativeAsync}). The GCS Java client does support
 * async operations, and adding native async would improve Parquet parallel column chunk reads.
 * TODO: implement native async via the GCS async client for improved Parquet read performance.
 */
public final class GcsStorageProvider implements StorageProvider {
    private volatile Storage storage;
    private final GcsConfiguration config;

    public GcsStorageProvider(GcsConfiguration config) {
        this.config = config;
        // When explicit credentials are provided, build the client eagerly so misconfigurations
        // are caught early. When using ADC (config is null), defer client creation to first use
        // so the plugin can load even when no GCS credentials are configured.
        if (config != null && config.hasCredentials()) {
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

    private static Storage buildStorageClient(GcsConfiguration config) {
        try {
            StorageOptions.Builder builder = StorageOptions.newBuilder();

            if (config != null && config.hasCredentials()) {
                ServiceAccountCredentials credentials = ServiceAccountCredentials.fromStream(
                    new ByteArrayInputStream(config.serviceAccountCredentials().getBytes(StandardCharsets.UTF_8))
                );
                // Override the token server URI if provided (used for testing with mock GCS fixtures)
                if (config.tokenUri() != null) {
                    credentials = credentials.toBuilder().setTokenServerUri(URI.create(config.tokenUri())).build();
                }
                builder.setCredentials(credentials);
            } else {
                builder.setCredentials(GoogleCredentials.getApplicationDefault());
            }

            if (config != null && config.projectId() != null) {
                builder.setProjectId(config.projectId());
            }

            if (config != null && config.endpoint() != null) {
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
            throw new IOException("Failed to check existence of " + path, e);
        }
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
            } catch (StorageException e) {
                throw new UncheckedIOException(new IOException("Failed to list objects in bucket " + bucket + " with prefix " + prefix, e));
            } catch (Exception e) {
                throw new UncheckedIOException(new IOException("Failed to list objects in bucket " + bucket + " with prefix " + prefix, e));
            }
        }
    }
}
