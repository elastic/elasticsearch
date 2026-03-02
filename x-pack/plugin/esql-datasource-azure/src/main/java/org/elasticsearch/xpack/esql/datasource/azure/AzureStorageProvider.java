/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.StorageSharedKeyCredential;

import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;

/**
 * StorageProvider implementation for Azure Blob Storage.
 * <p>
 * Supports the {@code wasbs://} and {@code wasb://} URI schemes.
 * URI format: {@code wasbs://account.blob.core.windows.net/container/path/to/blob}
 * <ul>
 *   <li>host: account.blob.core.windows.net (account name is first segment)</li>
 *   <li>path: /container/path/to/blob (first segment = container, remainder = blob name)</li>
 * </ul>
 * <p>
 * Authentication can be provided via connection string, account+key, SAS token,
 * or DefaultAzureCredential when no explicit credentials are configured.
 */
public final class AzureStorageProvider implements StorageProvider {
    private volatile BlobServiceClient blobServiceClient;
    private final AzureConfiguration config;

    public AzureStorageProvider(AzureConfiguration config) {
        this.config = config;
        if (config != null && config.hasCredentials()) {
            this.blobServiceClient = buildBlobServiceClient(config);
        }
    }

    /**
     * Constructor for testing with a pre-built BlobServiceClient.
     */
    public AzureStorageProvider(BlobServiceClient blobServiceClient) {
        this.config = null;
        this.blobServiceClient = blobServiceClient;
    }

    private BlobServiceClient client(String accountFromPath) {
        if (blobServiceClient != null) {
            return blobServiceClient;
        }
        synchronized (this) {
            if (blobServiceClient == null) {
                blobServiceClient = buildBlobServiceClient(config, accountFromPath);
            }
        }
        return blobServiceClient;
    }

    private static BlobServiceClient buildBlobServiceClient(AzureConfiguration config) {
        return buildBlobServiceClient(config, null);
    }

    private static BlobServiceClient buildBlobServiceClient(AzureConfiguration config, String accountFromPath) {
        BlobServiceClientBuilder builder = new BlobServiceClientBuilder();

        if (config != null && config.hasCredentials()) {
            if (config.connectionString() != null && config.connectionString().isEmpty() == false) {
                builder.connectionString(config.connectionString());
                if (config.endpoint() != null && config.endpoint().isEmpty() == false) {
                    builder.endpoint(config.endpoint());
                }
            } else if (config.account() != null && config.key() != null) {
                StorageSharedKeyCredential credential = new StorageSharedKeyCredential(config.account(), config.key());
                String endpoint = config.endpoint();
                if (endpoint == null || endpoint.isEmpty()) {
                    endpoint = "https://" + config.account() + ".blob.core.windows.net";
                }
                builder.endpoint(endpoint).credential(credential);
            } else if (config.sasToken() != null && config.sasToken().isEmpty() == false && config.account() != null) {
                String endpoint = config.endpoint();
                if (endpoint == null || endpoint.isEmpty()) {
                    endpoint = "https://" + config.account() + ".blob.core.windows.net";
                }
                builder.endpoint(endpoint).sasToken(config.sasToken());
            } else {
                throw new IllegalStateException("Azure credentials require connection_string, (account + key), or (account + sas_token)");
            }
        } else {
            String account = accountFromPath;
            if (account == null && config != null && config.account() != null) {
                account = config.account();
            }
            if (account == null) {
                throw new IllegalStateException(
                    "Azure DefaultAzureCredential requires account from path (wasbs://account.blob.core.windows.net/...) or config"
                );
            }
            String endpoint = config != null && config.endpoint() != null && config.endpoint().isEmpty() == false
                ? config.endpoint()
                : "https://" + account + ".blob.core.windows.net";
            builder.endpoint(endpoint).credential(new DefaultAzureCredentialBuilder().build());
        }

        return builder.buildClient();
    }

    @Override
    public StorageObject newObject(StoragePath path) {
        validateAzureScheme(path);
        ParsedPath parsed = parsePath(path);
        String account = extractAccountFromHost(parsed.host);
        BlobClient blobClient = client(account).getBlobContainerClient(parsed.container).getBlobClient(parsed.blobName);
        return new AzureStorageObject(blobClient, parsed.container, parsed.blobName, path);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length) {
        validateAzureScheme(path);
        ParsedPath parsed = parsePath(path);
        String account = extractAccountFromHost(parsed.host);
        BlobClient blobClient = client(account).getBlobContainerClient(parsed.container).getBlobClient(parsed.blobName);
        return new AzureStorageObject(blobClient, parsed.container, parsed.blobName, path, length);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
        validateAzureScheme(path);
        ParsedPath parsed = parsePath(path);
        String account = extractAccountFromHost(parsed.host);
        BlobClient blobClient = client(account).getBlobContainerClient(parsed.container).getBlobClient(parsed.blobName);
        return new AzureStorageObject(blobClient, parsed.container, parsed.blobName, path, length, lastModified);
    }

    @Override
    public StorageIterator listObjects(StoragePath prefix, boolean recursive) throws IOException {
        validateAzureScheme(prefix);
        ParsedPath parsed = parsePathForListing(prefix);
        String account = extractAccountFromHost(parsed.host);
        BlobContainerClient containerClient = client(account).getBlobContainerClient(parsed.container);
        ListBlobsOptions options = new ListBlobsOptions().setPrefix(parsed.blobName);
        Iterable<BlobItem> blobItems = recursive
            ? containerClient.listBlobs(options, null)
            : containerClient.listBlobsByHierarchy("/", options, null);
        return new AzureStorageIterator(blobItems, prefix, parsed.container);
    }

    @Override
    public boolean exists(StoragePath path) throws IOException {
        validateAzureScheme(path);
        ParsedPath parsed = parsePath(path);
        String account = extractAccountFromHost(parsed.host);
        BlobClient blobClient = client(account).getBlobContainerClient(parsed.container).getBlobClient(parsed.blobName);
        return blobClient.exists();
    }

    @Override
    public List<String> supportedSchemes() {
        return List.of("wasbs", "wasb");
    }

    @Override
    public void close() throws IOException {
        BlobServiceClient client = blobServiceClient;
        blobServiceClient = null;
        if (client != null) {
            try {
                closeHttpClient(client.getHttpPipeline().getHttpClient());
            } catch (Exception e) {
                throw new IOException("Failed to close Azure BlobServiceClient", e);
            }
        }
    }

    /**
     * Close the underlying HttpClient if it implements AutoCloseable.
     * BlobServiceClient does not implement Closeable; the default NettyAsyncHttpClient
     * does not either, so this is typically a no-op. We attempt close when possible so
     * that custom HttpClient implementations or future SDK versions that support close
     * will be properly cleaned up. For full resource cleanup (like repository-azure),
     * the client would need to be built with a custom ConnectionProvider.
     */
    private static void closeHttpClient(com.azure.core.http.HttpClient httpClient) throws Exception {
        if (httpClient instanceof AutoCloseable closeable) {
            closeable.close();
        }
    }

    private static void validateAzureScheme(StoragePath path) {
        String scheme = path.scheme().toLowerCase(Locale.ROOT);
        if (scheme.equals("wasbs") == false && scheme.equals("wasb") == false) {
            throw new IllegalArgumentException("AzureStorageProvider only supports wasbs:// and wasb:// schemes, got: " + scheme);
        }
    }

    private static String extractAccountFromHost(String host) {
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Invalid Azure host: " + host);
        }
        int dot = host.indexOf('.');
        return dot > 0 ? host.substring(0, dot) : host;
    }

    /**
     * Parse path for object access: wasbs://account.blob.core.windows.net/container/path/to/blob
     * host = account.blob.core.windows.net -> account is first segment
     * path = /container/path/to/blob -> container = first segment, blob name = path/to/blob
     */
    private static ParsedPath parsePath(StoragePath path) {
        String host = path.host();
        String pathStr = path.path();
        if (pathStr.startsWith(StoragePath.PATH_SEPARATOR)) {
            pathStr = pathStr.substring(1);
        }
        if (pathStr.isEmpty()) {
            throw new IllegalArgumentException("Invalid Azure path: container and blob name required: " + path);
        }
        int firstSlash = pathStr.indexOf(StoragePath.PATH_SEPARATOR);
        String container;
        String blobName;
        if (firstSlash < 0) {
            container = pathStr;
            blobName = "";
        } else {
            container = pathStr.substring(0, firstSlash);
            blobName = pathStr.substring(firstSlash + 1);
        }
        if (container.isEmpty()) {
            throw new IllegalArgumentException("Invalid Azure path: container is required: " + path);
        }
        return new ParsedPath(host, container, blobName);
    }

    /**
     * Parse path for listing: prefix may end with / or a glob pattern.
     */
    private static ParsedPath parsePathForListing(StoragePath path) {
        ParsedPath parsed = parsePath(path);
        String prefix = parsed.blobName;
        if (prefix.isEmpty() == false && prefix.endsWith(StoragePath.PATH_SEPARATOR) == false) {
            prefix += StoragePath.PATH_SEPARATOR;
        }
        return new ParsedPath(parsed.host, parsed.container, prefix);
    }

    private record ParsedPath(String host, String container, String blobName) {}

    private static final class AzureStorageIterator implements StorageIterator {
        private final Iterable<BlobItem> blobItems;
        private final StoragePath basePath;
        private final String container;
        private final String scheme;

        private Iterator<BlobItem> iterator;
        private BlobItem current;

        AzureStorageIterator(Iterable<BlobItem> blobItems, StoragePath basePath, String container) {
            this.blobItems = blobItems;
            this.basePath = basePath;
            this.container = container;
            this.scheme = basePath.scheme();
        }

        @Override
        public boolean hasNext() {
            if (iterator == null) {
                iterator = blobItems.iterator();
            }
            if (current != null) {
                return true;
            }
            while (iterator.hasNext()) {
                BlobItem item = iterator.next();
                if (item.isPrefix()) {
                    continue;
                }
                String name = item.getName();
                if (name != null && name.endsWith(StoragePath.PATH_SEPARATOR) == false) {
                    current = item;
                    return true;
                }
            }
            return false;
        }

        @Override
        public StorageEntry next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            BlobItem item = current;
            current = null;
            String fullPath = scheme + StoragePath.SCHEME_SEPARATOR + basePath.host() + StoragePath.PATH_SEPARATOR + container
                + StoragePath.PATH_SEPARATOR + item.getName();
            StoragePath objectPath = StoragePath.of(fullPath);
            Instant lastModified = item.getProperties() != null && item.getProperties().getLastModified() != null
                ? item.getProperties().getLastModified().toInstant()
                : null;
            long size = item.getProperties() != null && item.getProperties().getContentLength() != null
                ? item.getProperties().getContentLength()
                : 0L;
            return new StorageEntry(objectPath, size, lastModified);
        }

        @Override
        public void close() throws IOException {
            // No resources to close
        }
    }
}
