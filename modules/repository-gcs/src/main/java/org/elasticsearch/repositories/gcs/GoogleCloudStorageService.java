/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.gcs;

import com.google.api.client.googleapis.GoogleUtils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.util.SecurityUtils;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.StorageRetryStrategy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URI;
import java.net.URL;
import java.security.KeyStore;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.core.Strings.format;

public class GoogleCloudStorageService {

    private static final Logger logger = LogManager.getLogger(GoogleCloudStorageService.class);

    private volatile Map<String, GoogleCloudStorageClientSettings> clientSettings = emptyMap();

    /**
     * Dictionary of client instances. Client instances are built lazily from the
     * latest settings. Each repository has its own client instance identified by
     * the repository name.
     */
    private volatile Map<String, Storage> clientCache = emptyMap();

    /**
     * Refreshes the client settings and clears the client cache. Subsequent calls to
     * {@code GoogleCloudStorageService#client} will return new clients constructed
     * using the parameter settings.
     *
     * @param clientsSettings the new settings used for building clients for subsequent requests
     */
    public synchronized void refreshAndClearCache(Map<String, GoogleCloudStorageClientSettings> clientsSettings) {
        this.clientCache = emptyMap();
        this.clientSettings = Maps.ofEntries(clientsSettings.entrySet());
    }

    /**
     * Attempts to retrieve a client from the cache. If the client does not exist it
     * will be created from the latest settings and will populate the cache. The
     * returned instance should not be cached by the calling code. Instead, for each
     * use, the (possibly updated) instance should be requested by calling this
     * method.
     *
     * @param clientName name of the client settings used to create the client
     * @param repositoryName name of the repository that would use the client
     * @param stats the stats collector used to gather information about the underlying SKD API calls.
     * @return a cached client storage instance that can be used to manage objects
     *         (blobs)
     */
    public Storage client(final String clientName, final String repositoryName, final GoogleCloudStorageOperationsStats stats)
        throws IOException {
        {
            final Storage storage = clientCache.get(repositoryName);
            if (storage != null) {
                return storage;
            }
        }
        synchronized (this) {
            final Storage existing = clientCache.get(repositoryName);

            if (existing != null) {
                return existing;
            }

            final GoogleCloudStorageClientSettings settings = clientSettings.get(clientName);

            if (settings == null) {
                throw new IllegalArgumentException(
                    "Unknown client name ["
                        + clientName
                        + "]. Existing client configs: "
                        + Strings.collectionToDelimitedString(clientSettings.keySet(), ",")
                );
            }

            logger.debug(() -> format("creating GCS client with client_name [%s], endpoint [%s]", clientName, settings.getHost()));
            final Storage storage = createClient(settings, stats);
            clientCache = Maps.copyMapWithAddedEntry(clientCache, repositoryName, storage);
            return storage;
        }
    }

    synchronized void closeRepositoryClient(String repositoryName) {
        clientCache = Maps.copyMapWithRemovedEntry(clientCache, repositoryName);
    }

    /**
     * Creates a client that can be used to manage Google Cloud Storage objects. The client is thread-safe.
     *
     * @param gcsClientSettings client settings to use, including secure settings
     * @param stats the stats collector to use by the underlying SDK
     * @return a new client storage instance that can be used to manage objects
     *         (blobs)
     */
    private Storage createClient(GoogleCloudStorageClientSettings gcsClientSettings, GoogleCloudStorageOperationsStats stats)
        throws IOException {
        final HttpTransport httpTransport = SocketAccess.doPrivilegedIOException(() -> {
            final NetHttpTransport.Builder builder = new NetHttpTransport.Builder();
            // requires java.lang.RuntimePermission "setFactory"
            // Pin the TLS trust certificates.
            // We manually load the key store from jks instead of using GoogleUtils.getCertificateTrustStore() because that uses a .p12
            // store format not compatible with FIPS mode.
            final KeyStore certTrustStore = SecurityUtils.getJavaKeyStore();
            try (InputStream keyStoreStream = GoogleUtils.class.getResourceAsStream("google.jks")) {
                SecurityUtils.loadKeyStore(certTrustStore, keyStoreStream, "notasecret");
            }
            builder.trustCertificates(certTrustStore);
            Proxy proxy = gcsClientSettings.getProxy();
            if (proxy != null) {
                builder.setProxy(proxy);
                notifyProxyIsSet(proxy);
            }
            return builder.build();
        });

        final GoogleCloudStorageHttpStatsCollector httpStatsCollector = new GoogleCloudStorageHttpStatsCollector(stats);

        final HttpTransportOptions httpTransportOptions = new HttpTransportOptions(
            HttpTransportOptions.newBuilder()
                .setConnectTimeout(toTimeout(gcsClientSettings.getConnectTimeout()))
                .setReadTimeout(toTimeout(gcsClientSettings.getReadTimeout()))
                .setHttpTransportFactory(() -> httpTransport)
        ) {

            @Override
            public HttpRequestInitializer getHttpRequestInitializer(ServiceOptions<?, ?> serviceOptions) {
                HttpRequestInitializer requestInitializer = super.getHttpRequestInitializer(serviceOptions);

                return (httpRequest) -> {
                    if (requestInitializer != null) requestInitializer.initialize(httpRequest);

                    httpRequest.setResponseInterceptor(httpStatsCollector);
                };
            }
        };

        final StorageOptions storageOptions = createStorageOptions(gcsClientSettings, httpTransportOptions);
        return storageOptions.getService();
    }

    StorageOptions createStorageOptions(
        final GoogleCloudStorageClientSettings gcsClientSettings,
        final HttpTransportOptions httpTransportOptions
    ) {
        final StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder()
            .setStorageRetryStrategy(StorageRetryStrategy.getLegacyStorageRetryStrategy())
            .setTransportOptions(httpTransportOptions)
            .setHeaderProvider(() -> {
                return Strings.hasLength(gcsClientSettings.getApplicationName())
                    ? Map.of("user-agent", gcsClientSettings.getApplicationName())
                    : Map.of();
            });
        if (Strings.hasLength(gcsClientSettings.getHost())) {
            storageOptionsBuilder.setHost(gcsClientSettings.getHost());
        }
        if (Strings.hasLength(gcsClientSettings.getProjectId())) {
            storageOptionsBuilder.setProjectId(gcsClientSettings.getProjectId());
        } else {
            String defaultProjectId = null;
            try {
                defaultProjectId = SocketAccess.doPrivilegedIOException(ServiceOptions::getDefaultProjectId);
                if (defaultProjectId != null) {
                    storageOptionsBuilder.setProjectId(defaultProjectId);
                }
            } catch (Exception e) {
                logger.warn("failed to load default project id", e);
            }
            if (defaultProjectId == null) {
                try {
                    // fallback to manually load project ID here as the above ServiceOptions method has the metadata endpoint hardcoded,
                    // which makes it impossible to test
                    SocketAccess.doPrivilegedVoidIOException(() -> {
                        final String projectId = getDefaultProjectId(gcsClientSettings.getProxy());
                        if (projectId != null) {
                            storageOptionsBuilder.setProjectId(projectId);
                        }
                    });
                } catch (Exception e) {
                    logger.warn("failed to load default project id fallback", e);
                }
            }
        }
        if (gcsClientSettings.getCredential() == null) {
            try {
                storageOptionsBuilder.setCredentials(SocketAccess.doPrivilegedIOException(GoogleCredentials::getApplicationDefault));
            } catch (Exception e) {
                logger.warn("failed to load Application Default Credentials", e);
            }
        } else {
            ServiceAccountCredentials serviceAccountCredentials = gcsClientSettings.getCredential();
            // override token server URI
            final URI tokenServerUri = gcsClientSettings.getTokenUri();
            if (Strings.hasLength(tokenServerUri.toString())) {
                // Rebuild the service account credentials in order to use a custom Token url.
                // This is mostly used for testing purpose.
                serviceAccountCredentials = serviceAccountCredentials.toBuilder().setTokenServerUri(tokenServerUri).build();
            }
            storageOptionsBuilder.setCredentials(serviceAccountCredentials);
        }
        return storageOptionsBuilder.build();
    }

    /**
     * This method imitates what MetadataConfig.getProjectId() does, but does not have the endpoint hardcoded.
     */
    @SuppressForbidden(reason = "ok to open connection here")
    static String getDefaultProjectId(@Nullable Proxy proxy) throws IOException {
        String metaHost = System.getenv("GCE_METADATA_HOST");
        if (metaHost == null) {
            metaHost = "metadata.google.internal";
        }
        URL url = new URL("http://" + metaHost + "/computeMetadata/v1/project/project-id");
        HttpURLConnection connection = (HttpURLConnection) (proxy != null ? url.openConnection(proxy) : url.openConnection());
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        connection.setRequestProperty("Metadata-Flavor", "Google");
        try (InputStream input = connection.getInputStream()) {
            if (connection.getResponseCode() == 200) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, UTF_8))) {
                    return reader.readLine();
                }
            }
        }
        return null;
    }

    /**
     * Converts timeout values from the settings to a timeout value for the Google
     * Cloud SDK
     **/
    static Integer toTimeout(final TimeValue timeout) {
        // Null or zero in settings means the default timeout
        if (timeout == null || TimeValue.ZERO.equals(timeout)) {
            // negative value means using the default value
            return -1;
        }
        // -1 means infinite timeout
        if (TimeValue.MINUS_ONE.equals(timeout)) {
            // 0 is the infinite timeout expected by Google Cloud SDK
            return 0;
        }
        return Math.toIntExact(timeout.getMillis());
    }

    // used for unit testing
    void notifyProxyIsSet(Proxy proxy) {}
}
