/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories.gcs;

import com.google.api.client.googleapis.GoogleUtils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponseInterceptor;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.LazyInitializable;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

public class GoogleCloudStorageService {

    private static final Logger logger = LogManager.getLogger(GoogleCloudStorageService.class);

    /**
     * Dictionary of client instances. Client instances are built lazily from the
     * latest settings.
     */
    private final AtomicReference<Map<String, LazyInitializable<StorageService, IOException>>> clientsCache =
        new AtomicReference<>(emptyMap());

    /**
     * Refreshes the client settings and clears the client cache. Subsequent calls to
     * {@code GoogleCloudStorageService#client} will return new clients constructed
     * using the parameter settings.
     *
     * @param clientsSettings the new settings used for building clients for subsequent requests
     */
    public synchronized void refreshAndClearCache(Map<String, GoogleCloudStorageClientSettings> clientsSettings) {
        // build the new lazy clients
        final Map<String, LazyInitializable<StorageService, IOException>> newClientsCache =
        clientsSettings.entrySet()
                .stream()
                .collect(Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        entry -> new LazyInitializable<>(() -> createClient(entry.getKey(), entry.getValue()))));

        // make the new clients available
        final Map<String, LazyInitializable<StorageService, IOException>> oldClientCache = clientsCache.getAndSet(newClientsCache);
        // release old clients
        oldClientCache.values().forEach(LazyInitializable::reset);
    }

    /**
     * Attempts to retrieve a client from the cache. If the client does not exist it
     * will be created from the latest settings and will populate the cache. The
     * returned instance should not be cached by the calling code. Instead, for each
     * use, the (possibly updated) instance should be requested by calling this
     * method.
     *
     * @param clientName name of the client settings used to create the client
     * @return a cached client storage instance that can be used to manage objects
     *         (blobs)
     */
    public Storage client(final String clientName) throws IOException {
        return getStorageService(clientName).storage;
    }

    /**
     * Attempts to retrieve the usage stats from the cache. If the client does not exist
     * it will be created from the latest settings and will populate the cache.
     *
     * @param clientName name of the client settings used to create the client
     * @return a cached client usage stats instance that keep tracks of the operations performed against GCS.
     */
    GoogleCloudStorageOperationsStats stats(final String clientName) throws IOException {
        return getStorageService(clientName).operationsStats;
    }

    private StorageService getStorageService(String clientName) throws IOException {
        final LazyInitializable<StorageService, IOException> lazyClient = clientsCache.get().get(clientName);
        if (lazyClient == null) {
            throw new IllegalArgumentException("Unknown client name [" + clientName + "]. Existing client configs: "
                + Strings.collectionToDelimitedString(clientsCache.get().keySet(), ","));
        }
        return lazyClient.getOrCompute();
    }

    /**
     * Creates a client that can be used to manage Google Cloud Storage objects. The client is thread-safe.
     *
     * @param clientName name of client settings to use, including secure settings
     * @param clientSettings name of client settings to use, including secure settings
     * @return a new client storage instance that can be used to manage objects
     *         (blobs)
     */
    private StorageService createClient(String clientName, GoogleCloudStorageClientSettings clientSettings) throws IOException {
        logger.debug(() -> new ParameterizedMessage("creating GCS client with client_name [{}], endpoint [{}]", clientName,
                clientSettings.getHost()));
        final HttpTransport httpTransport = SocketAccess.doPrivilegedIOException(() -> {
            final NetHttpTransport.Builder builder = new NetHttpTransport.Builder();
            // requires java.lang.RuntimePermission "setFactory"
            // Pin the TLS trust certificates.
            builder.trustCertificates(GoogleUtils.getCertificateTrustStore());
            return builder.build();
        });

        final GoogleCloudStorageOperationsStats operationsStats = new GoogleCloudStorageOperationsStats();
        final GoogleCloudStorageHttpStatsCollector httpStatsCollector = new GoogleCloudStorageHttpStatsCollector(operationsStats);

        final HttpTransportOptions httpTransportOptions = HttpTransportWithResponseInterceptorOptions.builder()
            .setConnectTimeout(toTimeout(clientSettings.getConnectTimeout()))
            .setReadTimeout(toTimeout(clientSettings.getReadTimeout()))
            .setHttpTransportFactory(() -> httpTransport)
            .setHttpResponseInterceptor(httpStatsCollector)
            .build();

        final StorageOptions storageOptions = createStorageOptions(clientSettings, httpTransportOptions);
        return new StorageService(storageOptions.getService(), operationsStats);
    }

    StorageOptions createStorageOptions(final GoogleCloudStorageClientSettings clientSettings,
                                        final HttpTransportOptions httpTransportOptions) {
        final StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder()
                .setTransportOptions(httpTransportOptions)
                .setHeaderProvider(() -> {
                    final MapBuilder<String, String> mapBuilder = MapBuilder.newMapBuilder();
                    if (Strings.hasLength(clientSettings.getApplicationName())) {
                        mapBuilder.put("user-agent", clientSettings.getApplicationName());
                    }
                    return mapBuilder.immutableMap();
                });
        if (Strings.hasLength(clientSettings.getHost())) {
            storageOptionsBuilder.setHost(clientSettings.getHost());
        }
        if (Strings.hasLength(clientSettings.getProjectId())) {
            storageOptionsBuilder.setProjectId(clientSettings.getProjectId());
        }
        if (clientSettings.getCredential() == null) {
            logger.warn("\"Application Default Credentials\" are not supported out of the box."
                    + " Additional file system permissions have to be granted to the plugin.");
        } else {
            ServiceAccountCredentials serviceAccountCredentials = clientSettings.getCredential();
            // override token server URI
            final URI tokenServerUri = clientSettings.getTokenUri();
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

    private static class StorageService {
        private final Storage storage;
        private final GoogleCloudStorageOperationsStats operationsStats;

        private StorageService(final Storage storage, final GoogleCloudStorageOperationsStats operationsStats) {
            this.storage = storage;
            this.operationsStats = operationsStats;
        }
    }

    /**
     * Custom HttpTransportOptions that allows injecting an {@link HttpResponseInterceptor}, this is not possible
     * with the public API provided by the SDK.
     */
    private static class HttpTransportWithResponseInterceptorOptions extends HttpTransportOptions {

        private final HttpResponseInterceptor httpResponseInterceptor;

        private HttpTransportWithResponseInterceptorOptions(final HttpTransportOptions.Builder builder,
                                                            final HttpResponseInterceptor httpResponseInterceptor) {
            super(builder);
            this.httpResponseInterceptor = httpResponseInterceptor;
        }

        private static Builder builder() {
            return new Builder();
        }

        private static class Builder {

            private HttpTransportFactory httpTransportFactory;
            private int connectTimeout = -1;
            private int readTimeout = -1;
            private HttpResponseInterceptor httpResponseInterceptor;

            private Builder() {}

            private HttpTransportWithResponseInterceptorOptions build() {
                HttpTransportOptions.Builder builder = HttpTransportOptions.newBuilder()
                    .setHttpTransportFactory(httpTransportFactory)
                    .setConnectTimeout(connectTimeout)
                    .setReadTimeout(readTimeout);

                return new HttpTransportWithResponseInterceptorOptions(builder, httpResponseInterceptor);
            }

            /**
             * Sets the HTTP transport factory.
             *
             * @return the builder
             */
            private Builder setHttpTransportFactory(HttpTransportFactory httpTransportFactory) {
                this.httpTransportFactory = httpTransportFactory;
                return this;
            }

            /**
             * Sets the timeout in milliseconds to establish a connection.
             *
             * @param connectTimeout connection timeout in milliseconds. 0 for an infinite timeout, a
             *     negative number for the default value (20000).
             * @return the builder
             */
            private Builder setConnectTimeout(int connectTimeout) {
                this.connectTimeout = connectTimeout;
                return this;
            }

            /**
             * Sets the timeout in milliseconds to read data from an established connection.
             *
             * @param readTimeout read timeout in milliseconds. 0 for an infinite timeout, a negative number
             *     for the default value (20000).
             * @return the builder
             */
            private Builder setReadTimeout(int readTimeout) {
                this.readTimeout = readTimeout;
                return this;
            }

            /**
             * Sets the {@link HttpResponseInterceptor} used to intercept http responses from the transport layer.
             * @param httpResponseInterceptor the {@link HttpResponseInterceptor}.
             * @return the builder
             */
            private Builder setHttpResponseInterceptor(HttpResponseInterceptor httpResponseInterceptor) {
                this.httpResponseInterceptor = httpResponseInterceptor;
                return this;
            }
        }

        @Override
        public HttpRequestInitializer getHttpRequestInitializer(ServiceOptions<?, ?> serviceOptions) {
            HttpRequestInitializer requestInitializer = super.getHttpRequestInitializer(serviceOptions);

            return (httpRequest) -> {
                if (requestInitializer != null)
                    requestInitializer.initialize(httpRequest);

                httpRequest.setResponseInterceptor(httpResponseInterceptor);
            };
        }
    }
}
