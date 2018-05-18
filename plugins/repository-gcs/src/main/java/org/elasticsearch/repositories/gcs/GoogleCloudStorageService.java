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
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.DefaultConnectionFactory;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;

public class GoogleCloudStorageService extends AbstractComponent {

    /** Clients settings identified by client name. */
    private final Map<String, GoogleCloudStorageClientSettings> clientsSettings;

    public GoogleCloudStorageService(final Environment environment, final Map<String, GoogleCloudStorageClientSettings> clientsSettings) {
        super(environment.settings());
        this.clientsSettings = clientsSettings;
    }

    /**
     * Creates a client that can be used to manage Google Cloud Storage objects.
     *
     * @param clientName name of client settings to use, including secure settings
     * @return a Client instance that can be used to manage Storage objects
     */
    public Storage createClient(final String clientName) throws Exception {
        final GoogleCloudStorageClientSettings clientSettings = clientsSettings.get(clientName);
        if (clientSettings == null) {
            throw new IllegalArgumentException("Unknown client name [" + clientName + "]. Existing client configs: "
                    + Strings.collectionToDelimitedString(clientsSettings.keySet(), ","));
        }
        final HttpTransport httpTransport = createHttpTransport(clientSettings.getHost());
        final HttpTransportOptions httpTransportOptions = HttpTransportOptions.newBuilder()
                .setConnectTimeout(toTimeout(clientSettings.getConnectTimeout()))
                .setReadTimeout(toTimeout(clientSettings.getReadTimeout()))
                .setHttpTransportFactory(() -> httpTransport)
                .build();
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
        return storageOptionsBuilder.build().getService();
    }

    /**
     * Pins the TLS trust certificates and, more importantly, overrides connection
     * URLs in the case of a custom endpoint setting because some connections don't
     * fully honor this setting (bugs in the SDK). The default connection factory
     * opens a new connection for each request. This is required for the storage
     * instance to be thread-safe.
     **/
    private static HttpTransport createHttpTransport(final String endpoint) throws Exception {
        final NetHttpTransport.Builder builder = new NetHttpTransport.Builder();
        // requires java.lang.RuntimePermission "setFactory"
        builder.trustCertificates(GoogleUtils.getCertificateTrustStore());
        if (Strings.hasLength(endpoint)) {
            final URL endpointUrl = URI.create(endpoint).toURL();
            builder.setConnectionFactory(new DefaultConnectionFactory() {
                @Override
                public HttpURLConnection openConnection(final URL originalUrl) throws IOException {
                    // test if the URL is built correctly, ie following the `host` setting
                    if (originalUrl.getHost().equals(endpointUrl.getHost()) && originalUrl.getPort() == endpointUrl.getPort()
                            && originalUrl.getProtocol().equals(endpointUrl.getProtocol())) {
                        return super.openConnection(originalUrl);
                    }
                    // override connection URLs because some don't follow the config. See
                    // https://github.com/GoogleCloudPlatform/google-cloud-java/issues/3254 and
                    // https://github.com/GoogleCloudPlatform/google-cloud-java/issues/3255
                    URI originalUri;
                    try {
                        originalUri = originalUrl.toURI();
                    } catch (final URISyntaxException e) {
                        throw new RuntimeException(e);
                    }
                    String overridePath = "/";
                    if (originalUri.getRawPath() != null) {
                        overridePath = originalUri.getRawPath();
                    }
                    if (originalUri.getRawQuery() != null) {
                        overridePath += "?" + originalUri.getRawQuery();
                    }
                    return super.openConnection(
                            new URL(endpointUrl.getProtocol(), endpointUrl.getHost(), endpointUrl.getPort(), overridePath));
                }
            });
        }
        return builder.build();
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

}
