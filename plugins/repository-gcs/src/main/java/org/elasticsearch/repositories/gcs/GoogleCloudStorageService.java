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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.HttpUnsuccessfulResponseHandler;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.services.storage.Storage;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;

import java.io.IOException;
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
     * @param clientName name of client settings to use from secure settings
     * @return a Client instance that can be used to manage Storage objects
     */
    public Storage createClient(final String clientName) throws Exception {
        final GoogleCloudStorageClientSettings clientSettings = clientsSettings.get(clientName);
        if (clientSettings == null) {
            throw new IllegalArgumentException("Unknown client name [" + clientName + "]. Existing client configs: " +
                Strings.collectionToDelimitedString(clientsSettings.keySet(), ","));
        }

        HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
        HttpRequestInitializer requestInitializer =  createRequestInitializer(clientSettings);

        Storage.Builder storage = new Storage.Builder(transport, JacksonFactory.getDefaultInstance(), requestInitializer);
        if (Strings.hasLength(clientSettings.getApplicationName())) {
            storage.setApplicationName(clientSettings.getApplicationName());
        }
        if (Strings.hasLength(clientSettings.getEndpoint())) {
            storage.setRootUrl(clientSettings.getEndpoint());
        }
        return storage.build();
    }

    static HttpRequestInitializer createRequestInitializer(final GoogleCloudStorageClientSettings settings) throws IOException {
        GoogleCredential credential = settings.getCredential();
        if (credential == null) {
            credential = GoogleCredential.getApplicationDefault();
        }
        return new DefaultHttpRequestInitializer(credential, toTimeout(settings.getConnectTimeout()), toTimeout(settings.getReadTimeout()));
    }

    /** Converts timeout values from the settings to a timeout value for the Google Cloud SDK **/
    static Integer toTimeout(final TimeValue timeout) {
        // Null or zero in settings means the default timeout
        if (timeout == null || TimeValue.ZERO.equals(timeout)) {
            return null;
        }
        // -1 means infinite timeout
        if (TimeValue.MINUS_ONE.equals(timeout)) {
            // 0 is the infinite timeout expected by Google Cloud SDK
            return 0;
        }
        return Math.toIntExact(timeout.getMillis());
    }

    /**
     * HTTP request initializer that set timeouts and backoff handler while deferring authentication to GoogleCredential.
     * See https://cloud.google.com/storage/transfer/create-client#retry
     */
    static class DefaultHttpRequestInitializer implements HttpRequestInitializer {

        private final Integer connectTimeout;
        private final Integer readTimeout;
        private final GoogleCredential credential;

        DefaultHttpRequestInitializer(GoogleCredential credential, Integer connectTimeoutMillis, Integer readTimeoutMillis) {
            this.credential = credential;
            this.connectTimeout = connectTimeoutMillis;
            this.readTimeout = readTimeoutMillis;
        }

        @Override
        public void initialize(HttpRequest request) {
            if (connectTimeout != null) {
                request.setConnectTimeout(connectTimeout);
            }
            if (readTimeout != null) {
                request.setReadTimeout(readTimeout);
            }

            request.setIOExceptionHandler(new HttpBackOffIOExceptionHandler(newBackOff()));
            request.setInterceptor(credential);

            final HttpUnsuccessfulResponseHandler handler = new HttpBackOffUnsuccessfulResponseHandler(newBackOff());
            request.setUnsuccessfulResponseHandler((req, resp, supportsRetry) -> {
                    // Let the credential handle the response. If it failed, we rely on our backoff handler
                    return credential.handleResponse(req, resp, supportsRetry) || handler.handleResponse(req, resp, supportsRetry);
                }
            );
        }

        private ExponentialBackOff newBackOff() {
            return new ExponentialBackOff.Builder()
                    .setInitialIntervalMillis(100)
                    .setMaxIntervalMillis(6000)
                    .setMaxElapsedTimeMillis(900000)
                    .setMultiplier(1.5)
                    .setRandomizationFactor(0.5)
                    .build();
        }
    }

}
