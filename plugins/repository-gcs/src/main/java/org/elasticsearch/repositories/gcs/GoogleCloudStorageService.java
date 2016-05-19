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
import com.google.api.client.http.HttpIOExceptionHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpUnsuccessfulResponseHandler;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

public interface GoogleCloudStorageService {

    /**
     * Creates a client that can be used to manage Google Cloud Storage objects.
     *
     * @param serviceAccount path to service account file
     * @param application    name of the application
     * @param connectTimeout connection timeout for HTTP requests
     * @param readTimeout    read timeout for HTTP requests
     * @return a Client instance that can be used to manage objects
     */
    Storage createClient(String serviceAccount, String application, TimeValue connectTimeout, TimeValue readTimeout) throws Exception;

    /**
     * Default implementation
     */
    class InternalGoogleCloudStorageService extends AbstractComponent implements GoogleCloudStorageService {

        private static final String DEFAULT = "_default_";

        private final Environment environment;

        @Inject
        public InternalGoogleCloudStorageService(Settings settings, Environment environment) {
            super(settings);
            this.environment = environment;
        }

        @Override
        public Storage createClient(String serviceAccount, String application, TimeValue connectTimeout, TimeValue readTimeout)
                throws Exception {
            try {
                GoogleCredential credentials = (DEFAULT.equalsIgnoreCase(serviceAccount)) ? loadDefault() : loadCredentials(serviceAccount);
                NetHttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();

                Storage.Builder storage = new Storage.Builder(httpTransport, JacksonFactory.getDefaultInstance(),
                        new DefaultHttpRequestInitializer(credentials, connectTimeout, readTimeout));
                storage.setApplicationName(application);

                logger.debug("initializing client with service account [{}/{}]",
                        credentials.getServiceAccountId(), credentials.getServiceAccountUser());
                return storage.build();
            } catch (IOException e) {
                throw new ElasticsearchException("Error when loading Google Cloud Storage credentials file", e);
            }
        }

        /**
         * HTTP request initializer that loads credentials from the service account file
         * and manages authentication for HTTP requests
         */
        private GoogleCredential loadCredentials(String serviceAccount) throws IOException {
            if (serviceAccount == null) {
                throw new ElasticsearchException("Cannot load Google Cloud Storage service account file from a null path");
            }

            Path account = environment.configFile().resolve(serviceAccount);
            if (Files.exists(account) == false) {
                throw new ElasticsearchException("Unable to find service account file [" + serviceAccount
                        + "] defined for repository");
            }

            try (InputStream is = Files.newInputStream(account)) {
                GoogleCredential credential = GoogleCredential.fromStream(is);
                if (credential.createScopedRequired()) {
                    credential = credential.createScoped(Collections.singleton(StorageScopes.DEVSTORAGE_FULL_CONTROL));
                }
                return credential;
            }
        }

        /**
         * HTTP request initializer that loads default credentials when running on Compute Engine
         */
        private GoogleCredential loadDefault() throws IOException {
            return GoogleCredential.getApplicationDefault();
        }

        /**
         * HTTP request initializer that set timeouts and backoff handler while deferring authentication to GoogleCredential.
         * See https://cloud.google.com/storage/transfer/create-client#retry
         */
        class DefaultHttpRequestInitializer implements HttpRequestInitializer {

            private final TimeValue connectTimeout;
            private final TimeValue readTimeout;
            private final GoogleCredential credential;
            private final HttpUnsuccessfulResponseHandler handler;
            private final HttpIOExceptionHandler ioHandler;

            DefaultHttpRequestInitializer(GoogleCredential credential, TimeValue connectTimeout, TimeValue readTimeout) {
                this.credential = credential;
                this.connectTimeout = connectTimeout;
                this.readTimeout = readTimeout;
                this.handler = new HttpBackOffUnsuccessfulResponseHandler(newBackOff());
                this.ioHandler = new HttpBackOffIOExceptionHandler(newBackOff());
            }

            @Override
            public void initialize(HttpRequest request) throws IOException {
                if (connectTimeout != null) {
                    request.setConnectTimeout((int) connectTimeout.millis());
                }
                if (readTimeout != null) {
                    request.setReadTimeout((int) readTimeout.millis());
                }

                request.setIOExceptionHandler(ioHandler);
                request.setInterceptor(credential);

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
}
