/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.StorageOptions;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.notNullValue;

/**
 * This test class runs in its own JVM (via the {@code gcsAdcTest} Gradle task) so that the
 * {@code DefaultCredentialsProvider} singleton starts with a clean state. The singleton caches
 * credentials and tracks whether the GCE metadata server has been checked; sharing a JVM with
 * other tests can prime that state and cause flaky failures.
 */
public class GoogleCloudStorageAdcTests extends ESTestCase {

    /**
     * Verifies that when no explicit credentials are configured and no credential files are found,
     * the GCS service correctly falls back to obtaining credentials from the GCP metadata server
     * via Application Default Credentials.
     */
    public void testApplicationDefaultCredentialsFallbackToComputeEngine() throws IOException {
        final AtomicBoolean metadataServerContacted = new AtomicBoolean(false);

        // Mock HTTP transport that simulates the GCP metadata server.
        // DefaultCredentialsProvider uses this transport to detect GCE and obtain tokens
        // when no file-based credentials are found (File.isFile returns false due to entitlements).
        final HttpTransportFactory mockMetadataTransportFactory = () -> new HttpTransport() {
            @Override
            protected LowLevelHttpRequest buildRequest(String method, String url) {
                metadataServerContacted.set(true);
                final byte[] responseBody = url.contains("token")
                    ? "{\"access_token\":\"test-token\",\"token_type\":\"Bearer\",\"expires_in\":3600}".getBytes(UTF_8)
                    : new byte[0];
                return new LowLevelHttpRequest() {
                    @Override
                    public void addHeader(String name, String value) {}

                    @Override
                    public LowLevelHttpResponse execute() {
                        return new LowLevelHttpResponse() {
                            @Override
                            public InputStream getContent() {
                                return new ByteArrayInputStream(responseBody);
                            }

                            @Override
                            public String getContentEncoding() {
                                return null;
                            }

                            @Override
                            public long getContentLength() {
                                return responseBody.length;
                            }

                            @Override
                            public String getContentType() {
                                return "application/json";
                            }

                            @Override
                            public String getStatusLine() {
                                return "HTTP/1.1 200 OK";
                            }

                            @Override
                            public int getStatusCode() {
                                return 200;
                            }

                            @Override
                            public String getReasonPhrase() {
                                return "OK";
                            }

                            @Override
                            public int getHeaderCount() {
                                return 1;
                            }

                            @Override
                            public String getHeaderName(int index) {
                                return "Metadata-Flavor";
                            }

                            @Override
                            public String getHeaderValue(int index) {
                                return "Google";
                            }
                        };
                    }
                };
            }
        };

        // Override createStorageOptions to use our mock transport for Application Default
        // Credentials, simulating what happens on a GCP VM when no credentials_file is set.
        final GoogleCloudStorageService service = new GoogleCloudStorageService() {
            @Override
            StorageOptions createStorageOptions(
                GoogleCloudStorageClientSettings gcsClientSettings,
                HttpTransportOptions httpTransportOptions
            ) {
                assertNull("Test expects no explicit credential to be configured", gcsClientSettings.getCredential());
                try {
                    return StorageOptions.newBuilder()
                        .setCredentials(GoogleCredentials.getApplicationDefault(mockMetadataTransportFactory))
                        .build();
                } catch (IOException e) {
                    throw new RuntimeException("Failed to obtain Application Default Credentials from mock metadata server", e);
                }
            }
        };

        final Settings settings = Settings.builder()
            .put(GoogleCloudStorageClientSettings.ENDPOINT_SETTING.getConcreteSettingForNamespace("default").getKey(), "http://unused")
            .build();
        service.refreshAndClearCache(GoogleCloudStorageClientSettings.load(settings));

        final var storage = service.client("default", "repo", new GoogleCloudStorageOperationsStats("bucket"));
        assertThat(storage.getOptions().getCredentials(), notNullValue());
        assertTrue(
            "GCP metadata server should have been contacted to obtain Application Default Credentials",
            metadataServerContacted.get()
        );
    }
}
