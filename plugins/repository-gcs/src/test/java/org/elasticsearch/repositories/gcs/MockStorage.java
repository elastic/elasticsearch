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

import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.storage.Storage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * {@link MockStorage} is a utility class that provides {@link Storage} clients that works
 * against an embedded {@link GoogleCloudStorageTestServer}.
 */
class MockStorage extends com.google.api.client.testing.http.MockHttpTransport {

    /**
     * Embedded test server that emulates a Google Cloud Storage service
     **/
    private final GoogleCloudStorageTestServer server = new GoogleCloudStorageTestServer();

    private MockStorage() {
    }

    @Override
    public LowLevelHttpRequest buildRequest(String method, String url) throws IOException {
        return new MockLowLevelHttpRequest() {
            @Override
            public LowLevelHttpResponse execute() throws IOException {
                return convert(server.handle(method, url, getHeaders(), getContentAsBytes()));
            }

            /** Returns the LowLevelHttpRequest body as an array of bytes **/
            byte[] getContentAsBytes() throws IOException {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                if (getStreamingContent() != null) {
                    getStreamingContent().writeTo(out);
                }
                return out.toByteArray();
            }
        };
    }

    private static MockLowLevelHttpResponse convert(final GoogleCloudStorageTestServer.Response response) {
        final MockLowLevelHttpResponse lowLevelHttpResponse = new MockLowLevelHttpResponse();
        for (Map.Entry<String, String> header : response.headers.entrySet()) {
            lowLevelHttpResponse.addHeader(header.getKey(), header.getValue());
        }
        lowLevelHttpResponse.setContentType(response.contentType);
        lowLevelHttpResponse.setStatusCode(response.status.getStatus());
        lowLevelHttpResponse.setReasonPhrase(response.status.toString());
        if (response.body != null) {
            lowLevelHttpResponse.setContent(response.body);
            lowLevelHttpResponse.setContentLength(response.body.length);
        }
        return lowLevelHttpResponse;
    }

    /**
     * Instanciates a mocked Storage client for tests.
     */
    public static Storage newStorageClient(final String bucket, final String applicationName) {
        MockStorage mockStorage = new MockStorage();
        mockStorage.server.createBucket(bucket);

        return new Storage.Builder(mockStorage, JacksonFactory.getDefaultInstance(), null)
            .setApplicationName(applicationName)
            .build();
    }
}
