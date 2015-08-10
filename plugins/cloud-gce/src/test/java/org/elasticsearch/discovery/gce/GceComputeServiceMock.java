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

package org.elasticsearch.discovery.gce;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.Json;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import org.elasticsearch.cloud.gce.GceComputeServiceImpl;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Callback;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.GeneralSecurityException;

/**
 *
 */
public class GceComputeServiceMock extends GceComputeServiceImpl {

    protected HttpTransport mockHttpTransport;

    public GceComputeServiceMock(Settings settings) {
        super(settings);
        this.mockHttpTransport = configureMock();
    }

    @Override
    protected HttpTransport getGceHttpTransport() throws GeneralSecurityException, IOException {
        return this.mockHttpTransport;
    }

    protected HttpTransport configureMock() {
        HttpTransport transport = new MockHttpTransport() {
            @Override
            public LowLevelHttpRequest buildRequest(String method, final String url) throws IOException {
                return new MockLowLevelHttpRequest() {
                    @Override
                    public LowLevelHttpResponse execute() throws IOException {
                        MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                        response.setStatusCode(200);
                        response.setContentType(Json.MEDIA_TYPE);
                        if (url.equals(TOKEN_SERVER_ENCODED_URL)) {
                            logger.info("--> Simulate GCE Auth response for [{}]", url);
                            response.setContent(readGoogleInternalJsonResponse(url));
                        } else {
                            logger.info("--> Simulate GCE API response for [{}]", url);
                            response.setContent(readGoogleApiJsonResponse(url));
                        }

                        return response;
                    }
                };
            }
        };

        return transport;
    }

    private String readGoogleInternalJsonResponse(String url) throws IOException {
        return readJsonResponse(url, "http://metadata.google.internal/");
    }

    private String readGoogleApiJsonResponse(String url) throws IOException {
        return readJsonResponse(url, "https://www.googleapis.com/");
    }

    private String readJsonResponse(String url, String urlRoot) throws IOException {
        // We extract from the url the mock file path we want to use
        String mockFileName = Strings.replace(url, urlRoot, "") + ".json";

        logger.debug("--> read mock file from [{}]", mockFileName);
        URL resource = GceComputeServiceMock.class.getResource(mockFileName);
        try (InputStream is = resource.openStream()) {
            final StringBuilder sb = new StringBuilder();
            Streams.readAllLines(is, new Callback<String>() {
                @Override
                public void handle(String s) {
                    sb.append(s).append("\n");
                }
            });
            String response = sb.toString();
            logger.trace("{}", response);
            return response;
        } catch (IOException e) {
            throw e;
        }
    }
}
