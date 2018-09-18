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
package org.elasticsearch.cloud.gce;

import com.google.api.client.googleapis.testing.compute.MockMetadataServerTransport;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.Json;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.core.Is.is;

public class GceInstancesServiceImplTests extends ESTestCase {
    public void testHeaderContainsMetadataFlavor() throws IOException {

        final AtomicBoolean addMetdataFlavor = new AtomicBoolean();
        final MockHttpTransport transport = new MockHttpTransport() {
            @Override
            public LowLevelHttpRequest buildRequest(String method, final String url) throws IOException {
                return new MockLowLevelHttpRequest() {
                    @Override
                    public LowLevelHttpResponse execute() throws IOException {
                        MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                        response.setStatusCode(200);
                        response.setContentType(Json.MEDIA_TYPE);
                        if (addMetdataFlavor.get()) {
                            response.addHeader("Metadata-Flavor", "Google");
                        }
                        return response;
                    }
                };
            }
        };
        final HttpRequestFactory requestFactory = transport.createRequestFactory();
        final GenericUrl url = new GenericUrl("https://localhost:8080/");
        final HttpRequest request = requestFactory.buildGetRequest(url);
        final HttpResponse response = request.execute();

        assertThat(GceInstancesServiceImpl.headerContainsMetadataFlavor(response), is(false));

        addMetdataFlavor.set(true);
        final HttpRequest request2 = requestFactory.buildGetRequest(url);
        final HttpResponse response2 = request2.execute();
        assertThat(GceInstancesServiceImpl.headerContainsMetadataFlavor(response2), is(true));
    }
}
