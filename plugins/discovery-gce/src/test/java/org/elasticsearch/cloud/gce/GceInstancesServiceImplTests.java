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

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.Json;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;

public class GceInstancesServiceImplTests extends ESTestCase {

    public void testHeaderContainsMetadataFlavor() throws Exception {
        final AtomicBoolean addMetdataFlavor = new AtomicBoolean();
        final MockHttpTransport transport = new MockHttpTransport() {
            @Override
            public LowLevelHttpRequest buildRequest(String method, final String url) {
                return new MockLowLevelHttpRequest() {
                    @Override
                    public LowLevelHttpResponse execute() {
                        MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                        response.setStatusCode(200);
                        response.setContentType(Json.MEDIA_TYPE);
                        response.setContent("value");
                        if (addMetdataFlavor.get()) {
                            response.addHeader("Metadata-Flavor", "Google");
                        }
                        return response;
                    }
                };
            }
        };

        final GceInstancesServiceImpl service = new GceInstancesServiceImpl(Settings.EMPTY) {
            @Override
            protected synchronized HttpTransport getGceHttpTransport() {
                return transport;
            }
        };

        final String serviceURL = "/computeMetadata/v1/project/project-id";
        assertThat(service.getAppEngineValueFromMetadataServer(serviceURL), is(nullValue()));

        addMetdataFlavor.set(true);
        assertThat(service.getAppEngineValueFromMetadataServer(serviceURL), is("value"));
    }
}
