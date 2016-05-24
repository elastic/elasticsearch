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

package org.elasticsearch.options.detailederrors;

import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.rest.client.http.HttpDeleteWithEntity;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

/**
 * Tests that by default the error_trace parameter can be used to show stacktraces
 */
@ClusterScope(scope = Scope.TEST, supportsDedicatedMasters = false, numDataNodes = 1)
public class DetailedErrorsEnabledIT extends ESIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .build();
    }

    public void testThatErrorTraceWorksByDefault() throws Exception {
        // Make the HTTP request
        HttpResponse response = new HttpRequestBuilder(HttpClients.createDefault())
                .httpTransport(internalCluster().getDataNodeInstance(HttpServerTransport.class))
                .path("/")
                .addParam("error_trace", "true")
                .method(HttpDeleteWithEntity.METHOD_NAME)
                .execute();

        assertThat(response.getHeaders().get("Content-Type"), containsString("application/json"));
        assertThat(response.getBody(), containsString("\"stack_trace\":\"[Validation Failed: 1: index / indices is missing;]; nested: ActionRequestValidationException[Validation Failed: 1:"));

        // Make the HTTP request
        response = new HttpRequestBuilder(HttpClients.createDefault())
                .httpTransport(internalCluster().getDataNodeInstance(HttpServerTransport.class))
                .path("/")
                .method(HttpDeleteWithEntity.METHOD_NAME)
                .execute();

        assertThat(response.getHeaders().get("Content-Type"), containsString("application/json"));
        assertThat(response.getBody(), not(containsString("\"stack_trace\":\"[Validation Failed: 1: index / indices is missing;]; nested: ActionRequestValidationException[Validation Failed: 1:")));
    }
}
