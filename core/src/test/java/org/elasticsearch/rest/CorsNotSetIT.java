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

package org.elasticsearch.rest;

import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.ElasticsearchResponse;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.Collections;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
@ClusterScope(scope = ESIntegTestCase.Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1)
public class CorsNotSetIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(NetworkModule.HTTP_ENABLED.getKey(), true)
            .put(super.nodeSettings(nodeOrdinal)).build();
    }

    public void testCorsSettingDefaultBehaviourDoesNotReturnAnything() throws Exception {
        String corsValue = "http://localhost:9200";
        try (ElasticsearchResponse response = getRestClient().performRequest("GET", "/", Collections.emptyMap(), null,
                new BasicHeader("User-Agent", "Mozilla Bar"), new BasicHeader("Origin", corsValue))) {
            assertThat(response.getStatusLine().getStatusCode(), is(200));
            assertThat(response.getFirstHeader("Access-Control-Allow-Origin"), nullValue());
            assertThat(response.getFirstHeader("Access-Control-Allow-Credentials"), nullValue());
        }
    }

    public void testThatOmittingCorsHeaderDoesNotReturnAnything() throws Exception {
        try (ElasticsearchResponse response = getRestClient().performRequest("GET", "/", Collections.emptyMap(), null)) {
            assertThat(response.getStatusLine().getStatusCode(), is(200));
            assertThat(response.getFirstHeader("Access-Control-Allow-Origin"), nullValue());
            assertThat(response.getFirstHeader("Access-Control-Allow-Credentials"), nullValue());
        }
    }
}
