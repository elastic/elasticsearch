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
package org.elasticsearch.rest.action.main;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RestMainActionIT extends ESIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(NetworkModule.HTTP_ENABLED.getKey(), true)
            .build();
    }

    public void testHeadRequest() throws IOException {
        try (Response response = getRestClient().performRequest("HEAD", "/", Collections.emptyMap(), null)) {
            assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
            assertNull(response.getEntity());
        }
    }

    public void testGetRequest() throws IOException {
        try (Response response = getRestClient().performRequest("GET", "/", Collections.emptyMap(), null)) {
            assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
            assertNotNull(response.getEntity());
            assertThat(EntityUtils.toString(response.getEntity()), containsString("cluster_name"));
        }
    }
}
