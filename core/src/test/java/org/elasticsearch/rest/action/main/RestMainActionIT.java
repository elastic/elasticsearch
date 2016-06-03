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

import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.rest.client.http.HttpResponse;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class RestMainActionIT extends ESIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(NetworkModule.HTTP_ENABLED.getKey(), true)
            .build();
    }

    public void testHeadRequest() throws IOException {
        final HttpResponse response = httpClient().method("HEAD").path("/").execute();
        assertThat(response.getStatusCode(), equalTo(200));
        assertThat(response.getBody(), nullValue());
    }

    public void testGetRequest() throws IOException {
        final HttpResponse response = httpClient().path("/").execute();
        assertThat(response.getStatusCode(), equalTo(200));
        assertThat(response.getBody(), containsString("cluster_name"));
    }
}
