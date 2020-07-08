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
package org.elasticsearch.http;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.io.IOException;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_CREDENTIALS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_METHODS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ENABLED;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Test Indexing Pressure Metrics and Statistics
 */
@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 2)
public class IndexingPressureRestIT extends HttpSmokeTestCase {

    private static final Settings unboundedWriteQueue = Settings.builder().put("thread_pool.write.queue_size", -1).build();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(unboundedWriteQueue)
            .build();
    }

    public void testThatRegularExpressionWorksOnMatch() throws IOException {
        {
            String corsValue = "http://localhost:9200";
            Request request = new Request("GET", "/");
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("User-Agent", "Mozilla Bar");
            options.addHeader("Origin", corsValue);
            request.setOptions(options);
            Response response = getRestClient().performRequest(request);
        }
        {
            String corsValue = "https://localhost:9201";
            Request request = new Request("GET", "/");
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("User-Agent", "Mozilla Bar");
            options.addHeader("Origin", corsValue);
            request.setOptions(options);
            Response response = getRestClient().performRequest(request);
            assertThat(response.getHeader("Access-Control-Allow-Credentials"), is("true"));
        }
        throw new AssertionError("Failed");
    }
}
