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

import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.junit.Test;

import static org.elasticsearch.rest.CorsRegexTests.httpClient;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class CorsRegexDefaultTests extends ElasticsearchIntegrationTest {

    @Test
    public void testCorsSettingDefaultBehaviourDoesNotReturnAnything() throws Exception {
        String corsValue = "http://localhost:9200";
        HttpResponse response = httpClient().method("GET").path("/").addHeader("User-Agent", "Mozilla Bar").addHeader("Origin", corsValue).execute();

        assertThat(response.getStatusCode(), is(200));
        assertThat(response.getHeaders(), not(hasKey("Access-Control-Allow-Origin")));
        assertThat(response.getHeaders(), not(hasKey("Access-Control-Allow-Credentials")));
    }

    @Test
    public void testThatOmittingCorsHeaderDoesNotReturnAnything() throws Exception {
        HttpResponse response = httpClient().method("GET").path("/").execute();

        assertThat(response.getStatusCode(), is(200));
        assertThat(response.getHeaders(), not(hasKey("Access-Control-Allow-Origin")));
        assertThat(response.getHeaders(), not(hasKey("Access-Control-Allow-Credentials")));
    }
}
