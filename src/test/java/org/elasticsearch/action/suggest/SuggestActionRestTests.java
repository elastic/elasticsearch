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

package org.elasticsearch.action.suggest;

import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.rest.ElasticsearchRestIntegrationTest;

import org.elasticsearch.test.rest.client.RestException;
import org.elasticsearch.test.rest.client.RestResponse;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;


/**
 * Tests for the suggest REST API endpoint.
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class SuggestActionRestTests extends ElasticsearchRestIntegrationTest {

    private static final String TEST_INDEX = "test_idx_1";
    private static final String TEST_TYPE = "test_type_1";

    @Test
    public void postIndexTypeSuggest() throws Exception {

        createIndex(TEST_INDEX);
        Map<String, String> params = new HashMap<>();
        params.put("index", TEST_INDEX);
        params.put("type", TEST_TYPE);
        RestResponse response = null;

        try {
            response = restClient.callApi("suggest", "POST", params, "{user:andrew}");
        } catch (RestException e) {
            assertThat(e.statusCode(), equalTo(BAD_REQUEST.getStatus()));
            return;
        }

        Assert.fail("Expected response code " + BAD_REQUEST.getStatus() + " but got " + response.getStatusCode());
    }

    @Test
    public void putIndexTypeSuggest() throws Exception {

        createIndex(TEST_INDEX);
        Map<String, String> params = new HashMap<>();
        params.put("index", TEST_INDEX);
        params.put("type", TEST_TYPE);
        RestResponse response = null;

        try {
            response = restClient.callApi("suggest", "PUT", params, "{user:andrew}");
        } catch (RestException e) {
            assertThat(e.statusCode(), equalTo(BAD_REQUEST.getStatus()));
            return;
        }

        Assert.fail("Expected response code " + BAD_REQUEST.getStatus() + " but got " + response.getStatusCode());
    }
}
