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

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.rest.BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER;
import static org.mockito.Mockito.mock;

public class RestGetMappingActionTests extends RestActionTestCase {

    private ThreadPool threadPool;

    @Before
    public void setUpAction() {
        threadPool = new TestThreadPool(RestValidateQueryActionTests.class.getName());
        controller().registerHandler(new RestGetMappingAction(threadPool));
    }

    @After
    public void tearDownAction() {
        assertTrue(terminate(threadPool));
    }

    public void testTypeExistsDeprecation() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("type", "_doc");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withMethod(RestRequest.Method.HEAD)
            .withParams(params)
            .build();

        RestGetMappingAction handler = new RestGetMappingAction(threadPool);
        handler.prepareRequest(request, mock(NodeClient.class));

        assertWarnings("Type exists requests are deprecated, as types have been deprecated.");
    }

    public void testTypeInPath() {
        // Test that specifying a type while setting include_type_name to false
        // results in an illegal argument exception.
        Map<String, String> params = new HashMap<>();
        params.put(INCLUDE_TYPE_NAME_PARAMETER, "false");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withMethod(RestRequest.Method.GET)
            .withPath("some_index/some_type/_mapping/some_field")
            .withParams(params)
            .build();

        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteVerifier((arg1, arg2) -> null);

        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        controller().dispatchRequest(request, channel, threadContext);

        assertEquals(1, channel.errors().get());
        assertEquals(RestStatus.BAD_REQUEST, channel.capturedResponse().status());
    }

    /**
     * Setting "include_type_name" to true or false should cause a deprecation warning starting in 7.0
     */
    public void testTypeUrlParameterDeprecation() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put(INCLUDE_TYPE_NAME_PARAMETER, Boolean.toString(randomBoolean()));
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withMethod(RestRequest.Method.GET)
            .withParams(params)
            .withPath("/some_index/_mappings")
            .build();

        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteVerifier((arg1, arg2) -> null);

        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        controller().dispatchRequest(request, channel, threadContext);

        assertWarnings(RestGetMappingAction.TYPES_DEPRECATION_MESSAGE);
    }

}
