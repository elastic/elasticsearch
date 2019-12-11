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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.AbstractSearchTestCase;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.usage.UsageService;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RestValidateQueryActionTests extends AbstractSearchTestCase {

    private static ThreadPool threadPool = new TestThreadPool(RestValidateQueryActionTests.class.getName());
    private static NodeClient client = new NodeClient(Settings.EMPTY, threadPool);

    private static UsageService usageService = new UsageService();
    private static RestController controller = new RestController(emptySet(), null, client,
        new NoneCircuitBreakerService(), usageService);
    private static RestValidateQueryAction action = new RestValidateQueryAction(controller);

    /**
     * Configures {@link NodeClient} to stub {@link ValidateQueryAction} transport action.
     * <p>
     * This lower level of validation is out of the scope of this test.
     */
    @BeforeClass
    public static void stubValidateQueryAction() {
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet());

        final TransportAction transportAction = new TransportAction(ValidateQueryAction.NAME,
            new ActionFilters(Collections.emptySet()), taskManager) {
            @Override
            protected void doExecute(Task task, ActionRequest request, ActionListener listener) {
            }
        };

        final Map<ActionType, TransportAction> actions = new HashMap<>();
        actions.put(ValidateQueryAction.INSTANCE, transportAction);

        client.initialize(actions, taskManager, () -> "local", null);
    }

    @AfterClass
    public static void terminateThreadPool() {
        terminate(threadPool);

        threadPool = null;
        client = null;

        usageService = null;
        controller = null;
        action = null;
    }

    public void testRestValidateQueryAction() throws Exception {
        // GIVEN a valid query
        final String content = "{\"query\":{\"bool\":{\"must\":{\"term\":{\"user\":\"kimchy\"}}}}}";

        final RestRequest request = createRestRequest(content);
        final FakeRestChannel channel = new FakeRestChannel(request, true, 0);

        // WHEN
        action.handleRequest(request, channel, client);

        // THEN query is valid (i.e. not marked as invalid)
        assertThat(channel.responses().get(), equalTo(0));
        assertThat(channel.errors().get(), equalTo(0));
        assertNull(channel.capturedResponse());
    }

    public void testRestValidateQueryAction_emptyQuery() throws Exception {
        // GIVEN an empty (i.e. invalid) query wrapped into a valid JSON
        final String content = "{\"query\":{}}";

        final RestRequest request = createRestRequest(content);
        final FakeRestChannel channel = new FakeRestChannel(request, true, 0);

        // WHEN
        action.handleRequest(request, channel, client);

        // THEN query is marked as invalid
        assertThat(channel.responses().get(), equalTo(1));
        assertThat(channel.errors().get(), equalTo(0));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("{\"valid\":false}"));
    }

    public void testRestValidateQueryAction_malformedQuery() throws Exception {
        // GIVEN an invalid query due to a malformed JSON
        final String content = "{malformed_json}";

        final RestRequest request = createRestRequest(content);
        final FakeRestChannel channel = new FakeRestChannel(request, true, 0);

        // WHEN
        action.handleRequest(request, channel, client);

        // THEN query is marked as invalid
        assertThat(channel.responses().get(), equalTo(1));
        assertThat(channel.errors().get(), equalTo(0));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("{\"valid\":false}"));
    }

    private RestRequest createRestRequest(String content) {
        return new FakeRestRequest.Builder(xContentRegistry())
            .withPath("index1/type1/_validate/query")
            .withParams(emptyMap())
            .withContent(new BytesArray(content), XContentType.JSON)
            .build();
    }

}
