/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.AbstractSearchTestCase;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class RestValidateQueryActionTests extends AbstractSearchTestCase {

    private ThreadPool threadPool = new TestThreadPool(RestValidateQueryActionTests.class.getName());
    private NodeClient client = new NodeClient(Settings.EMPTY, threadPool);

    private UsageService usageService = new UsageService();
    private RestController controller = new RestController(
        null,
        client,
        new NoneCircuitBreakerService(),
        usageService,
        TelemetryProvider.NOOP
    );
    private RestValidateQueryAction action = new RestValidateQueryAction();

    /**
     * Configures {@link NodeClient} to stub {@link ValidateQueryAction} transport action.
     * <p>
     * This lower level of validation is out of the scope of this test.
     */
    @Before
    public void stubValidateQueryAction() {
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet());

        final TransportAction<? extends ActionRequest, ? extends ActionResponse> transportAction = new TransportAction<>(
            ValidateQueryAction.NAME,
            new ActionFilters(Collections.emptySet()),
            taskManager,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        ) {
            @Override
            protected void doExecute(Task task, ActionRequest request, ActionListener<ActionResponse> listener) {}
        };

        final Map<ActionType<?>, TransportAction<?, ?>> actions = new HashMap<>();
        actions.put(ValidateQueryAction.INSTANCE, transportAction);

        client.initialize(actions, taskManager, () -> "local", mock(Transport.Connection.class), null);
        controller.registerHandler(action);
    }

    @After
    public void terminateThreadPool() {
        terminate(threadPool);

        threadPool = null;
        client = null;

        usageService = null;
        controller = null;
        action = null;
    }

    public void testRestValidateQueryAction() throws Exception {
        // GIVEN a valid query
        final String content = """
            {"query":{"bool":{"must":{"term":{"user":"kimchy"}}}}}
            """;

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
        return new FakeRestRequest.Builder(xContentRegistry()).withPath("index1/type1/_validate/query")
            .withParams(emptyMap())
            .withContent(new BytesArray(content), XContentType.JSON)
            .build();
    }
}
