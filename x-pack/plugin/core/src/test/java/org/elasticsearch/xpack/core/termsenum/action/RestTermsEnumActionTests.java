/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termsenum.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.termsenum.rest.RestTermsEnumAction;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class RestTermsEnumActionTests extends ESTestCase {

    private static ThreadPool threadPool = new TestThreadPool(RestTermsEnumActionTests.class.getName());
    private static NodeClient client = new NodeClient(Settings.EMPTY, threadPool, TestProjectResolvers.alwaysThrow());

    private static UsageService usageService = new UsageService();
    private static RestController controller = new RestController(
        null,
        client,
        new NoneCircuitBreakerService(),
        usageService,
        TelemetryProvider.NOOP
    );
    private static RestTermsEnumAction action = new RestTermsEnumAction(CrossProjectModeDecider.NOOP);

    /**
     * Captures the {@link TermsEnumRequest} that the stubbed transport action receives so tests can assert how
     * {@link RestTermsEnumAction} prepared it (e.g. whether cross-project index expression resolution was enabled).
     */
    private static final AtomicReference<TermsEnumRequest> capturedRequest = new AtomicReference<>();

    /**
     * Configures {@link NodeClient} to stub {@link TermsEnumAction} transport action.
     * <p>
     * This lower level of execution is out of the scope of this test.
     */
    @BeforeClass
    public static void stubTermEnumAction() {
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet());

        final TransportAction<? extends ActionRequest, ? extends ActionResponse> transportAction = new TransportAction<>(
            TermsEnumAction.NAME,
            new ActionFilters(Collections.emptySet()),
            taskManager,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        ) {
            @Override
            protected void doExecute(Task task, ActionRequest request, ActionListener<ActionResponse> listener) {
                capturedRequest.set((TermsEnumRequest) request);
            }
        };

        final Map<ActionType<?>, TransportAction<?, ?>> actions = new HashMap<>();
        actions.put(TermsEnumAction.INSTANCE, transportAction);

        client.initialize(actions, taskManager, () -> "local", mock(Transport.Connection.class), null);
        controller.registerHandler(action);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
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

    public void testRestTermEnumAction() throws Exception {
        // GIVEN a valid query
        final String content = """
            {
              "field": "a",
              "string": "foo",
              "search_after": "football",
              "index_filter": {
                "bool": {
                  "must": {
                    "term": {
                      "user": "kimchy"
                    }
                  }
                }
              }
            }""";

        final RestRequest request = createRestRequest(content);
        final FakeRestChannel channel = new FakeRestChannel(request, true);

        // WHEN
        action.handleRequest(request, channel, client);

        // THEN request is parsed OK
        assertThat(channel.responses().get(), equalTo(0));
        assertThat(channel.errors().get(), equalTo(0));
        assertNull(channel.capturedResponse());
    }

    public void testRestTermEnumActionMissingField() throws Exception {
        // GIVEN an invalid query
        final String content = """
            {
              "string": "foo",
              "index_filter": {
                "bool": {
                  "must": {
                    "term": {
                      "user": "kimchy"
                    }
                  }
                }
              }
            }""";

        final RestRequest request = createRestRequest(content);
        final FakeRestChannel channel = new FakeRestChannel(request, true);

        // WHEN
        action.handleRequest(request, channel, client);

        // THEN request is invalid - missing mandatory "field" parameter.
        assertThat(channel.responses().get(), equalTo(0));
        assertThat(channel.errors().get(), equalTo(1));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("field cannot be null"));
    }

    public void testRestTermEnumActionCrossProjectEnabled() throws Exception {
        // GIVEN a REST handler whose decider has cross-project mode enabled
        final RestTermsEnumAction crossProjectAction = new RestTermsEnumAction(
            new CrossProjectModeDecider(Settings.builder().put("serverless.cross_project.enabled", true).build())
        );

        capturedRequest.set(null);
        final RestRequest request = createRestRequest("""
            {
              "field": "a"
            }""");
        final FakeRestChannel channel = new FakeRestChannel(request, true);

        // WHEN the handler prepares and executes the request (no route registration, direct dispatch)
        crossProjectAction.handleRequest(request, channel, client);

        // THEN the executed request resolves a cross-project index expression
        assertThat(channel.errors().get(), equalTo(0));
        assertNotNull(capturedRequest.get());
        assertThat(capturedRequest.get().indicesOptions().resolveCrossProjectIndexExpression(), is(true));
    }

    public void testRestTermEnumActionCrossProjectDisabled() throws Exception {
        // GIVEN a REST handler whose decider leaves cross-project mode disabled (the NOOP control)
        final RestTermsEnumAction nonCrossProjectAction = new RestTermsEnumAction(CrossProjectModeDecider.NOOP);

        capturedRequest.set(null);
        final RestRequest request = createRestRequest("""
            {
              "field": "a"
            }""");
        final FakeRestChannel channel = new FakeRestChannel(request, true);

        // WHEN the handler prepares and executes the request (no route registration, direct dispatch)
        nonCrossProjectAction.handleRequest(request, channel, client);

        // THEN the executed request does not resolve a cross-project index expression
        assertThat(channel.errors().get(), equalTo(0));
        assertNotNull(capturedRequest.get());
        assertThat(capturedRequest.get().indicesOptions().resolveCrossProjectIndexExpression(), is(false));
    }

    private RestRequest createRestRequest(String content) {
        return new FakeRestRequest.Builder(xContentRegistry()).withPath("index1/_terms_enum")
            .withParams(emptyMap())
            .withContent(new BytesArray(content), XContentType.JSON)
            .build();
    }

}
