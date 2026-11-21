/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor;
import org.elasticsearch.xpack.esql.streaming.GetStreamResultRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Tests the route, the registration and the parameter parsing of {@code GET /_query/stream/{id}}.
 */
public class RestEsqlGetStreamResultActionTests extends ESTestCase {

    private static RestRequest request(Map<String, String> params) {
        Map<String, String> all = new HashMap<>(params);
        // the REST layer puts the path parameters into the same map
        all.put("id", "the-id");
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.GET)
            .withPath("/_query/stream/the-id")
            .withParams(all)
            .build();
    }

    private static GetStreamResultRequest parse(Map<String, String> params) {
        return RestEsqlGetStreamResultAction.parseRequest(request(params));
    }

    // ---- route and registration ----

    public void testRoute() {
        RestEsqlGetStreamResultAction handler = new RestEsqlGetStreamResultAction();
        List<RestHandler.Route> routes = handler.routes();
        assertThat(routes, hasSize(1));
        assertThat(routes.get(0).getMethod(), equalTo(RestRequest.Method.GET));
        assertThat(routes.get(0).getPath(), equalTo("/_query/stream/{id}"));
        assertThat(handler.getName(), equalTo("esql_get_stream_result"));
    }

    /**
     * The route is registered exactly when the stream POST is registered and the feature flag for stored stream results is enabled:
     * a stream can be read only where it can be created, and a read needs what is stored.
     */
    public void testRegisteredOnlyWhenStreamsAndStoredStreamResultsEnabled() throws IOException {
        try (EsqlPlugin plugin = new EsqlPlugin()) {
            List<RestHandler> handlers = plugin.getRestHandlers(
                // only the settings are read while the handlers are created
                new ActionPlugin.RestHandlersServices(Settings.EMPTY, null, null, null),
                null,
                null
            );
            boolean registered = handlers.stream().anyMatch(h -> h instanceof RestEsqlGetStreamResultAction);
            assertThat(registered, equalTo(RestEsqlStreamQueryAction.ENABLED && EsqlStreamResultsIndexDescriptor.FEATURE_FLAG.isEnabled()));
        }
    }

    // ---- parameters that are accepted ----

    public void testDefaults() {
        GetStreamResultRequest get = parse(Map.of());
        assertThat(get.streamId(), equalTo("the-id"));
        assertThat(get.offset(), equalTo(0L));
        assertThat("no max_rows means no limit", get.hasRowLimit(), equalTo(false));
        assertThat("no max_bytes means no limit", get.hasByteLimit(), equalTo(false));
        assertThat("no wait_timeout means no wait", get.waitTimeout().millis(), lessThanOrEqualTo(0L));
    }

    public void testValues() {
        GetStreamResultRequest get = parse(Map.of("offset", "1000", "max_rows", "50", "max_bytes", "1mb", "wait_timeout", "2s"));
        assertThat(get.offset(), equalTo(1000L));
        assertThat(get.maxRows(), equalTo(50L));
        assertThat(get.maxBytes(), equalTo(1024L * 1024L));
        assertThat(get.waitTimeout(), equalTo(TimeValue.timeValueSeconds(2)));
    }

    public void testZeroLimitsMeanStatusOnly() {
        assertThat(parse(Map.of("max_rows", "0")).statusOnly(), equalTo(true));
        assertThat(parse(Map.of("max_bytes", "0")).statusOnly(), equalTo(true));
    }

    public void testWaitTimeoutAtTheUpperLimitIsAccepted() {
        assertThat(parse(Map.of("wait_timeout", "5m")).waitTimeout(), equalTo(GetStreamResultRequest.MAX_WAIT_TIMEOUT));
    }

    /**
     * A wait_timeout of 0 or -1 means no wait, as an unset one does, and -1 is accepted with a unit too. Any other negative value is
     * rejected, see below.
     */
    public void testZeroAndMinusOneWaitTimeoutMeanNoWait() {
        for (String value : new String[] { "0", "0s", "-1", "-1s" }) {
            assertThat(value, parse(Map.of("wait_timeout", value)).waitTimeout().millis(), lessThanOrEqualTo(0L));
        }
    }

    // ---- parameters that are rejected ----

    /** A value that cannot be parsed, a negative offset or limit, and a wait_timeout above the upper limit are all a 400. */
    public void testInvalidValuesAreBadRequests() {
        for (Map<String, String> params : List.of(
            Map.of("offset", "-1"),
            Map.of("offset", "abc"),
            Map.of("max_rows", "-5"),
            Map.of("max_rows", "x"),
            Map.of("max_bytes", "-1"),
            Map.of("max_bytes", "lots"),
            Map.of("wait_timeout", "soon"),
            Map.of("wait_timeout", "-2s"),
            Map.of("wait_timeout", "6m")
        )) {
            RuntimeException e = expectThrows(RuntimeException.class, () -> parse(params));
            assertThat("a bad request for " + params, ExceptionsHelper.status(e), equalTo(RestStatus.BAD_REQUEST));
        }
    }

    /** The handler reads nothing but the parameters above, so the REST layer rejects every other one as unrecognized. */
    public void testUnrecognizedParametersAreRejected() {
        String parameter = randomFrom("keep_alive", "wait_for_completion_timeout", "ids");
        RestRequest request = request(Map.of(parameter, "x"));
        // the request is refused before anything runs, so the client is not used
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RestEsqlGetStreamResultAction().handleRequest(request, new FakeRestChannel(request, true), null)
        );
        assertThat(e.getMessage(), containsString("unrecognized parameter: [" + parameter + "]"));
    }

    // ---- execution ----

    /**
     * The handler runs {@link EsqlStreamGetResultAction} with the parsed request, and answers 404 when the action does not find the
     * stream, which is what every id gets while nothing is stored. The client answers the action itself, so only the handler is under
     * test.
     */
    public void testExecutesTheActionAndAnswersNotFoundWhenTheStreamIsNotFound() throws Exception {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        RestRequest request = request(Map.of("offset", "5", "max_rows", "10"));
        try {
            List<ActionRequest> executed = new ArrayList<>();
            // the REST handler runs the action through a RestCancellableNodeClient, which needs a cancellable task and a node id
            NodeClient client = new NoOpNodeClient(threadPool) {
                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> Task executeAndReturnTask(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    assertThat(action, equalTo(EsqlStreamGetResultAction.INSTANCE));
                    executed.add(request);
                    listener.onFailure(new ResourceNotFoundException("the-id"));
                    return new CancellableTask(1, "transport", action.name(), "", null, Map.of());
                }

                @Override
                public String getLocalNodeId() {
                    return "node";
                }
            };
            FakeRestChannel channel = new FakeRestChannel(request, true);
            new RestEsqlGetStreamResultAction().handleRequest(request, channel, client);

            assertThat(executed, hasSize(1));
            GetStreamResultRequest get = (GetStreamResultRequest) executed.get(0);
            assertThat(get.streamId(), equalTo("the-id"));
            assertThat(get.offset(), equalTo(5L));
            assertThat(get.maxRows(), equalTo(10L));
            assertThat(channel.capturedResponse().status(), equalTo(RestStatus.NOT_FOUND));
            assertThat(channel.capturedResponse().content().utf8ToString(), containsString("the-id"));
        } finally {
            // RestCancellableNodeClient tracks the HTTP channel in a static map until it is closed
            request.getHttpChannel().close();
            terminate(threadPool);
        }
    }
}
