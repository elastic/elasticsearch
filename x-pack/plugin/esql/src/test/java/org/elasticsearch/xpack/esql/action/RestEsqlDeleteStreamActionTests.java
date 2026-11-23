/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.streaming.DeleteStreamResultRequest;
import org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor;
import org.elasticsearch.xpack.esql.streaming.StreamIdTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests the route, the registration and the id of {@code DELETE /_query/stream/{id}}.
 */
public class RestEsqlDeleteStreamActionTests extends ESTestCase {

    private static RestRequest request(String id, Map<String, String> queryParams) {
        Map<String, String> all = new HashMap<>(queryParams);
        // the REST layer puts the path parameters into the same map
        all.put("id", id);
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.DELETE)
            .withPath("/_query/stream/" + id)
            .withParams(all)
            .build();
    }

    // ---- route and registration ----

    /** The id is required: the only route has it in the path. */
    public void testRoute() {
        RestEsqlDeleteStreamAction handler = new RestEsqlDeleteStreamAction();
        List<RestHandler.Route> routes = handler.routes();
        assertThat(routes, hasSize(1));
        assertThat(routes.get(0).getMethod(), equalTo(RestRequest.Method.DELETE));
        assertThat(routes.get(0).getPath(), equalTo("/_query/stream/{id}"));
        assertThat(handler.getName(), equalTo("esql_delete_stream"));
    }

    /**
     * The route is registered exactly when the stream POST is registered and the feature flag for stored stream results is enabled:
     * a stream can be deleted only where it can be created, and a delete removes what is stored.
     */
    public void testRegisteredOnlyWhenStreamsAndStoredStreamResultsEnabled() throws IOException {
        try (EsqlPlugin plugin = new EsqlPlugin()) {
            List<RestHandler> handlers = plugin.getRestHandlers(
                // only the settings are read while the handlers are created
                new ActionPlugin.RestHandlersServices(Settings.EMPTY, null, null, null),
                null,
                null
            );
            boolean registered = handlers.stream().anyMatch(h -> h instanceof RestEsqlDeleteStreamAction);
            assertThat(registered, equalTo(RestEsqlStreamQueryAction.ENABLED && EsqlStreamResultsIndexDescriptor.FEATURE_FLAG.isEnabled()));
        }
    }

    // ---- id ----

    public void testIdComesFromThePath() {
        String id = StreamIdTests.randomStreamId().encoded();
        DeleteStreamResultRequest delete = RestEsqlDeleteStreamAction.parseRequest(request(id, Map.of()));
        assertThat(delete.id(), equalTo(id));
        assertThat(delete.validate(), nullValue());
    }

    /** There is no pattern and no list of ids. Whatever is in the path is one id, and the action answers that it is not found. */
    public void testIdIsPassedThroughUnchanged() {
        for (String id : new String[] { "*", "_all", "a,b", "a*" }) {
            assertThat(RestEsqlDeleteStreamAction.parseRequest(request(id, Map.of())).id(), equalTo(id));
        }
    }

    // ---- parameters ----

    /** The handler reads nothing but the id, so the REST layer rejects every other parameter as unrecognized. */
    public void testUnrecognizedParametersAreRejected() {
        String parameter = randomFrom("wait_for_completion", "keep_alive", "ids");
        RestRequest request = request("the-id", Map.of(parameter, "x"));
        // the request is refused before anything runs, so the client is not used
        expectThrows(
            IllegalArgumentException.class,
            containsString("unrecognized parameter: [" + parameter + "]"),
            () -> new RestEsqlDeleteStreamAction().handleRequest(request, new FakeRestChannel(request, true), null)
        );
    }

    // ---- execution ----

    /**
     * The handler runs {@link EsqlStreamDeleteAction} with the id from the path, and answers 404 when the action does not find the
     * stream, which is what every id gets while nothing is stored. The client answers the action itself, so only the handler is under
     * test.
     */
    public void testExecutesTheActionAndAnswersNotFoundWhenTheStreamIsNotFound() throws Exception {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            List<ActionRequest> executed = new ArrayList<>();
            NodeClient client = new NoOpNodeClient(threadPool) {
                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    assertThat(action, equalTo(EsqlStreamDeleteAction.INSTANCE));
                    executed.add(request);
                    listener.onFailure(new ResourceNotFoundException(((DeleteStreamResultRequest) request).id()));
                }
            };
            String id = StreamIdTests.randomStreamId().encoded();
            RestRequest request = request(id, Map.of());
            FakeRestChannel channel = new FakeRestChannel(request, true);
            new RestEsqlDeleteStreamAction().handleRequest(request, channel, client);

            assertThat(executed, hasSize(1));
            assertThat(((DeleteStreamResultRequest) executed.get(0)).id(), equalTo(id));
            assertThat(channel.capturedResponse().status(), equalTo(RestStatus.NOT_FOUND));
            assertThat(channel.capturedResponse().content().utf8ToString(), containsString(id));
        } finally {
            terminate(threadPool);
        }
    }
}
