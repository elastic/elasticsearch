/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

/**
 * The route, the id and the feature gating of {@code DELETE /_query/stream/{id}}.
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

    /** The id is required: the only route has it in the path. */
    public void testRoute() {
        RestEsqlDeleteStreamAction handler = new RestEsqlDeleteStreamAction();
        List<RestHandler.Route> routes = handler.routes();
        assertThat(routes, hasSize(1));
        assertThat(routes.get(0).getMethod(), equalTo(RestRequest.Method.DELETE));
        assertThat(routes.get(0).getPath(), equalTo("/_query/stream/{id}"));
        assertThat(handler.getName(), equalTo("esql_delete_stream"));
    }

    public void testIdComesFromThePath() {
        String id = StreamIdTests.randomStreamId().encoded();
        DeleteStreamResultRequest delete = RestEsqlDeleteStreamAction.parseRequest(request(id, Map.of()));
        assertThat(delete.id(), equalTo(id));
        assertThat(delete.validate(), nullValue());
    }

    /** There is no pattern and no list of ids: whatever is in the path is one id, and the action answers that it is not found. */
    public void testIdIsPassedThroughUnchanged() {
        for (String id : new String[] { "*", "_all", "a,b", "a*" }) {
            assertThat(RestEsqlDeleteStreamAction.parseRequest(request(id, Map.of())).id(), equalTo(id));
        }
    }

    /** The handler reads nothing but the id, so the REST layer rejects every other parameter as unrecognized. */
    public void testBadRequestParameter() {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            NodeClient client = new NoOpNodeClient(threadPool);
            String parameter = randomFrom("wait_for_completion", "keep_alive", "ids");
            RestRequest request = request("the-id", Map.of(parameter, "x"));
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new RestEsqlDeleteStreamAction().handleRequest(request, new FakeRestChannel(request, true), client)
            );
            assertThat(e.getMessage(), containsString("unrecognized parameter: [" + parameter + "]"));
        } finally {
            terminate(threadPool);
        }
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
            boolean delete = handlers.stream().anyMatch(h -> h instanceof RestEsqlDeleteStreamAction);
            assertThat(delete, equalTo(RestEsqlStreamQueryAction.ENABLED && EsqlStreamResultsIndexDescriptor.FEATURE_FLAG.isEnabled()));
        }
    }
}
