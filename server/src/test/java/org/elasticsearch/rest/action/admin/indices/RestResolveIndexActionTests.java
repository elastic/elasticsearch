/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class RestResolveIndexActionTests extends ESTestCase {

    private void executeRequest(BytesArray body, boolean cpsEnabled) throws Exception {
        final Settings settings = Settings.builder().put("serverless.cross_project.enabled", cpsEnabled).build();
        final var action = new RestResolveIndexAction(settings);
        final var request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_resolve/index/foo")
            .withContent(body, XContentType.JSON)
            .build();

        final NodeClient nodeClient = new NodeClient(settings, mock(ThreadPool.class), TestProjectResolvers.DEFAULT_PROJECT_ONLY) {
            @SuppressWarnings("unchecked")
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                final var resolveIndexRequest = asInstanceOf(ResolveIndexAction.Request.class, request);
                assertThat(resolveIndexRequest.indicesOptions().resolveCrossProjectIndexExpression(), equalTo(cpsEnabled));
                listener.onResponse((Response) new ResolveIndexAction.Response(List.of(), List.of(), List.of()));
            }
        };

        final var restChannel = new FakeRestChannel(request, true, 1);
        action.handleRequest(request, restChannel, nodeClient);
        assertThat(restChannel.responses().get(), equalTo(1));
    }

    public void testAddResolveCrossProjectBasedOnSettingValue() throws Exception {
        // Empty body is always fine irrespective of CPS status.
        executeRequest(randomBoolean() ? new BytesArray("{}") : new BytesArray(""), randomBoolean());
    }

    public void testResolveIndexWithInvalidBody() {
        // Invalid request body.
        ElasticsearchException error = assertThrows(ElasticsearchException.class, () -> executeRequest(new BytesArray("""
            {
              "foo": "bar"
            }
            """), randomBoolean()));
        assertThat(error.getMessage(), equalTo("Couldn't parse request body"));
        assertThat(error.getCause().toString(), Matchers.containsString("request does not support [foo]"));
    }

    public void testResolveIndexWithProjectRoutingForCps() throws Exception {
        // Project routing is allowed when CPS is turned on.
        executeRequest(new BytesArray("""
            {
              "project_routing": "_alias:_origin"
            }
            """), true);
    }

    public void testResolveIndexWithProjectRoutingForNonCps() {
        // Project routing is disallowed when CPS is turned off and is treated as an invalid request body.
        ElasticsearchException error = assertThrows(ElasticsearchException.class, () -> executeRequest(new BytesArray("""
            {
              "project_routing": "_alias:_origin"
            }
            """), false));
        assertThat(error.getMessage(), equalTo("Couldn't parse request body"));
        assertThat(error.getCause().toString(), Matchers.containsString("request does not support [project_routing]"));
    }
}
