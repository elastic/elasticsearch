/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class RestResolveIndexActionTests extends ESTestCase {

    public void testAddResolveCrossProjectBasedOnSettingValue() throws Exception {
        final boolean cpsEnabled = randomBoolean();
        final Settings settings = Settings.builder().put("serverless.cross_project.enabled", cpsEnabled).build();
        final var action = new RestResolveIndexAction(settings);
        final var request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_resolve/index/foo")
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
}
