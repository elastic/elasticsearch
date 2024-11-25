/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class RestTasksActionTests extends ESTestCase {

    public void testConsumesParameters() throws Exception {
        RestTasksAction action = new RestTasksAction(() -> DiscoveryNodes.EMPTY_NODES);
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(
            Map.of("parent_task_id", "the node:3", "nodes", "node1,node2", "actions", "*")
        ).build();
        FakeRestChannel fakeRestChannel = new FakeRestChannel(fakeRestRequest, randomBoolean(), 1);
        try (var threadPool = createThreadPool()) {
            final var nodeClient = buildNodeClient(threadPool);
            action.handleRequest(fakeRestRequest, fakeRestChannel, nodeClient);
        }

        assertThat(fakeRestChannel.errors().get(), is(0));
        assertThat(fakeRestChannel.responses().get(), is(1));
    }

    private NoOpNodeClient buildNodeClient(ThreadPool threadPool) {
        return new NoOpNodeClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                listener.onResponse((Response) new ListTasksResponse(List.of(), List.of(), List.of()));
            }
        };
    }
}
