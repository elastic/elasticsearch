/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class RestTasksActionTests extends ESTestCase {

    public void testConsumesParameters() throws Exception {
        RestTasksAction action = new RestTasksAction(() -> DiscoveryNodes.EMPTY_NODES);
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withParams(Map.of("parent_task_id", "the node:3", "nodes", "node1,node2", "actions", "*"))
            .build();
        FakeRestChannel fakeRestChannel = new FakeRestChannel(fakeRestRequest, false, 1);
        try (NoOpNodeClient nodeClient = buildNodeClient()) {
            action.handleRequest(fakeRestRequest, fakeRestChannel, nodeClient);
        }

        assertThat(fakeRestChannel.errors().get(), is(0));
        assertThat(fakeRestChannel.responses().get(), is(1));
    }

    private NoOpNodeClient buildNodeClient() {
        return new NoOpNodeClient(getTestName()) {
            @Override
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse>
            void doExecute(ActionType<Response> action, Request request, ActionListener<Response> listener) {
                listener.onResponse((Response) new ListTasksResponse(List.of(), List.of(), List.of()));
            }
        };
    }
}
