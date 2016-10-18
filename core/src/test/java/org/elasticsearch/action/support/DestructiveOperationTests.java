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

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.collection.IsArrayContainingInOrder.arrayContaining;

public class DestructiveOperationTests extends ESTestCase {

    public void testDestructiveOperationsCheckRunsBeforeActionFilters() throws Exception {
        Settings settings = Settings.builder()
                .put(NODE_NAME_SETTING.getKey(), "node_1")
                .put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), true).build();
        ThreadPool threadPool = new ThreadPool(settings);
        try {
            ActionFilters actionFilters = new ActionFilters(Collections.singleton(new TestActionFilter()));
            DestructiveOperations destructiveOperations = new DestructiveOperations(settings, new ClusterSettings(settings,
                    Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING)));
            String action = randomFrom(DeleteIndexAction.NAME, CloseIndexAction.NAME, OpenIndexAction.NAME);
            TestTransportAction testTransportAction = new TestTransportAction(action, threadPool, actionFilters, destructiveOperations);
            String[] indices;
            switch (randomIntBetween(0, 3)) {
                case 0:
                    indices = null;
                    break;
                case 1:
                    indices = Strings.EMPTY_ARRAY;
                    break;
                case 2:
                    indices = new String[]{"_all"};
                    break;
                case 3:
                    indices = new String[]{"*"};
                    break;
                case 4:
                    indices = new String[]{"*", "-index1"};
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            MockIndicesRequest indicesRequest = new MockIndicesRequest(indices);
            ExecutionException executionException = expectThrows(ExecutionException.class,
                    () -> testTransportAction.execute(indicesRequest).get());
            assertThat(executionException.getCause(), instanceOf(IllegalArgumentException.class));
            assertEquals("Wildcard expressions or all indices are not allowed for action [" + action + "]",
                    executionException.getCause().getMessage());
            if (indices == null) {
                assertNull(indicesRequest.indices());
            } else if (indices.length == 0) {
                assertEquals(0, indices.length);
            } else {
                assertThat(indicesRequest.indices(), arrayContaining(indices));
            }
        } finally {
            terminate(threadPool);
        }
    }

    private static class MockIndicesRequest extends ActionRequest<MockIndicesRequest> implements IndicesRequest {
        private final String[] indices;

        MockIndicesRequest(String... indices) {
            this.indices = indices;
        }

        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    private static class MockTransportResponse extends ActionResponse {

    }

    private static class TestTransportAction extends TransportAction<MockIndicesRequest, MockTransportResponse> {
        protected TestTransportAction(String actionName, ThreadPool threadPool, ActionFilters actionFilters,
                                      DestructiveOperations destructiveOperations) {
            super(Settings.EMPTY, actionName, threadPool, actionFilters, new IndexNameExpressionResolver(Settings.EMPTY),
                    new TaskManager(Settings.EMPTY), destructiveOperations);
        }

        @Override
        protected void doExecute(MockIndicesRequest request, ActionListener<MockTransportResponse> listener) {
        }
    }

    private static class TestActionFilter implements ActionFilter {
        @Override
        public int order() {
            return 0;
        }

        @Override
        public <Request extends ActionRequest<Request>, Response extends ActionResponse> void apply(Task task, String action, Request
                request, ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {
            if (request instanceof IndicesRequest.Replaceable) {
                //replace wildcards with non wildcard expression
                ((IndicesRequest.Replaceable) request).indices("index1", "index2", "index3");
            }
        }

        @Override
        public <Response extends ActionResponse> void apply(String action, Response response, ActionListener<Response> listener,
                                                            ActionFilterChain<?, Response> chain) {
        }
    }
}
