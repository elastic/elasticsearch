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
package org.elasticsearch.action.admin.cluster.node.tasks;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cockroach.CockroachRequest;
import org.elasticsearch.cockroach.CockroachResponse;
import org.elasticsearch.cockroach.CockroachService;
import org.elasticsearch.cockroach.TransportCockroachAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;

/**
 * A plugin that adds a test cockroach task.
 */
public class TestCockroachActionPlugin extends Plugin implements ActionPlugin {

    @Override
    public List<ActionHandler<? extends ActionRequest<?>, ? extends ActionResponse>> getActions() {
        return singletonList(new ActionHandler<>(TestCockroachAction.INSTANCE, TransportTestCockroachAction.class));
    }

    public static class TestRequest extends CockroachRequest<TestRequest> {

        private String targetNode = null;

        private String responseNode = null;

        private String testParam = null;

        public TestRequest() {

        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String getWriteableName() {
            return TestCockroachAction.NAME;
        }

        public void setTargetNode(String targetNode) {
            this.targetNode = targetNode;
        }

        public void setResponseNode(String responseNode) {
            this.responseNode = responseNode;
        }

        public void setTestParam(String testParam) {
            this.testParam = testParam;
        }

        public String getTargetNode() {
            return targetNode;
        }

        public String getResponseNode() {
            return responseNode;
        }

        public String getTestParam() {
            return testParam;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(targetNode);
            out.writeOptionalString(responseNode);
            out.writeOptionalString(testParam);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            targetNode = in.readOptionalString();
            responseNode = in.readOptionalString();
            testParam = in.readOptionalString();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.endObject();
            return builder;
        }

    }

    public static class TestResponse extends CockroachResponse {

        public TestResponse() {

        }

        @Override
        public String getWriteableName() {
            return TestCockroachAction.NAME;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.endObject();
            return builder;
        }


    }

    public static class TestCockroachTaskRequestBuilder extends
        ActionRequestBuilder<TestRequest, TestResponse, TestCockroachTaskRequestBuilder> {

        protected TestCockroachTaskRequestBuilder(ElasticsearchClient client, Action<TestRequest, TestResponse,
            TestCockroachTaskRequestBuilder> action, TestRequest request) {
            super(client, action, request);
        }

        public TestCockroachTaskRequestBuilder testParam(String testParam) {
            request.setTestParam(testParam);
            return this;
        }

        public TestCockroachTaskRequestBuilder responseNode(String responseNode) {
            request.setResponseNode(responseNode);
            return this;
        }

        public TestCockroachTaskRequestBuilder targetNode(String targetNode) {
            request.setTargetNode(targetNode);
            return this;
        }

    }

    public static class TestCockroachAction extends Action<TestRequest, TestResponse, TestCockroachTaskRequestBuilder> {

        public static final TestCockroachAction INSTANCE = new TestCockroachAction();
        public static final String NAME = "cluster:admin/cockroach/test";

        private TestCockroachAction() {
            super(NAME);
        }

        @Override
        public TestResponse newResponse() {
            return new TestResponse();
        }

        @Override
        public TestCockroachTaskRequestBuilder newRequestBuilder(ElasticsearchClient client) {
            return new TestCockroachTaskRequestBuilder(client, this, new TestRequest());
        }
    }


    public static class TransportTestCockroachAction extends TransportCockroachAction<TestRequest, TestResponse> {

        @Inject
        public TransportTestCockroachAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                            CockroachService cockroachService, ActionFilters actionFilters,
                                            IndexNameExpressionResolver indexNameExpressionResolver) {
            super(settings, TestCockroachAction.NAME, false, threadPool, transportService, cockroachService, actionFilters,
                indexNameExpressionResolver, TestRequest::new, TestResponse::new, ThreadPool.Names.MANAGEMENT);
        }

        @Override
        public DiscoveryNode executorNode(TestRequest request, ClusterState clusterState) {
            if (request.getTargetNode() == null) {
                return clusterState.getNodes().getMasterNode();
            } else {
                return clusterState.getNodes().get(request.getTargetNode());
            }
        }

        @Override
        public DiscoveryNode responseNode(TestRequest request, ClusterState clusterState) {
            if (request.getResponseNode() == null) {
                return clusterState.getNodes().getMasterNode();
            } else {
                return clusterState.getNodes().get(request.getResponseNode());
            }
        }

        @Override
        public void executorNodeOperation(Task task, TestRequest request, ActionListener<TestResponse> listener) {
            listener.onResponse(new TestResponse());
        }

    }

}
