/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.datasources.DataSourceModule;
import org.elasticsearch.xpack.esql.datasources.TestConnectionResult;

import java.io.IOException;
import java.util.Map;

/**
 * Internal node-level action for {@link TransportTestDataSourceConnectionAction}'s fan-out.
 * Runs the test-connection probe on the local node and returns the {@link TestConnectionResult}.
 * The coordinator sends this to every eligible node in parallel and aggregates the results.
 */
public class TestDataSourceNodeAction {

    static final String NAME = "internal:xpack/esql/datasource/test_connection/node";
    public static final ActionType<NodeResponse> TYPE = new ActionType<>(NAME);

    private TestDataSourceNodeAction() {}

    static class NodeRequest extends UntypedActionRequest {

        final String type;
        final Map<String, Object> rawSettings;

        NodeRequest(String type, Map<String, Object> rawSettings) {
            this.type = type;
            this.rawSettings = rawSettings;
        }

        @SuppressWarnings("unchecked")
        NodeRequest(StreamInput in) throws IOException {
            super(in);
            this.type = in.readString();
            this.rawSettings = (Map<String, Object>) in.readGenericValue();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(type);
            out.writeGenericValue(rawSettings);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    static class NodeResponse extends ActionResponse {

        final TestConnectionResult result;

        NodeResponse(TestConnectionResult result) {
            this.result = result;
        }

        NodeResponse(StreamInput in) throws IOException {
            byte status = in.readByte();
            this.result = switch (status) {
                case 0 -> TestConnectionResult.SUCCESS;
                case 1 -> TestConnectionResult.failure(in.readString());
                case 2 -> new TestConnectionResult.Untestable(in.readOptionalString());
                default -> throw new IOException("Unknown TestConnectionResult status byte: " + status);
            };
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            switch (result) {
                case TestConnectionResult.Success s -> out.writeByte((byte) 0);
                case TestConnectionResult.Failure f -> {
                    out.writeByte((byte) 1);
                    out.writeString(f.error());
                }
                case TestConnectionResult.Untestable u -> {
                    out.writeByte((byte) 2);
                    out.writeOptionalString(u.reason());
                }
            }
        }
    }

    public static class TransportAction extends HandledTransportAction<NodeRequest, NodeResponse> {

        private final DataSourceModule dataSourceModule;

        @Inject
        public TransportAction(
            TransportService transportService,
            ActionFilters actionFilters,
            ThreadPool threadPool,
            DataSourceModule dataSourceModule
        ) {
            super(NAME, transportService, actionFilters, NodeRequest::new, threadPool.executor(ThreadPool.Names.GENERIC));
            this.dataSourceModule = dataSourceModule;
        }

        @Override
        protected void doExecute(Task task, NodeRequest request, ActionListener<NodeResponse> listener) {
            try {
                TestConnectionResult result = dataSourceModule.testConnection(request.type, request.rawSettings);
                listener.onResponse(new NodeResponse(result));
            } catch (IllegalArgumentException e) {
                listener.onFailure(new ElasticsearchStatusException(e.getMessage(), RestStatus.BAD_REQUEST, e));
            }
        }
    }
}
