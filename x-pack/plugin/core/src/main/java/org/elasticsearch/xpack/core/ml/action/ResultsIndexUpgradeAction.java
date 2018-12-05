/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;


public class ResultsIndexUpgradeAction extends Action<AcknowledgedResponse> {
    public static final ResultsIndexUpgradeAction INSTANCE = new ResultsIndexUpgradeAction();
    public static final String NAME = "cluster:admin/xpack/ml/results/upgrade";

    private ResultsIndexUpgradeAction() {
        super(NAME);
    }

    @Override
    public AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    public static class Request
        extends MasterNodeReadRequest<ResultsIndexUpgradeAction.Request>
        implements IndicesRequest, ToXContentObject {

        private static final ParseField REINDEX_BATCH_SIZE = new ParseField("reindex_batch_size");

        public static ObjectParser<Request, Void> PARSER = new ObjectParser<>("ml_results_index_upgrade", true, Request::new);
        static {
            PARSER.declareInt(Request::setReindexBatchSize, REINDEX_BATCH_SIZE);
        }

        static final String INDEX = AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + "*";
        private int reindexBatchSize = 1000;

        /**
         * Should this task store its result?
         */
        private boolean shouldStoreResult;

        // for serialization
        public Request() {
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            reindexBatchSize = in.readInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(reindexBatchSize);
        }

        @Override
        public String[] indices() {
            return new String[]{INDEX};
        }

        @Override
        public IndicesOptions indicesOptions() {
            //TODO consider lenientExpandOpen() ?
            return IndicesOptions.strictExpandOpenAndForbidClosed();
        }

        /**
         * Should this task store its result after it has finished?
         */
        public ResultsIndexUpgradeAction.Request setShouldStoreResult(boolean shouldStoreResult) {
            this.shouldStoreResult = shouldStoreResult;
            return this;
        }

        @Override
        public boolean getShouldStoreResult() {
            return shouldStoreResult;
        }

        public ResultsIndexUpgradeAction.Request setReindexBatchSize(int reindexBatchSize) {
            this.reindexBatchSize = reindexBatchSize;
            return this;
        }

        public int getReindexBatchSize() {
            return reindexBatchSize;
        }

        @Override
        public ActionRequestValidationException validate() {
           return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ResultsIndexUpgradeAction.Request request = (ResultsIndexUpgradeAction.Request) o;
            return Objects.equals(reindexBatchSize, request.reindexBatchSize);
        }

        @Override
        public int hashCode() {
            return Objects.hash(reindexBatchSize);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers) {
                @Override
                public boolean shouldCancelChildrenOnCancellation() {
                    return true;
                }
            };
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(REINDEX_BATCH_SIZE.getPreferredName(), reindexBatchSize);
            builder.endObject();
            return builder;
        }
    }

    public static class RequestBuilder extends MasterNodeReadOperationRequestBuilder<ResultsIndexUpgradeAction.Request,
        AcknowledgedResponse,
        ResultsIndexUpgradeAction.RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client) {
            super(client, INSTANCE, new ResultsIndexUpgradeAction.Request());
        }
    }

}
