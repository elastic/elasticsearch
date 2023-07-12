/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.downsample;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class DownsampleAction extends ActionType<AcknowledgedResponse> {
    public static final DownsampleAction INSTANCE = new DownsampleAction();
    public static final String NAME = "indices:admin/xpack/downsample";

    private DownsampleAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends MasterNodeRequest<Request> implements IndicesRequest, ToXContentObject {
        private String sourceIndex;
        private String targetIndex;
        private DownsampleConfig downsampleConfig;

        public Request(final String sourceIndex, final String targetIndex, final DownsampleConfig downsampleConfig) {
            this.sourceIndex = sourceIndex;
            this.targetIndex = targetIndex;
            this.downsampleConfig = downsampleConfig;
        }

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            sourceIndex = in.readString();
            targetIndex = in.readString();
            downsampleConfig = new DownsampleConfig(in);
        }

        @Override
        public String[] indices() {
            return new String[] { sourceIndex };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new DownsampleTask(id, type, action, parentTaskId, targetIndex, downsampleConfig, headers);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sourceIndex);
            out.writeString(targetIndex);
            downsampleConfig.writeTo(out);
        }

        public String getSourceIndex() {
            return sourceIndex;
        }

        public String getTargetIndex() {
            return targetIndex;
        }

        public DownsampleConfig getDownsampleConfig() {
            return downsampleConfig;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (sourceIndex == null) {
                validationException = addValidationError("source index is missing", validationException);
            }
            if (targetIndex == null) {
                validationException = addValidationError("target index name is missing", validationException);
            }
            if (downsampleConfig == null) {
                validationException = addValidationError("downsample configuration is missing", validationException);
            }
            return validationException;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("source_index", sourceIndex);
            builder.field("target_index", targetIndex);
            downsampleConfig.toXContent(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceIndex, targetIndex, downsampleConfig);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(sourceIndex, other.sourceIndex)
                && Objects.equals(targetIndex, other.targetIndex)
                && Objects.equals(downsampleConfig, other.downsampleConfig);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, AcknowledgedResponse> {

        protected RequestBuilder(ElasticsearchClient client, DownsampleAction action) {
            super(client, action, new Request());
        }
    }
}
