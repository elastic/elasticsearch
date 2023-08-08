/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.downsample;

import org.elasticsearch.TransportVersion;
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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class DownsampleAction extends ActionType<AcknowledgedResponse> {
    public static final DownsampleAction INSTANCE = new DownsampleAction();
    public static final String NAME = "indices:admin/xpack/downsample";
    public static final TimeValue DEFAULT_TIMEOUT = new TimeValue(1, TimeUnit.DAYS);

    private DownsampleAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends MasterNodeRequest<Request> implements IndicesRequest, ToXContentObject {
        private String sourceIndex;
        private String targetIndex;
        private TimeValue timeout;
        private DownsampleConfig downsampleConfig;

        public Request(
            final String sourceIndex,
            final String targetIndex,
            final TimeValue timeout,
            final DownsampleConfig downsampleConfig
        ) {
            this.sourceIndex = sourceIndex;
            this.targetIndex = targetIndex;
            this.timeout = timeout == null ? DEFAULT_TIMEOUT : timeout;
            this.downsampleConfig = downsampleConfig;
        }

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            sourceIndex = in.readString();
            targetIndex = in.readString();
            timeout = in.getTransportVersion().onOrAfter(TransportVersion.V_8_500_054)
                ? TimeValue.parseTimeValue(in.readString(), "timeout")
                : DEFAULT_TIMEOUT;
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
            out.writeString(
                out.getTransportVersion().onOrAfter(TransportVersion.V_8_500_054) ? timeout.getStringRep() : DEFAULT_TIMEOUT.getStringRep()
            );
            downsampleConfig.writeTo(out);
        }

        public String getSourceIndex() {
            return sourceIndex;
        }

        public String getTargetIndex() {
            return targetIndex;
        }

        public TimeValue getTimeout() {
            return timeout;
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
            builder.field("timeout", timeout);
            downsampleConfig.toXContent(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceIndex, targetIndex, timeout, downsampleConfig);
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
                && Objects.equals(timeout, other.timeout)
                && Objects.equals(downsampleConfig, other.downsampleConfig);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, AcknowledgedResponse> {

        protected RequestBuilder(ElasticsearchClient client, DownsampleAction action) {
            super(client, action, new Request());
        }
    }
}
