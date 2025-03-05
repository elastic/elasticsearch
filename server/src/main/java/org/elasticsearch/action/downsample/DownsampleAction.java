/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.downsample;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
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
    public static final TimeValue DEFAULT_WAIT_TIMEOUT = new TimeValue(1, TimeUnit.DAYS);

    private DownsampleAction() {
        super(NAME);
    }

    public static class Request extends MasterNodeRequest<Request> implements IndicesRequest, ToXContentObject {
        private String sourceIndex;
        private String targetIndex;
        private TimeValue waitTimeout;
        private DownsampleConfig downsampleConfig;

        public Request(
            TimeValue masterNodeTimeout,
            final String sourceIndex,
            final String targetIndex,
            final TimeValue waitTimeout,
            final DownsampleConfig downsampleConfig
        ) {
            super(masterNodeTimeout);
            this.sourceIndex = sourceIndex;
            this.targetIndex = targetIndex;
            this.waitTimeout = waitTimeout == null ? DEFAULT_WAIT_TIMEOUT : waitTimeout;
            this.downsampleConfig = downsampleConfig;
        }

        public Request(TimeValue masterNodeTimeout) {
            super(masterNodeTimeout);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            sourceIndex = in.readString();
            targetIndex = in.readString();
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_10_X)) {
                waitTimeout = TimeValue.parseTimeValue(in.readString(), "timeout");
            } else {
                waitTimeout = DEFAULT_WAIT_TIMEOUT;
            }
            downsampleConfig = new DownsampleConfig(in);
        }

        @Override
        public String[] indices() {
            return new String[] { sourceIndex };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
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
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_10_X)) {
                out.writeString(waitTimeout.getStringRep());
            }
            downsampleConfig.writeTo(out);
        }

        public String getSourceIndex() {
            return sourceIndex;
        }

        public String getTargetIndex() {
            return targetIndex;
        }

        /**
         * @return the time to wait for the persistent tasks the complete downsampling
         */
        public TimeValue getWaitTimeout() {
            return waitTimeout;
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
            builder.field("wait_timeout", waitTimeout);
            downsampleConfig.toXContent(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceIndex, targetIndex, waitTimeout, downsampleConfig);
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
                && Objects.equals(waitTimeout, other.waitTimeout)
                && Objects.equals(downsampleConfig, other.downsampleConfig);
        }
    }

}
