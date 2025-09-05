/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sample;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class PutSampleConfigAction extends ActionType<AcknowledgedResponse> {
    public static final String NAME = "indices:admin/sample/config/update";
    public static final PutSampleConfigAction INSTANCE = new PutSampleConfigAction();

    public PutSampleConfigAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<PutSampleConfigAction.Request> implements IndicesRequest.Replaceable {
        private final double rate;
        private final Integer maxSamples;
        private final ByteSizeValue maxSize;
        private final TimeValue timeToLive;
        private final String condition;
        private String[] indices = Strings.EMPTY_ARRAY;

        public Request(
            double rate,
            @Nullable Integer maxSamples,
            @Nullable ByteSizeValue maxSize,
            @Nullable TimeValue timeToLive,
            @Nullable String condition,
            @Nullable TimeValue masterNodeTimeout,
            @Nullable TimeValue ackTimeout
        ) {
            super(masterNodeTimeout, ackTimeout);
            this.rate = rate;
            this.maxSamples = maxSamples;
            this.maxSize = maxSize;
            this.timeToLive = timeToLive;
            this.condition = condition;
        }

        public double getRate() {
            return rate;
        }

        public Integer getMaxSamples() {
            return maxSamples;
        }

        public ByteSizeValue getMaxSize() {
            return maxSize;
        }

        public TimeValue getTimeToLive() {
            return timeToLive;
        }

        public String getCondition() {
            return condition;
        }

        @Override
        public PutSampleConfigAction.Request indices(String... dataStreamNames) {
            this.indices = dataStreamNames;
            return this;
        }

        @Override
        public boolean includeDataStreams() {
            return true;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.indices = in.readStringArray();
            this.rate = in.readDouble();
            this.maxSamples = in.readOptionalInt();
            this.maxSize = in.readOptionalWriteable(ByteSizeValue::readFrom);
            this.timeToLive = in.readOptionalTimeValue();
            this.condition = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(indices);
            out.writeDouble(rate);
            out.writeOptionalInt(maxSamples);
            out.writeOptionalWriteable(maxSize);
            out.writeOptionalTimeValue(timeToLive);
            out.writeOptionalString(condition);
        }

        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PutSampleConfigAction.Request request = (PutSampleConfigAction.Request) o;
            return Arrays.equals(indices, request.indices())
                && rate == request.rate
                && Objects.equals(maxSamples, request.maxSamples)
                && Objects.equals(maxSize, request.maxSize)
                && Objects.equals(timeToLive, request.timeToLive)
                && Objects.equals(condition, request.condition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                Arrays.hashCode(indices),
                rate,
                maxSamples,
                maxSize,
                timeToLive,
                condition,
                masterNodeTimeout(),
                ackTimeout()
            );
        }

    }
}
