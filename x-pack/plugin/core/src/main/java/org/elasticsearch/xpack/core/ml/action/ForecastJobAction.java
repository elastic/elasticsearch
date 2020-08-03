/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.Forecast;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class ForecastJobAction extends ActionType<ForecastJobAction.Response> {

    public static final ForecastJobAction INSTANCE = new ForecastJobAction();
    public static final String NAME = "cluster:admin/xpack/ml/job/forecast";

    private ForecastJobAction() {
        super(NAME, ForecastJobAction.Response::new);
    }

    public static class Request extends JobTaskRequest<Request> implements ToXContentObject {

        public static final ParseField DURATION = new ParseField("duration");
        public static final ParseField EXPIRES_IN = new ParseField("expires_in");
        public static final ParseField MAX_MODEL_MEMORY = new ParseField("max_model_memory");

        public static final ByteSizeValue FORECAST_LOCAL_STORAGE_LIMIT = new ByteSizeValue(500, ByteSizeUnit.MB);

        // Max allowed duration: 10 years
        private static final TimeValue MAX_DURATION = TimeValue.parseTimeValue("3650d", "");
        private static final long MIN_MODEL_MEMORY = new ByteSizeValue(1, ByteSizeUnit.MB).getBytes();

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, jobId) -> request.jobId = jobId, Job.ID);
            PARSER.declareString(Request::setDuration, DURATION);
            PARSER.declareString(Request::setExpiresIn, EXPIRES_IN);
            PARSER.declareField(Request::setMaxModelMemory, (p, c) -> {
                if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return ByteSizeValue.parseBytesSizeValue(p.text(), MAX_MODEL_MEMORY.getPreferredName()).getBytes();
                } else if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                    return p.longValue();
                }
                throw new XContentParseException("Unsupported token [" + p.currentToken() + "]");
            }, MAX_MODEL_MEMORY, ObjectParser.ValueType.VALUE);
        }

        public static Request parseRequest(String jobId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (jobId != null) {
                request.jobId = jobId;
            }
            return request;
        }

        private TimeValue duration;
        private TimeValue expiresIn;
        private Long maxModelMemory;

        public Request() {
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.duration = in.readOptionalTimeValue();
            this.expiresIn = in.readOptionalTimeValue();
            this.maxModelMemory = in.readOptionalVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalTimeValue(duration);
            out.writeOptionalTimeValue(expiresIn);
            out.writeOptionalVLong(maxModelMemory);
        }

        public Request(String jobId) {
            super(jobId);
        }

        public TimeValue getDuration() {
            return duration;
        }

        public void setDuration(String duration) {
            setDuration(TimeValue.parseTimeValue(duration, DURATION.getPreferredName()));
        }

        public void setDuration(TimeValue duration) {
            this.duration = duration;
            if (this.duration.compareTo(TimeValue.ZERO) <= 0) {
                throw new IllegalArgumentException("[" + DURATION.getPreferredName() + "] must be positive: ["
                        + duration.getStringRep() + "]");
            }
            if (this.duration.compareTo(MAX_DURATION) > 0) {
                throw new IllegalArgumentException("[" + DURATION.getPreferredName() + "] must be "
                        + MAX_DURATION.getStringRep() + " or less: [" + duration.getStringRep() + "]");
            }
        }

        public TimeValue getExpiresIn() {
            return expiresIn;
        }

        public void setExpiresIn(String expiration) {
            setExpiresIn(TimeValue.parseTimeValue(expiration, EXPIRES_IN.getPreferredName()));
        }

        public void setExpiresIn(TimeValue expiresIn) {
            this.expiresIn = expiresIn;
            if (this.expiresIn.compareTo(TimeValue.ZERO) < 0) {
                throw new IllegalArgumentException("[" + EXPIRES_IN.getPreferredName() + "] must be non-negative: ["
                        + expiresIn.getStringRep() + "]");
            }
        }

        public void setMaxModelMemory(long numBytes) {
            if (numBytes < MIN_MODEL_MEMORY) {
                throw new IllegalArgumentException("[" + MAX_MODEL_MEMORY.getPreferredName() + "] must be at least 1mb.");
            }
            if (numBytes >= FORECAST_LOCAL_STORAGE_LIMIT.getBytes()) {
                throw ExceptionsHelper.badRequestException(
                    "[{}] must be less than {}",
                    MAX_MODEL_MEMORY.getPreferredName(),
                    FORECAST_LOCAL_STORAGE_LIMIT.getStringRep());
            }
            this.maxModelMemory = numBytes;
        }

        public Long getMaxModelMemory() {
            return maxModelMemory;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, duration, expiresIn, maxModelMemory);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(jobId, other.jobId)
                    && Objects.equals(duration, other.duration)
                    && Objects.equals(expiresIn, other.expiresIn)
                    && Objects.equals(maxModelMemory, other.maxModelMemory);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Job.ID.getPreferredName(), jobId);
            if (duration != null) {
                builder.field(DURATION.getPreferredName(), duration.getStringRep());
            }
            if (expiresIn != null) {
                builder.field(EXPIRES_IN.getPreferredName(), expiresIn.getStringRep());
            }
            if (maxModelMemory != null) {
                builder.field(MAX_MODEL_MEMORY.getPreferredName(), new ByteSizeValue(maxModelMemory).getStringRep());
            }
            builder.endObject();
            return builder;
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable, ToXContentObject {

        private boolean acknowledged;
        private String forecastId;

        public Response(boolean acknowledged, String forecastId) {
            super(null, null);
            this.acknowledged = acknowledged;
            this.forecastId = forecastId;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            acknowledged = in.readBoolean();
            forecastId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(acknowledged);
            out.writeString(forecastId);
        }

        public boolean isAcknowledged() {
            return acknowledged;
        }

        public String getForecastId() {
            return forecastId;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("acknowledged", acknowledged);
            builder.field(Forecast.FORECAST_ID.getPreferredName(), forecastId);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return this.acknowledged == other.acknowledged && Objects.equals(this.forecastId, other.forecastId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(acknowledged, forecastId);
        }
    }
}

