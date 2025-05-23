/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ml.action.StartDatafeedAction.DatafeedParams.parseDateOrThrow;
import static org.elasticsearch.xpack.core.ml.action.StartDatafeedAction.END_TIME;
import static org.elasticsearch.xpack.core.ml.action.StartDatafeedAction.START_TIME;

public class PreviewDatafeedAction extends ActionType<PreviewDatafeedAction.Response> {

    public static final PreviewDatafeedAction INSTANCE = new PreviewDatafeedAction();
    public static final String NAME = "cluster:admin/xpack/ml/datafeeds/preview";

    private PreviewDatafeedAction() {
        super(NAME);
    }

    public static class Request extends LegacyActionRequest implements ToXContentObject {

        private static final String BLANK_ID = "";

        public static final ParseField DATAFEED_CONFIG = new ParseField("datafeed_config");
        public static final ParseField JOB_CONFIG = new ParseField("job_config");

        private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("preview_datafeed_action", Builder::new);

        static {
            PARSER.declareObject(Builder::setDatafeedBuilder, DatafeedConfig.STRICT_PARSER, DATAFEED_CONFIG);
            PARSER.declareObject(Builder::setJobBuilder, Job.REST_REQUEST_PARSER, JOB_CONFIG);
            PARSER.declareString(Builder::setStart, START_TIME);
            PARSER.declareString(Builder::setEnd, END_TIME);
        }

        public static Builder fromXContent(XContentParser parser, @Nullable String datafeedId) {
            Builder builder = PARSER.apply(parser, null);
            // We don't need to check for "inconsistent ids" as we don't parse an ID from the body
            if (datafeedId != null) {
                builder.setDatafeedId(datafeedId);
            }
            return builder;
        }

        private final String datafeedId;
        private final DatafeedConfig datafeedConfig;
        private final Job.Builder jobConfig;
        private final Long startTime;
        private final Long endTime;

        public Request(StreamInput in) throws IOException {
            super(in);
            datafeedId = in.readString();
            datafeedConfig = in.readOptionalWriteable(DatafeedConfig::new);
            jobConfig = in.readOptionalWriteable(Job.Builder::new);
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_3_0)) {
                this.startTime = in.readOptionalLong();
                this.endTime = in.readOptionalLong();
            } else {
                this.startTime = null;
                this.endTime = null;
            }
        }

        public Request(String datafeedId, String start, String end) {
            this.datafeedId = ExceptionsHelper.requireNonNull(datafeedId, DatafeedConfig.ID);
            this.datafeedConfig = null;
            this.jobConfig = null;
            this.startTime = start == null ? null : parseDateOrThrow(start, START_TIME, System::currentTimeMillis);
            this.endTime = end == null ? null : parseDateOrThrow(end, END_TIME, System::currentTimeMillis);
        }

        Request(String datafeedId, Long start, Long end) {
            this.datafeedId = ExceptionsHelper.requireNonNull(datafeedId, DatafeedConfig.ID);
            this.datafeedConfig = null;
            this.jobConfig = null;
            this.startTime = start;
            this.endTime = end;
        }

        public Request(DatafeedConfig datafeedConfig, Job.Builder jobConfig, Long start, Long end) {
            this.datafeedId = BLANK_ID;
            this.datafeedConfig = ExceptionsHelper.requireNonNull(datafeedConfig, DATAFEED_CONFIG.getPreferredName());
            this.jobConfig = jobConfig;
            this.startTime = start;
            this.endTime = end;
        }

        public String getDatafeedId() {
            return datafeedId;
        }

        public DatafeedConfig getDatafeedConfig() {
            return datafeedConfig;
        }

        public Job.Builder getJobConfig() {
            return jobConfig;
        }

        public OptionalLong getStartTime() {
            return startTime == null ? OptionalLong.empty() : OptionalLong.of(startTime);
        }

        public OptionalLong getEndTime() {
            return endTime == null ? OptionalLong.empty() : OptionalLong.of(endTime);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException e = null;
            if (endTime != null && startTime != null && endTime <= startTime) {
                e = ValidateActions.addValidationError(
                    START_TIME.getPreferredName()
                        + " ["
                        + startTime
                        + "] must be earlier than "
                        + END_TIME.getPreferredName()
                        + " ["
                        + endTime
                        + "]",
                    e
                );
            }
            return e;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(datafeedId);
            out.writeOptionalWriteable(datafeedConfig);
            out.writeOptionalWriteable(jobConfig);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_3_0)) {
                out.writeOptionalLong(startTime);
                out.writeOptionalLong(endTime);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (datafeedId.equals("") == false) {
                builder.field(DatafeedConfig.ID.getPreferredName(), datafeedId);
            }
            if (datafeedConfig != null) {
                builder.field(DATAFEED_CONFIG.getPreferredName(), datafeedConfig);
            }
            if (jobConfig != null) {
                builder.field(JOB_CONFIG.getPreferredName(), jobConfig);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(datafeedId, datafeedConfig, jobConfig);
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
            return Objects.equals(datafeedId, other.datafeedId)
                && Objects.equals(datafeedConfig, other.datafeedConfig)
                && Objects.equals(jobConfig, other.jobConfig);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, format("preview_datafeed[%s]", datafeedId), parentTaskId, headers);
        }

        public static class Builder {
            private String datafeedId;
            private DatafeedConfig.Builder datafeedBuilder;
            private Job.Builder jobBuilder;
            private Long startTime;
            private Long endTime;

            public Builder setDatafeedId(String datafeedId) {
                this.datafeedId = datafeedId;
                return this;
            }

            public Builder setDatafeedBuilder(DatafeedConfig.Builder datafeedBuilder) {
                this.datafeedBuilder = datafeedBuilder;
                return this;
            }

            public Builder setJobBuilder(Job.Builder jobBuilder) {
                this.jobBuilder = jobBuilder;
                return this;
            }

            public Builder setStart(String startTime) {
                if (startTime == null) {
                    return this;
                }
                return setStart(parseDateOrThrow(startTime, START_TIME, System::currentTimeMillis));
            }

            public Builder setStart(long start) {
                this.startTime = start;
                return this;
            }

            public Builder setEnd(String endTime) {
                if (endTime == null) {
                    return this;
                }
                return setEnd(parseDateOrThrow(endTime, END_TIME, System::currentTimeMillis));
            }

            public Builder setEnd(long end) {
                this.endTime = end;
                return this;
            }

            public Request build() {
                if (datafeedBuilder != null) {
                    datafeedBuilder.setId("preview_id");
                    if (datafeedBuilder.getJobId() == null && jobBuilder == null) {
                        throw new IllegalArgumentException("[datafeed_config.job_id] must be set or a [job_config] must be provided");
                    }
                    if (datafeedBuilder.getJobId() == null) {
                        datafeedBuilder.setJobId("preview_job_id");
                    }
                }
                if (jobBuilder != null) {
                    jobBuilder.setId("preview_job_id");
                    if (datafeedBuilder == null && jobBuilder.getDatafeedConfig() == null) {
                        throw new IllegalArgumentException(
                            "[datafeed_config] must be present when a [job_config.datafeed_config] is not present"
                        );
                    }
                    if (datafeedBuilder != null && jobBuilder.getDatafeedConfig() != null) {
                        throw new IllegalArgumentException(
                            "[datafeed_config] must not be present when a [job_config.datafeed_config] is present"
                        );
                    }
                    // If the datafeed_config has been provided via the jobBuilder, set it here for easier serialization and use
                    if (jobBuilder.getDatafeedConfig() != null) {
                        datafeedBuilder = jobBuilder.getDatafeedConfig().setJobId(jobBuilder.getId()).setId(jobBuilder.getId());
                    }
                }
                if (datafeedId != null && (datafeedBuilder != null || jobBuilder != null)) {
                    throw new IllegalArgumentException(
                        "[datafeed_id] cannot be supplied when either [job_config] or [datafeed_config] is present"
                    );
                }
                return datafeedId != null
                    ? new Request(datafeedId, startTime, endTime)
                    : new Request(datafeedBuilder == null ? null : datafeedBuilder.build(), jobBuilder, startTime, endTime);
            }
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final BytesReference preview;

        public Response(BytesReference preview) {
            this.preview = preview;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBytesReference(preview);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            try (InputStream stream = preview.streamInput()) {
                builder.rawValue(stream, XContentType.JSON);
            }
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(preview);
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
            return Objects.equals(preview, other.preview);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }

}
