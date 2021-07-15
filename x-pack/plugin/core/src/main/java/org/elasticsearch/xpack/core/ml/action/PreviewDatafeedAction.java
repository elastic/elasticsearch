/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public class PreviewDatafeedAction extends ActionType<PreviewDatafeedAction.Response> {

    public static final PreviewDatafeedAction INSTANCE = new PreviewDatafeedAction();
    public static final String NAME = "cluster:admin/xpack/ml/datafeeds/preview";

    private PreviewDatafeedAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest implements ToXContentObject {

        private static final String BLANK_ID = "";

        public static final ParseField DATAFEED_CONFIG = new ParseField("datafeed_config");
        public static final ParseField JOB_CONFIG = new ParseField("job_config");

        private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(
            "preview_datafeed_action",
            Request.Builder::new
        );
        static {
            PARSER.declareObject(Builder::setDatafeedBuilder, DatafeedConfig.STRICT_PARSER, DATAFEED_CONFIG);
            PARSER.declareObject(Builder::setJobBuilder, Job.STRICT_PARSER, JOB_CONFIG);
        }

        public static Request fromXContent(XContentParser parser, @Nullable String datafeedId) {
            Builder builder = PARSER.apply(parser, null);
            // We don't need to check for "inconsistent ids" as we don't parse an ID from the body
            if (datafeedId != null) {
                builder.setDatafeedId(datafeedId);
            }
            return builder.build();
        }

        private final String datafeedId;
        private final DatafeedConfig datafeedConfig;
        private final Job.Builder jobConfig;

        public Request(StreamInput in) throws IOException {
            super(in);
            datafeedId = in.readString();
            datafeedConfig = in.readOptionalWriteable(DatafeedConfig::new);
            jobConfig = in.readOptionalWriteable(Job.Builder::new);
        }

        public Request(String datafeedId) {
            this.datafeedId = ExceptionsHelper.requireNonNull(datafeedId, DatafeedConfig.ID);
            this.datafeedConfig = null;
            this.jobConfig = null;
        }

        public Request(DatafeedConfig datafeedConfig, Job.Builder jobConfig) {
            this.datafeedId = BLANK_ID;
            this.datafeedConfig = ExceptionsHelper.requireNonNull(datafeedConfig, DATAFEED_CONFIG.getPreferredName());
            this.jobConfig = jobConfig;
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

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(datafeedId);
            out.writeOptionalWriteable(datafeedConfig);
            out.writeOptionalWriteable(jobConfig);
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

        public static class Builder {
            private String datafeedId;
            private DatafeedConfig.Builder datafeedBuilder;
            private Job.Builder jobBuilder;

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
                    if (datafeedBuilder == null) {
                        throw new IllegalArgumentException("[datafeed_config] must be present when a [job_config] is provided");
                    }
                }
                if (datafeedId != null && (datafeedBuilder != null || jobBuilder != null)) {
                    throw new IllegalArgumentException(
                        "[datafeed_id] cannot be supplied when either [job_config] or [datafeed_config] is present"
                    );
                }
                return datafeedId != null ?
                    new Request(datafeedId) :
                    new Request(datafeedBuilder == null ? null : datafeedBuilder.build(), jobBuilder);
            }
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final BytesReference preview;

        public Response(StreamInput in) throws IOException {
            super(in);
            preview = in.readBytesReference();
        }

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
