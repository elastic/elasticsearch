/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MlTaskParams;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

public class StartDatafeedAction extends ActionType<NodeAcknowledgedResponse> {

    public static final ParseField START_TIME = new ParseField("start");
    public static final ParseField END_TIME = new ParseField("end");
    public static final ParseField TIMEOUT = new ParseField("timeout");

    public static final StartDatafeedAction INSTANCE = new StartDatafeedAction();
    public static final String NAME = "cluster:admin/xpack/ml/datafeed/start";

    private StartDatafeedAction() {
        super(NAME);
    }

    public static class Request extends MasterNodeRequest<Request> implements ToXContentObject {

        public static Request parseRequest(String datafeedId, XContentParser parser) {
            DatafeedParams params = DatafeedParams.PARSER.apply(parser, null);
            if (datafeedId != null) {
                params.datafeedId = datafeedId;
            }
            return new Request(params);
        }

        private DatafeedParams params;

        public Request(String datafeedId, long startTime) {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT);
            this.params = new DatafeedParams(datafeedId, startTime);
        }

        public Request(String datafeedId, String startTime) {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT);
            this.params = new DatafeedParams(datafeedId, startTime);
        }

        public Request(DatafeedParams params) {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT);
            this.params = params;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            params = new DatafeedParams(in);
        }

        public DatafeedParams getParams() {
            return params;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException e = null;
            if (params.endTime != null && params.endTime <= params.startTime) {
                e = ValidateActions.addValidationError(
                    START_TIME.getPreferredName()
                        + " ["
                        + params.startTime
                        + "] must be earlier than "
                        + END_TIME.getPreferredName()
                        + " ["
                        + params.endTime
                        + "]",
                    e
                );
            }
            return e;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            params.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params xContentParams) throws IOException {
            this.params.toXContent(builder, xContentParams);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(params);
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
            return Objects.equals(params, other.params);
        }
    }

    public static class DatafeedParams implements PersistentTaskParams, MlTaskParams {

        public static final ParseField INDICES = new ParseField("indices");

        public static final ObjectParser<DatafeedParams, Void> PARSER = new ObjectParser<>(
            MlTasks.DATAFEED_TASK_NAME,
            true,
            DatafeedParams::new
        );
        static {
            PARSER.declareString((params, datafeedId) -> params.datafeedId = datafeedId, DatafeedConfig.ID);
            PARSER.declareString(
                (params, startTime) -> params.startTime = parseDateOrThrow(startTime, START_TIME, System::currentTimeMillis),
                START_TIME
            );
            PARSER.declareString(DatafeedParams::setEndTime, END_TIME);
            PARSER.declareString((params, val) -> params.setTimeout(TimeValue.parseTimeValue(val, TIMEOUT.getPreferredName())), TIMEOUT);
            PARSER.declareString(DatafeedParams::setJobId, Job.ID);
            PARSER.declareStringArray(DatafeedParams::setDatafeedIndices, INDICES);
            PARSER.declareObject(
                DatafeedParams::setIndicesOptions,
                (p, c) -> IndicesOptions.fromMap(p.map(), SearchRequest.DEFAULT_INDICES_OPTIONS),
                DatafeedConfig.INDICES_OPTIONS
            );
        }

        public static long parseDateOrThrow(String date, ParseField paramName, LongSupplier now) {
            DateMathParser dateMathParser = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.toDateMathParser();

            try {
                return dateMathParser.parse(date, now).toEpochMilli();
            } catch (Exception e) {
                String msg = Messages.getMessage(Messages.REST_INVALID_DATETIME_PARAMS, paramName.getPreferredName(), date);
                throw new ElasticsearchParseException(msg, e);
            }
        }

        public static DatafeedParams fromXContent(XContentParser parser) {
            return parseRequest(null, parser);
        }

        public static DatafeedParams parseRequest(String datafeedId, XContentParser parser) {
            DatafeedParams params = PARSER.apply(parser, null);
            if (datafeedId != null) {
                params.datafeedId = datafeedId;
            }
            return params;
        }

        public DatafeedParams(String datafeedId, long startTime) {
            this.datafeedId = ExceptionsHelper.requireNonNull(datafeedId, DatafeedConfig.ID.getPreferredName());
            if (startTime < 0) {
                throw new IllegalArgumentException("[" + START_TIME.getPreferredName() + "] must not be negative [" + startTime + "].");
            }
            this.startTime = startTime;
        }

        public DatafeedParams(String datafeedId, String startTime) {
            this(datafeedId, parseDateOrThrow(startTime, START_TIME, System::currentTimeMillis));
        }

        public DatafeedParams(StreamInput in) throws IOException {
            datafeedId = in.readString();
            startTime = in.readVLong();
            endTime = in.readOptionalLong();
            timeout = TimeValue.timeValueMillis(in.readVLong());
            jobId = in.readOptionalString();
            datafeedIndices = in.readStringCollectionAsList();
            indicesOptions = IndicesOptions.readIndicesOptions(in);
        }

        DatafeedParams() {}

        private String datafeedId;
        private long startTime;
        private Long endTime;
        private TimeValue timeout = TimeValue.timeValueSeconds(20);
        private List<String> datafeedIndices = Collections.emptyList();
        private String jobId;
        private IndicesOptions indicesOptions = SearchRequest.DEFAULT_INDICES_OPTIONS;

        public String getDatafeedId() {
            return datafeedId;
        }

        public long getStartTime() {
            return startTime;
        }

        public Long getEndTime() {
            return endTime;
        }

        public void setEndTime(String endTime) {
            setEndTime(parseDateOrThrow(endTime, END_TIME, System::currentTimeMillis));
        }

        public void setEndTime(Long endTime) {
            this.endTime = endTime;
        }

        public TimeValue getTimeout() {
            return timeout;
        }

        public void setTimeout(TimeValue timeout) {
            this.timeout = timeout;
        }

        public String getJobId() {
            return jobId;
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        public List<String> getDatafeedIndices() {
            return datafeedIndices;
        }

        public void setDatafeedIndices(List<String> datafeedIndices) {
            this.datafeedIndices = datafeedIndices;
        }

        public IndicesOptions getIndicesOptions() {
            return indicesOptions;
        }

        public DatafeedParams setIndicesOptions(IndicesOptions indicesOptions) {
            this.indicesOptions = ExceptionsHelper.requireNonNull(indicesOptions, DatafeedConfig.INDICES_OPTIONS);
            return this;
        }

        @Override
        public String getWriteableName() {
            return MlTasks.DATAFEED_TASK_NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.MINIMUM_COMPATIBLE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(datafeedId);
            out.writeVLong(startTime);
            out.writeOptionalLong(endTime);
            out.writeVLong(timeout.millis());
            out.writeOptionalString(jobId);
            out.writeStringCollection(datafeedIndices);
            indicesOptions.writeIndicesOptions(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field(DatafeedConfig.ID.getPreferredName(), datafeedId);
            builder.field(START_TIME.getPreferredName(), String.valueOf(startTime));
            if (endTime != null) {
                builder.field(END_TIME.getPreferredName(), String.valueOf(endTime));
            }
            builder.field(TIMEOUT.getPreferredName(), timeout.getStringRep());
            if (jobId != null) {
                builder.field(Job.ID.getPreferredName(), jobId);
            }
            if (datafeedIndices.isEmpty() == false) {
                builder.field(INDICES.getPreferredName(), datafeedIndices);
            }

            builder.startObject(DatafeedConfig.INDICES_OPTIONS.getPreferredName());
            indicesOptions.toXContent(builder, params);
            builder.endObject();

            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(datafeedId, startTime, endTime, timeout, jobId, datafeedIndices, indicesOptions);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            DatafeedParams other = (DatafeedParams) obj;
            return Objects.equals(datafeedId, other.datafeedId)
                && Objects.equals(startTime, other.startTime)
                && Objects.equals(endTime, other.endTime)
                && Objects.equals(timeout, other.timeout)
                && Objects.equals(jobId, other.jobId)
                && Objects.equals(indicesOptions, other.indicesOptions)
                && Objects.equals(datafeedIndices, other.datafeedIndices);
        }

        @Override
        public String getMlId() {
            return datafeedId;
        }
    }

    public interface DatafeedTaskMatcher {

    }

}
