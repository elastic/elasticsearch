/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;
import java.util.function.LongSupplier;

public class StartDatafeedAction extends Action<StartDatafeedAction.Request, StartDatafeedAction.Response> {

    public static final ParseField START_TIME = new ParseField("start");
    public static final ParseField END_TIME = new ParseField("end");
    public static final ParseField TIMEOUT = new ParseField("timeout");

    public static final StartDatafeedAction INSTANCE = new StartDatafeedAction();
    public static final String NAME = "cluster:admin/xpack/ml/datafeed/start";
    public static final String TASK_NAME = "xpack/ml/datafeed";

    private StartDatafeedAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends MasterNodeRequest<Request> implements ToXContentObject {

        public static Request fromXContent(XContentParser parser) {
            return parseRequest(null, parser);
        }

        public static Request parseRequest(String datafeedId, XContentParser parser) {
            DatafeedParams params = DatafeedParams.PARSER.apply(parser, null);
            if (datafeedId != null) {
                params.datafeedId = datafeedId;
            }
            return new Request(params);
        }

        private DatafeedParams params;

        public Request(String datafeedId, long startTime) {
            this.params = new DatafeedParams(datafeedId, startTime);
        }

        public Request(String datafeedId, String startTime) {
            this.params = new DatafeedParams(datafeedId, startTime);
        }

        public Request(DatafeedParams params) {
            this.params = params;
        }

        public Request(StreamInput in) throws IOException {
            readFrom(in);
        }

        public Request() {
        }

        public DatafeedParams getParams() {
            return params;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException e = null;
            if (params.endTime != null && params.endTime <= params.startTime) {
                e = ValidateActions.addValidationError(START_TIME.getPreferredName() + " ["
                        + params.startTime + "] must be earlier than " + END_TIME.getPreferredName()
                        + " [" + params.endTime + "]", e);
            }
            return e;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            params = new DatafeedParams(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            params.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            this.params.toXContent(builder, params);
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

    public static class DatafeedParams implements XPackPlugin.XPackPersistentTaskParams {

        public static ObjectParser<DatafeedParams, Void> PARSER = new ObjectParser<>(TASK_NAME, DatafeedParams::new);

        static {
            PARSER.declareString((params, datafeedId) -> params.datafeedId = datafeedId, DatafeedConfig.ID);
            PARSER.declareString((params, startTime) -> params.startTime = parseDateOrThrow(
                    startTime, START_TIME, System::currentTimeMillis), START_TIME);
            PARSER.declareString(DatafeedParams::setEndTime, END_TIME);
            PARSER.declareString((params, val) ->
                    params.setTimeout(TimeValue.parseTimeValue(val, TIMEOUT.getPreferredName())), TIMEOUT);
        }

        static long parseDateOrThrow(String date, ParseField paramName, LongSupplier now) {
            DateMathParser dateMathParser = new DateMathParser(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER);

            try {
                return dateMathParser.parse(date, now);
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
        }

        DatafeedParams() {
        }

        private String datafeedId;
        private long startTime;
        private Long endTime;
        private TimeValue timeout = TimeValue.timeValueSeconds(20);

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

        @Override
        public String getWriteableName() {
            return TASK_NAME;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT.minimumCompatibilityVersion();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(datafeedId);
            out.writeVLong(startTime);
            out.writeOptionalLong(endTime);
            out.writeVLong(timeout.millis());
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
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(datafeedId, startTime, endTime, timeout);
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
            return Objects.equals(datafeedId, other.datafeedId) &&
                    Objects.equals(startTime, other.startTime) &&
                    Objects.equals(endTime, other.endTime) &&
                    Objects.equals(timeout, other.timeout);
        }
    }

    public static class Response extends AcknowledgedResponse {
        public Response() {
            super();
        }

        public Response(boolean acknowledged) {
            super(acknowledged);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AcknowledgedResponse that = (AcknowledgedResponse) o;
            return isAcknowledged() == that.isAcknowledged();
        }

        @Override
        public int hashCode() {
            return Objects.hash(isAcknowledged());
        }

    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response> {

        RequestBuilder(ElasticsearchClient client, StartDatafeedAction action) {
            super(client, action, new Request());
        }
    }

    public interface DatafeedTaskMatcher {

    }

}
