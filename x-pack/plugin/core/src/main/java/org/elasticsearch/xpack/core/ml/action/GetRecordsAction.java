/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesResponse;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class GetRecordsAction extends ActionType<GetRecordsAction.Response> {

    public static final GetRecordsAction INSTANCE = new GetRecordsAction();
    public static final String NAME = "cluster:monitor/xpack/ml/job/results/records/get";

    private GetRecordsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest implements ToXContentObject {

        public static final ParseField START = new ParseField("start");
        public static final ParseField END = new ParseField("end");
        public static final ParseField EXCLUDE_INTERIM = new ParseField("exclude_interim");
        public static final ParseField RECORD_SCORE_FILTER = new ParseField("record_score");
        public static final ParseField SORT = new ParseField("sort");
        public static final ParseField DESCENDING = new ParseField("desc");

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, jobId) -> request.jobId = jobId, Job.ID);
            PARSER.declareStringOrNull(Request::setStart, START);
            PARSER.declareStringOrNull(Request::setEnd, END);
            PARSER.declareString(Request::setSort, SORT);
            PARSER.declareBoolean(Request::setDescending, DESCENDING);
            PARSER.declareBoolean(Request::setExcludeInterim, EXCLUDE_INTERIM);
            PARSER.declareObject(Request::setPageParams, PageParams.PARSER, PageParams.PAGE);
            PARSER.declareDouble(Request::setRecordScore, RECORD_SCORE_FILTER);
        }

        public static Request parseRequest(String jobId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (jobId != null) {
                request.jobId = jobId;
            }
            return request;
        }

        private String jobId;
        private String start;
        private String end;
        private boolean excludeInterim = false;
        private PageParams pageParams = new PageParams();
        private double recordScoreFilter = 0.0;
        private String sort = RECORD_SCORE_FILTER.getPreferredName();
        private boolean descending = true;

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            jobId = in.readString();
            excludeInterim = in.readBoolean();
            pageParams = new PageParams(in);
            start = in.readOptionalString();
            end = in.readOptionalString();
            sort = in.readOptionalString();
            descending = in.readBoolean();
            recordScoreFilter = in.readDouble();
        }

        public Request(String jobId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
        }

        public String getJobId() {
            return jobId;
        }

        public String getStart() {
            return start;
        }

        public void setStart(String start) {
            this.start = start;
        }

        public String getEnd() {
            return end;
        }

        public void setEnd(String end) {
            this.end = end;
        }

        public boolean isDescending() {
            return descending;
        }

        public void setDescending(boolean descending) {
            this.descending = descending;
        }

        public boolean isExcludeInterim() {
            return excludeInterim;
        }

        public void setExcludeInterim(boolean excludeInterim) {
            this.excludeInterim = excludeInterim;
        }

        public void setPageParams(PageParams pageParams) {
            this.pageParams = pageParams;
        }

        public PageParams getPageParams() {
            return pageParams;
        }

        public double getRecordScoreFilter() {
            return recordScoreFilter;
        }

        public void setRecordScore(double recordScore) {
            this.recordScoreFilter = recordScore;
        }

        public String getSort() {
            return sort;
        }

        public void setSort(String sort) {
            this.sort = ExceptionsHelper.requireNonNull(sort, SORT.getPreferredName());
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            out.writeBoolean(excludeInterim);
            pageParams.writeTo(out);
            out.writeOptionalString(start);
            out.writeOptionalString(end);
            out.writeOptionalString(sort);
            out.writeBoolean(descending);
            out.writeDouble(recordScoreFilter);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Job.ID.getPreferredName(), jobId);
            builder.field(START.getPreferredName(), start);
            builder.field(END.getPreferredName(), end);
            builder.field(SORT.getPreferredName(), sort);
            builder.field(DESCENDING.getPreferredName(), descending);
            builder.field(RECORD_SCORE_FILTER.getPreferredName(), recordScoreFilter);
            builder.field(EXCLUDE_INTERIM.getPreferredName(), excludeInterim);
            builder.field(PageParams.PAGE.getPreferredName(), pageParams);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, start, end, sort, descending, recordScoreFilter, excludeInterim, pageParams);
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
            return Objects.equals(jobId, other.jobId)
                && Objects.equals(start, other.start)
                && Objects.equals(end, other.end)
                && Objects.equals(sort, other.sort)
                && Objects.equals(descending, other.descending)
                && Objects.equals(recordScoreFilter, other.recordScoreFilter)
                && Objects.equals(excludeInterim, other.excludeInterim)
                && Objects.equals(pageParams, other.pageParams);
        }
    }

    public static class Response extends AbstractGetResourcesResponse<AnomalyRecord> implements ToXContentObject {

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(QueryPage<AnomalyRecord> records) {
            super(records);
        }

        public QueryPage<AnomalyRecord> getRecords() {
            return getResources();
        }

        @Override
        protected Reader<AnomalyRecord> getReader() {
            return AnomalyRecord::new;
        }
    }

}
