/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesResponse;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.Influencer;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class GetInfluencersAction extends ActionType<GetInfluencersAction.Response> {

    public static final GetInfluencersAction INSTANCE = new GetInfluencersAction();
    public static final String NAME = "cluster:monitor/xpack/ml/job/results/influencers/get";

    private GetInfluencersAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest implements ToXContentObject {

        public static final ParseField START = new ParseField("start");
        public static final ParseField END = new ParseField("end");
        public static final ParseField EXCLUDE_INTERIM = new ParseField("exclude_interim");
        public static final ParseField INFLUENCER_SCORE = new ParseField("influencer_score");
        public static final ParseField SORT_FIELD = new ParseField("sort");
        public static final ParseField DESCENDING_SORT = new ParseField("desc");

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, jobId) -> request.jobId = jobId, Job.ID);
            PARSER.declareStringOrNull(Request::setStart, START);
            PARSER.declareStringOrNull(Request::setEnd, END);
            PARSER.declareBoolean(Request::setExcludeInterim, EXCLUDE_INTERIM);
            PARSER.declareObject(Request::setPageParams, PageParams.PARSER, PageParams.PAGE);
            PARSER.declareDouble(Request::setInfluencerScore, INFLUENCER_SCORE);
            PARSER.declareString(Request::setSort, SORT_FIELD);
            PARSER.declareBoolean(Request::setDescending, DESCENDING_SORT);
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
        private double influencerScore = 0.0;
        private String sort = Influencer.INFLUENCER_SCORE.getPreferredName();
        private boolean descending = true;

        public Request() {
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            jobId = in.readString();
            excludeInterim = in.readBoolean();
            pageParams = new PageParams(in);
            start = in.readOptionalString();
            end = in.readOptionalString();
            sort = in.readOptionalString();
            descending = in.readBoolean();
            influencerScore = in.readDouble();
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

        public double getInfluencerScore() {
            return influencerScore;
        }

        public void setInfluencerScore(double anomalyScoreFilter) {
            this.influencerScore = anomalyScoreFilter;
        }

        public String getSort() {
            return sort;
        }

        public void setSort(String sort) {
            this.sort = ExceptionsHelper.requireNonNull(sort, SORT_FIELD.getPreferredName());
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
            out.writeDouble(influencerScore);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Job.ID.getPreferredName(), jobId);
            builder.field(EXCLUDE_INTERIM.getPreferredName(), excludeInterim);
            builder.field(PageParams.PAGE.getPreferredName(), pageParams);
            builder.field(START.getPreferredName(), start);
            builder.field(END.getPreferredName(), end);
            builder.field(SORT_FIELD.getPreferredName(), sort);
            builder.field(DESCENDING_SORT.getPreferredName(), descending);
            builder.field(INFLUENCER_SCORE.getPreferredName(), influencerScore);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, excludeInterim, pageParams, start, end, sort, descending, influencerScore);
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
            return Objects.equals(jobId, other.jobId) && Objects.equals(start, other.start)
                    && Objects.equals(end, other.end)
                    && Objects.equals(excludeInterim, other.excludeInterim)
                    && Objects.equals(pageParams, other.pageParams)
                    && Objects.equals(influencerScore, other.influencerScore)
                    && Objects.equals(descending, other.descending)
                    && Objects.equals(sort, other.sort);
        }
    }

    public static class Response extends AbstractGetResourcesResponse<Influencer> implements ToXContentObject {

        public Response() {
        }

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(QueryPage<Influencer> influencers) {
            super(influencers);
        }

        @Override
        protected Reader<Influencer> getReader() {
            return Influencer::new;
        }

        public QueryPage<Influencer> getInfluencers() {
            return getResources();
        }
    }

}
