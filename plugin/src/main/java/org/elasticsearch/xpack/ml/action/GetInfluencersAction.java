/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.action.util.PageParams;
import org.elasticsearch.xpack.ml.action.util.QueryPage;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.persistence.InfluencersQueryBuilder;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.results.Influencer;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class GetInfluencersAction
extends Action<GetInfluencersAction.Request, GetInfluencersAction.Response, GetInfluencersAction.RequestBuilder> {

    public static final GetInfluencersAction INSTANCE = new GetInfluencersAction();
    public static final String NAME = "cluster:monitor/ml/anomaly_detectors/results/influencers/get";

    private GetInfluencersAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends ActionRequest implements ToXContent {

        public static final ParseField START = new ParseField("start");
        public static final ParseField END = new ParseField("end");
        public static final ParseField INCLUDE_INTERIM = new ParseField("include_interim");
        public static final ParseField ANOMALY_SCORE = new ParseField("anomaly_score");
        public static final ParseField SORT_FIELD = new ParseField("sort");
        public static final ParseField DESCENDING_SORT = new ParseField("desc");

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, jobId) -> request.jobId = jobId, Job.ID);
            PARSER.declareStringOrNull(Request::setStart, START);
            PARSER.declareStringOrNull(Request::setEnd, END);
            PARSER.declareBoolean(Request::setIncludeInterim, INCLUDE_INTERIM);
            PARSER.declareObject(Request::setPageParams, PageParams.PARSER, PageParams.PAGE);
            PARSER.declareDouble(Request::setAnomalyScore, ANOMALY_SCORE);
            PARSER.declareString(Request::setSort, SORT_FIELD);
            PARSER.declareBoolean(Request::setDecending, DESCENDING_SORT);
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
        private boolean includeInterim = false;
        private PageParams pageParams = new PageParams();
        private double anomalyScoreFilter = 0.0;
        private String sort = Influencer.ANOMALY_SCORE.getPreferredName();
        private boolean decending = false;

        Request() {
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

        public boolean isDecending() {
            return decending;
        }

        public void setDecending(boolean decending) {
            this.decending = decending;
        }

        public boolean isIncludeInterim() {
            return includeInterim;
        }

        public void setIncludeInterim(boolean includeInterim) {
            this.includeInterim = includeInterim;
        }

        public void setPageParams(PageParams pageParams) {
            this.pageParams = pageParams;
        }

        public PageParams getPageParams() {
            return pageParams;
        }

        public double getAnomalyScoreFilter() {
            return anomalyScoreFilter;
        }

        public void setAnomalyScore(double anomalyScoreFilter) {
            this.anomalyScoreFilter = anomalyScoreFilter;
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
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            jobId = in.readString();
            includeInterim = in.readBoolean();
            pageParams = new PageParams(in);
            start = in.readOptionalString();
            end = in.readOptionalString();
            sort = in.readOptionalString();
            decending = in.readBoolean();
            anomalyScoreFilter = in.readDouble();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            out.writeBoolean(includeInterim);
            pageParams.writeTo(out);
            out.writeOptionalString(start);
            out.writeOptionalString(end);
            out.writeOptionalString(sort);
            out.writeBoolean(decending);
            out.writeDouble(anomalyScoreFilter);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Job.ID.getPreferredName(), jobId);
            builder.field(INCLUDE_INTERIM.getPreferredName(), includeInterim);
            builder.field(PageParams.PAGE.getPreferredName(), pageParams);
            builder.field(START.getPreferredName(), start);
            builder.field(END.getPreferredName(), end);
            builder.field(SORT_FIELD.getPreferredName(), sort);
            builder.field(DESCENDING_SORT.getPreferredName(), decending);
            builder.field(ANOMALY_SCORE.getPreferredName(), anomalyScoreFilter);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, includeInterim, pageParams, start, end, sort, decending, anomalyScoreFilter);
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
            return Objects.equals(jobId, other.jobId) && Objects.equals(start, other.start) && Objects.equals(end, other.end)
                    && Objects.equals(includeInterim, other.includeInterim) && Objects.equals(pageParams, other.pageParams)
                    && Objects.equals(anomalyScoreFilter, other.anomalyScoreFilter) && Objects.equals(decending, other.decending)
                    && Objects.equals(sort, other.sort);
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        RequestBuilder(ElasticsearchClient client) {
            super(client, INSTANCE, new Request());
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private QueryPage<Influencer> influencers;

        Response() {
        }

        Response(QueryPage<Influencer> influencers) {
            this.influencers = influencers;
        }

        public QueryPage<Influencer> getInfluencers() {
            return influencers;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            influencers = new QueryPage<>(in, Influencer::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            influencers.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            influencers.doXContentBody(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(influencers);
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
            return Objects.equals(influencers, other.influencers);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final JobProvider jobProvider;
        private final Client client;

        @Inject
        public TransportAction(Settings settings, ThreadPool threadPool, TransportService transportService, ActionFilters actionFilters,
                IndexNameExpressionResolver indexNameExpressionResolver, JobProvider jobProvider, Client client) {
            super(settings, NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, Request::new);
            this.jobProvider = jobProvider;
            this.client = client;
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            InfluencersQueryBuilder.InfluencersQuery query = new InfluencersQueryBuilder().includeInterim(request.includeInterim)
                    .start(request.start).end(request.end).from(request.pageParams.getFrom()).size(request.pageParams.getSize())
                    .anomalyScoreThreshold(request.anomalyScoreFilter).sortField(request.sort).sortDescending(request.decending).build();
            jobProvider.influencers(request.jobId, query, page -> listener.onResponse(new Response(page)), listener::onFailure, client);
        }
    }

}
