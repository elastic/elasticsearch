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
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.action.util.PageParams;
import org.elasticsearch.xpack.ml.action.util.QueryPage;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.persistence.RecordsQueryBuilder;
import org.elasticsearch.xpack.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.ml.job.results.Influencer;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class GetRecordsAction extends Action<GetRecordsAction.Request, GetRecordsAction.Response, GetRecordsAction.RequestBuilder> {

    public static final GetRecordsAction INSTANCE = new GetRecordsAction();
    public static final String NAME = "cluster:admin/ml/results/records/get";

    private GetRecordsAction() {
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
        public static final ParseField ANOMALY_SCORE_FILTER = new ParseField("anomaly_score");
        public static final ParseField SORT = new ParseField("sort");
        public static final ParseField DESCENDING = new ParseField("desc");
        public static final ParseField MAX_NORMALIZED_PROBABILITY = new ParseField("normalized_probability");
        public static final ParseField PARTITION_VALUE = new ParseField("partition_value");

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, jobId) -> request.jobId = jobId, Job.ID);
            PARSER.declareStringOrNull(Request::setStart, START);
            PARSER.declareStringOrNull(Request::setEnd, END);
            PARSER.declareString(Request::setPartitionValue, PARTITION_VALUE);
            PARSER.declareString(Request::setSort, SORT);
            PARSER.declareBoolean(Request::setDecending, DESCENDING);
            PARSER.declareBoolean(Request::setIncludeInterim, INCLUDE_INTERIM);
            PARSER.declareObject(Request::setPageParams, PageParams.PARSER, PageParams.PAGE);
            PARSER.declareDouble(Request::setAnomalyScore, ANOMALY_SCORE_FILTER);
            PARSER.declareDouble(Request::setMaxNormalizedProbability, MAX_NORMALIZED_PROBABILITY);
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
        private double maxNormalizedProbability = 0.0;
        private String partitionValue;

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
            this.sort = ExceptionsHelper.requireNonNull(sort, SORT.getPreferredName());
        }

        public double getMaxNormalizedProbability() {
            return maxNormalizedProbability;
        }

        public void setMaxNormalizedProbability(double maxNormalizedProbability) {
            this.maxNormalizedProbability = maxNormalizedProbability;
        }

        public String getPartitionValue() {
            return partitionValue;
        }

        public void setPartitionValue(String partitionValue) {
            this.partitionValue = ExceptionsHelper.requireNonNull(partitionValue, PARTITION_VALUE.getPreferredName());
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
            maxNormalizedProbability = in.readDouble();
            partitionValue = in.readOptionalString();
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
            out.writeDouble(maxNormalizedProbability);
            out.writeOptionalString(partitionValue);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Job.ID.getPreferredName(), jobId);
            builder.field(START.getPreferredName(), start);
            builder.field(END.getPreferredName(), end);
            builder.field(SORT.getPreferredName(), sort);
            builder.field(DESCENDING.getPreferredName(), decending);
            builder.field(ANOMALY_SCORE_FILTER.getPreferredName(), anomalyScoreFilter);
            builder.field(INCLUDE_INTERIM.getPreferredName(), includeInterim);
            builder.field(MAX_NORMALIZED_PROBABILITY.getPreferredName(), maxNormalizedProbability);
            builder.field(PageParams.PAGE.getPreferredName(), pageParams);
            if (partitionValue != null) {
                builder.field(PARTITION_VALUE.getPreferredName(), partitionValue);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, start, end, sort, decending, anomalyScoreFilter, includeInterim, maxNormalizedProbability,
                    pageParams, partitionValue);
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
            return Objects.equals(jobId, other.jobId) &&
                    Objects.equals(start, other.start) &&
                    Objects.equals(end, other.end) &&
                    Objects.equals(sort, other.sort) &&
                    Objects.equals(decending, other.decending) &&
                    Objects.equals(anomalyScoreFilter, other.anomalyScoreFilter) &&
                    Objects.equals(includeInterim, other.includeInterim) &&
                    Objects.equals(maxNormalizedProbability, other.maxNormalizedProbability) &&
                    Objects.equals(pageParams, other.pageParams) &&
                    Objects.equals(partitionValue, other.partitionValue);
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        RequestBuilder(ElasticsearchClient client) {
            super(client, INSTANCE, new Request());
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private QueryPage<AnomalyRecord> records;

        Response() {
        }

        Response(QueryPage<AnomalyRecord> records) {
            this.records = records;
        }

        public QueryPage<AnomalyRecord> getRecords() {
            return records;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            records = new QueryPage<>(in, AnomalyRecord::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            records.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            records.doXContentBody(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(records);
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
            return Objects.equals(records, other.records);
        }

        @SuppressWarnings("deprecation")
        @Override
        public final String toString() {
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.prettyPrint();
                builder.startObject();
                toXContent(builder, EMPTY_PARAMS);
                builder.endObject();
                return builder.string();
            } catch (Exception e) {
                // So we have a stack trace logged somewhere
                return "{ \"error\" : \"" + org.elasticsearch.ExceptionsHelper.detailedMessage(e) + "\"}";
            }
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final JobProvider jobProvider;

        @Inject
        public TransportAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                JobProvider jobProvider) {
            super(settings, NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, Request::new);
            this.jobProvider = jobProvider;
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            RecordsQueryBuilder.RecordsQuery query = new RecordsQueryBuilder()
                    .includeInterim(request.includeInterim)
                    .epochStart(request.start)
                    .epochEnd(request.end)
                    .from(request.pageParams.getFrom())
                    .size(request.pageParams.getSize())
                    .anomalyScoreThreshold(request.anomalyScoreFilter)
                    .sortField(request.sort)
                    .sortDescending(request.decending)
                    .build();
            jobProvider.records(request.jobId, query, page -> listener.onResponse(new Response(page)), listener::onFailure);
        }
    }

}
