/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.action.util.QueryPage;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.job.persistence.BucketsQueryBuilder;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.job.results.OverallBucket;
import org.elasticsearch.xpack.ml.job.results.Result;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.utils.Intervals;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * This action returns summarized bucket results over multiple jobs.
 * Overall buckets have the span of the largest job's bucket_span.
 * Their score is calculated by finding the max anomaly score per job
 * and then averaging the top N.
 */
public class GetOverallBucketsAction
        extends Action<GetOverallBucketsAction.Request, GetOverallBucketsAction.Response, GetOverallBucketsAction.RequestBuilder> {

    public static final GetOverallBucketsAction INSTANCE = new GetOverallBucketsAction();
    public static final String NAME = "cluster:monitor/xpack/ml/job/results/overall_buckets/get";

    private GetOverallBucketsAction() {
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

    public static class Request extends ActionRequest implements ToXContentObject {

        public static final ParseField TOP_N = new ParseField("top_n");
        public static final ParseField OVERALL_SCORE = new ParseField("overall_score");
        public static final ParseField EXCLUDE_INTERIM = new ParseField("exclude_interim");
        public static final ParseField START = new ParseField("start");
        public static final ParseField END = new ParseField("end");
        public static final ParseField ALLOW_NO_JOBS = new ParseField("allow_no_jobs");

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, jobId) -> request.jobId = jobId, Job.ID);
            PARSER.declareInt(Request::setTopN, TOP_N);
            PARSER.declareDouble(Request::setOverallScore, OVERALL_SCORE);
            PARSER.declareBoolean(Request::setExcludeInterim, EXCLUDE_INTERIM);
            PARSER.declareString((request, startTime) -> request.setStart(parseDateOrThrow(
                    startTime, START, System::currentTimeMillis)), START);
            PARSER.declareString((request, endTime) -> request.setEnd(parseDateOrThrow(
                    endTime, END, System::currentTimeMillis)), END);
            PARSER.declareBoolean(Request::setAllowNoJobs, ALLOW_NO_JOBS);
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

        public static Request parseRequest(String jobId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (jobId != null) {
                request.jobId = jobId;
            }
            return request;
        }

        private String jobId;
        private int topN = 1;
        private double overallScore = 0.0;
        private boolean excludeInterim = false;
        private Long start;
        private Long end;
        private boolean allowNoJobs = true;

        Request() {
        }

        public Request(String jobId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
        }

        public String getJobId() {
            return jobId;
        }

        public int getTopN() {
            return topN;
        }

        public void setTopN(int topN) {
            if (topN <= 0) {
                throw new IllegalArgumentException("[topN] parameter must be positive, found [" + topN + "]");
            }
            this.topN = topN;
        }

        public double getOverallScore() {
            return overallScore;
        }

        public void setOverallScore(double overallScore) {
            this.overallScore = overallScore;
        }

        public boolean isExcludeInterim() {
            return excludeInterim;
        }

        public void setExcludeInterim(boolean excludeInterim) {
            this.excludeInterim = excludeInterim;
        }

        public Long getStart() {
            return start;
        }

        public void setStart(Long start) {
            this.start = start;
        }

        public void setStart(String start) {
            setStart(parseDateOrThrow(start, START, System::currentTimeMillis));
        }

        public Long getEnd() {
            return end;
        }

        public void setEnd(Long end) {
            this.end = end;
        }

        public void setEnd(String end) {
            setEnd(parseDateOrThrow(end, END, System::currentTimeMillis));
        }

        public boolean allowNoJobs() {
            return allowNoJobs;
        }

        public void setAllowNoJobs(boolean allowNoJobs) {
            this.allowNoJobs = allowNoJobs;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            jobId = in.readString();
            topN = in.readVInt();
            overallScore = in.readDouble();
            excludeInterim = in.readBoolean();
            start = in.readOptionalLong();
            end = in.readOptionalLong();
            allowNoJobs = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            out.writeVInt(topN);
            out.writeDouble(overallScore);
            out.writeBoolean(excludeInterim);
            out.writeOptionalLong(start);
            out.writeOptionalLong(end);
            out.writeBoolean(allowNoJobs);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Job.ID.getPreferredName(), jobId);
            builder.field(TOP_N.getPreferredName(), topN);
            builder.field(OVERALL_SCORE.getPreferredName(), overallScore);
            builder.field(EXCLUDE_INTERIM.getPreferredName(), excludeInterim);
            if (start != null) {
                builder.field(START.getPreferredName(), String.valueOf(start));
            }
            if (end != null) {
                builder.field(END.getPreferredName(), String.valueOf(end));
            }
            builder.field(ALLOW_NO_JOBS.getPreferredName(), allowNoJobs);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, topN, overallScore, excludeInterim, start, end, allowNoJobs);
        }

        @Override
        public boolean equals(Object other) {
            if (other == null) {
                return false;
            }
            if (getClass() != other.getClass()) {
                return false;
            }
            Request that = (Request) other;
            return Objects.equals(jobId, that.jobId) &&
                    this.topN == that.topN &&
                    this.excludeInterim == that.excludeInterim &&
                    this.overallScore == that.overallScore &&
                    Objects.equals(start, that.start) &&
                    Objects.equals(end, that.end) &&
                    this.allowNoJobs == that.allowNoJobs;
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        RequestBuilder(ElasticsearchClient client) {
            super(client, INSTANCE, new Request());
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private QueryPage<OverallBucket> overallBuckets;

        Response() {
            overallBuckets = new QueryPage<>(Collections.emptyList(), 0, OverallBucket.RESULTS_FIELD);
        }

        Response(QueryPage<OverallBucket> overallBuckets) {
            this.overallBuckets = overallBuckets;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            overallBuckets = new QueryPage<>(in, OverallBucket::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            overallBuckets.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            overallBuckets.doXContentBody(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(overallBuckets);
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
            return Objects.equals(overallBuckets, other.overallBuckets);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final Client client;
        private final ClusterService clusterService;
        private final JobManager jobManager;

        @Inject
        public TransportAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               ClusterService clusterService, JobManager jobManager, Client client) {
            super(settings, NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, Request::new);
            this.clusterService = clusterService;
            this.client = client;
            this.jobManager = jobManager;
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            QueryPage<Job> jobsPage = jobManager.expandJobs(request.getJobId(), request.allowNoJobs(), clusterService.state());
            if (jobsPage.count() == 0) {
                listener.onResponse(new Response());
                return;
            }

            List<String> indices = new ArrayList<>();
            TimeValue maxBucketSpan = TimeValue.ZERO;
            for (Job job : jobsPage.results()) {
                indices.add(AnomalyDetectorsIndex.jobResultsAliasedName(job.getId()));
                TimeValue bucketSpan = job.getAnalysisConfig().getBucketSpan();
                if (maxBucketSpan.compareTo(bucketSpan) < 0) {
                    maxBucketSpan = bucketSpan;
                }
            }
            final long maxBucketSpanSeconds = maxBucketSpan.seconds();

            SearchRequest searchRequest = buildSearchRequest(request, maxBucketSpan.millis(), indices);
            client.search(searchRequest, ActionListener.wrap(searchResponse -> {
                List<OverallBucket> overallBuckets = computeOverallBuckets(request, maxBucketSpanSeconds, searchResponse);
                listener.onResponse(new Response(new QueryPage<>(overallBuckets, overallBuckets.size(), OverallBucket.RESULTS_FIELD)));
            }, listener::onFailure));
        }

        private static SearchRequest buildSearchRequest(Request request, long bucketSpanMillis, List<String> indices) {
            String startTime = request.getStart() == null ? null : String.valueOf(
                    Intervals.alignToCeil(request.getStart(), bucketSpanMillis));
            String endTime = request.getEnd() == null ? null : String.valueOf(Intervals.alignToFloor(request.getEnd(), bucketSpanMillis));

            SearchSourceBuilder searchSourceBuilder = new BucketsQueryBuilder()
                    .size(0)
                    .includeInterim(request.isExcludeInterim() == false)
                    .start(startTime)
                    .end(endTime)
                    .build();
            searchSourceBuilder.aggregation(buildAggregations(bucketSpanMillis));

            SearchRequest searchRequest = new SearchRequest(indices.toArray(new String[indices.size()]));
            searchRequest.indicesOptions(JobProvider.addIgnoreUnavailable(SearchRequest.DEFAULT_INDICES_OPTIONS));
            searchRequest.source(searchSourceBuilder);
            return searchRequest;
        }

        private static AggregationBuilder buildAggregations(long bucketSpanMillis) {
            AggregationBuilder overallScoreAgg = AggregationBuilders.max(OverallBucket.OVERALL_SCORE.getPreferredName())
                    .field(Bucket.ANOMALY_SCORE.getPreferredName());
            AggregationBuilder jobsAgg = AggregationBuilders.terms(Job.ID.getPreferredName())
                    .field(Job.ID.getPreferredName()).subAggregation(overallScoreAgg);
            AggregationBuilder interimAgg = AggregationBuilders.max(Result.IS_INTERIM.getPreferredName())
                    .field(Result.IS_INTERIM.getPreferredName());
            return AggregationBuilders.dateHistogram(Result.TIMESTAMP.getPreferredName())
                    .field(Result.TIMESTAMP.getPreferredName())
                    .interval(bucketSpanMillis)
                    .subAggregation(jobsAgg)
                    .subAggregation(interimAgg);
        }

        private List<OverallBucket> computeOverallBuckets(Request request, long bucketSpanSeconds, SearchResponse searchResponse) {
            List<OverallBucket> overallBuckets = new ArrayList<>();
            Histogram histogram = searchResponse.getAggregations().get(Result.TIMESTAMP.getPreferredName());
            for (Histogram.Bucket histogramBucket : histogram.getBuckets()) {
                Aggregations histogramBucketAggs = histogramBucket.getAggregations();
                Terms jobsAgg = histogramBucketAggs.get(Job.ID.getPreferredName());
                int jobsCount = jobsAgg.getBuckets().size();
                int topN = Math.min(request.getTopN(), jobsCount);
                List<OverallBucket.JobInfo> jobs = new ArrayList<>(jobsCount);
                TopNScores topNScores = new TopNScores(topN);
                for (Terms.Bucket jobsBucket : jobsAgg.getBuckets()) {
                    Max maxScore = jobsBucket.getAggregations().get(OverallBucket.OVERALL_SCORE.getPreferredName());
                    topNScores.insertWithOverflow(maxScore.getValue());
                    jobs.add(new OverallBucket.JobInfo((String) jobsBucket.getKey(), maxScore.getValue()));
                }

                double overallScore = topNScores.overallScore();
                if (overallScore < request.getOverallScore()) {
                    continue;
                }

                Max interimAgg = histogramBucketAggs.get(Result.IS_INTERIM.getPreferredName());
                boolean isInterim = interimAgg.getValue() > 0;
                if (request.isExcludeInterim() && isInterim) {
                    continue;
                }

                overallBuckets.add(new OverallBucket(getHistogramBucketTimestamp(histogramBucket),
                        bucketSpanSeconds, overallScore, jobs, isInterim));
            }
            return overallBuckets;
        }

        private static Date getHistogramBucketTimestamp(Histogram.Bucket bucket) {
            DateTime bucketTimestamp = (DateTime) bucket.getKey();
            return new Date(bucketTimestamp.getMillis());
        }

        static class TopNScores extends PriorityQueue<Double> {

            TopNScores(int n) {
                super(n, false);
            }

            @Override
            protected boolean lessThan(Double a, Double b) {
                return a < b;
            }

            double overallScore() {
                double overallScore = 0.0;
                for (double score : this) {
                    overallScore += score;
                }
                return size() > 0 ? overallScore / size() : 0.0;
            }
        }
    }
}
