/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchRequest;
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
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.action.util.QueryPage;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.job.persistence.BucketsQueryBuilder;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.persistence.overallbuckets.OverallBucketsAggregator;
import org.elasticsearch.xpack.ml.job.persistence.overallbuckets.OverallBucketsCollector;
import org.elasticsearch.xpack.ml.job.persistence.overallbuckets.OverallBucketsProcessor;
import org.elasticsearch.xpack.ml.job.persistence.overallbuckets.OverallBucketsProvider;
import org.elasticsearch.xpack.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.job.results.OverallBucket;
import org.elasticsearch.xpack.ml.job.results.Result;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.utils.Intervals;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * <p>
 * This action returns summarized bucket results over multiple jobs.
 * Overall buckets have the span of the largest job's bucket_span.
 * Their score is calculated by finding the max anomaly score per job
 * and then averaging the top N.
 * </p>
 * <p>
 * Overall buckets can be optionally aggregated into larger intervals
 * by setting the bucket_span parameter. When that is the case, the
 * overall_score is the max of the overall buckets that are within
 * the interval.
 * </p>
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
        public static final ParseField BUCKET_SPAN = new ParseField("bucket_span");
        public static final ParseField OVERALL_SCORE = new ParseField("overall_score");
        public static final ParseField EXCLUDE_INTERIM = new ParseField("exclude_interim");
        public static final ParseField START = new ParseField("start");
        public static final ParseField END = new ParseField("end");
        public static final ParseField ALLOW_NO_JOBS = new ParseField("allow_no_jobs");

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, jobId) -> request.jobId = jobId, Job.ID);
            PARSER.declareInt(Request::setTopN, TOP_N);
            PARSER.declareString(Request::setBucketSpan, BUCKET_SPAN);
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
        private TimeValue bucketSpan;
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

        public TimeValue getBucketSpan() {
            return bucketSpan;
        }

        public void setBucketSpan(TimeValue bucketSpan) {
            this.bucketSpan = bucketSpan;
        }

        public void setBucketSpan(String bucketSpan) {
            this.bucketSpan = TimeValue.parseTimeValue(bucketSpan, BUCKET_SPAN.getPreferredName());
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
            bucketSpan = in.readOptionalWriteable(TimeValue::new);
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
            out.writeOptionalWriteable(bucketSpan);
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
            if (bucketSpan != null) {
                builder.field(BUCKET_SPAN.getPreferredName(), bucketSpan.getStringRep());
            }
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
            return Objects.hash(jobId, topN, bucketSpan, overallScore, excludeInterim, start, end, allowNoJobs);
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
                    Objects.equals(bucketSpan, that.bucketSpan) &&
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

        public QueryPage<OverallBucket> getOverallBuckets() {
            return overallBuckets;
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

        private static final String EARLIEST_TIME = "earliest_time";
        private static final String LATEST_TIME = "latest_time";

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

            // As computing and potentially aggregating overall buckets might take a while,
            // we run in a different thread to avoid blocking the network thread.
            threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> {
                try {
                    getOverallBuckets(request, jobsPage.results(), listener);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            });
        }

        private void getOverallBuckets(Request request, List<Job> jobs, ActionListener<Response> listener) {
            JobsContext jobsContext = JobsContext.build(jobs, request);

            ActionListener<List<OverallBucket>> overallBucketsListener = ActionListener.wrap(overallBuckets -> {
                listener.onResponse(new Response(new QueryPage<>(overallBuckets, overallBuckets.size(), OverallBucket.RESULTS_FIELD)));
            }, listener::onFailure);

            ActionListener<ChunkedBucketSearcher> chunkedBucketSearcherListener = ActionListener.wrap(searcher -> {
                if (searcher == null) {
                    listener.onResponse(new Response());
                    return;
                }
                searcher.searchAndComputeOverallBuckets(overallBucketsListener);
            }, listener::onFailure);

            OverallBucketsProvider overallBucketsProvider = new OverallBucketsProvider(jobsContext.maxBucketSpan, request.getTopN(),
                    request.getOverallScore());
            OverallBucketsProcessor overallBucketsProcessor = requiresAggregation(request, jobsContext.maxBucketSpan) ?
                    new OverallBucketsAggregator(request.getBucketSpan()): new OverallBucketsCollector();
            initChunkedBucketSearcher(request, jobsContext, overallBucketsProvider, overallBucketsProcessor, chunkedBucketSearcherListener);
        }

        private static boolean requiresAggregation(Request request, TimeValue maxBucketSpan) {
            return request.getBucketSpan() != null && !request.getBucketSpan().equals(maxBucketSpan);
        }

        private static void checkValidBucketSpan(TimeValue bucketSpan, TimeValue maxBucketSpan) {
            if (bucketSpan != null && bucketSpan.compareTo(maxBucketSpan) < 0) {
                throw ExceptionsHelper.badRequestException("Param [{}] must be greater or equal to the max bucket_span [{}]",
                        Request.BUCKET_SPAN, maxBucketSpan.getStringRep());
            }
        }

        private void initChunkedBucketSearcher(Request request, JobsContext jobsContext, OverallBucketsProvider overallBucketsProvider,
                                               OverallBucketsProcessor overallBucketsProcessor,
                                               ActionListener<ChunkedBucketSearcher> listener) {
            long maxBucketSpanMillis = jobsContext.maxBucketSpan.millis();
            SearchRequest searchRequest = buildSearchRequest(request.getStart(), request.getEnd(), request.isExcludeInterim(),
                    maxBucketSpanMillis, jobsContext.indices);
            searchRequest.source().aggregation(AggregationBuilders.min(EARLIEST_TIME).field(Result.TIMESTAMP.getPreferredName()));
            searchRequest.source().aggregation(AggregationBuilders.max(LATEST_TIME).field(Result.TIMESTAMP.getPreferredName()));
            client.search(searchRequest, ActionListener.wrap(searchResponse -> {
                long totalHits = searchResponse.getHits().getTotalHits();
                if (totalHits > 0) {
                    Aggregations aggregations = searchResponse.getAggregations();
                    Min min = aggregations.get(EARLIEST_TIME);
                    long earliestTime = Intervals.alignToFloor((long) min.getValue(), maxBucketSpanMillis);
                    Max max = aggregations.get(LATEST_TIME);
                    long latestTime = Intervals.alignToCeil((long) max.getValue() + 1, maxBucketSpanMillis);
                    listener.onResponse(new ChunkedBucketSearcher(jobsContext, earliestTime, latestTime, request.isExcludeInterim(),
                            overallBucketsProvider, overallBucketsProcessor));
                } else {
                    listener.onResponse(null);
                }
            }, listener::onFailure));
        }

        private static class JobsContext {
            private final int jobCount;
            private final String[] indices;
            private final TimeValue maxBucketSpan;

            private JobsContext(int jobCount, String[] indices, TimeValue maxBucketSpan) {
                this.jobCount = jobCount;
                this.indices = indices;
                this.maxBucketSpan = maxBucketSpan;
            }

            private static JobsContext build(List<Job> jobs, Request request) {
                Set<String> indices = new HashSet<>();
                TimeValue maxBucketSpan = TimeValue.ZERO;
                for (Job job : jobs) {
                    indices.add(AnomalyDetectorsIndex.jobResultsAliasedName(job.getId()));
                    TimeValue bucketSpan = job.getAnalysisConfig().getBucketSpan();
                    if (maxBucketSpan.compareTo(bucketSpan) < 0) {
                        maxBucketSpan = bucketSpan;
                    }
                }
                checkValidBucketSpan(request.getBucketSpan(), maxBucketSpan);

                // If top_n is 1, we can use the request bucket_span in order to optimize the aggregations
                if (request.getBucketSpan() != null && (request.getTopN() == 1 || jobs.size() <= 1)) {
                    maxBucketSpan = request.getBucketSpan();
                }

                return new JobsContext(jobs.size(), indices.toArray(new String[indices.size()]), maxBucketSpan);
            }
        }

        private class ChunkedBucketSearcher {

            private static final int BUCKETS_PER_CHUNK = 1000;
            private static final int MAX_RESULT_COUNT = 10000;

            private final String[] indices;
            private final long maxBucketSpanMillis;
            private final boolean excludeInterim;
            private final long chunkMillis;
            private final long endTime;
            private volatile long curTime;
            private final AggregationBuilder aggs;
            private final OverallBucketsProvider overallBucketsProvider;
            private final OverallBucketsProcessor overallBucketsProcessor;

            ChunkedBucketSearcher(JobsContext jobsContext, long startTime, long endTime,
                                  boolean excludeInterim, OverallBucketsProvider overallBucketsProvider,
                                  OverallBucketsProcessor overallBucketsProcessor) {
                this.indices = jobsContext.indices;
                this.maxBucketSpanMillis = jobsContext.maxBucketSpan.millis();
                this.chunkMillis = BUCKETS_PER_CHUNK * maxBucketSpanMillis;
                this.endTime = endTime;
                this.curTime = startTime;
                this.excludeInterim = excludeInterim;
                this.aggs = buildAggregations(maxBucketSpanMillis, jobsContext.jobCount);
                this.overallBucketsProvider = overallBucketsProvider;
                this.overallBucketsProcessor = overallBucketsProcessor;
            }

            void searchAndComputeOverallBuckets(ActionListener<List<OverallBucket>> listener) {
                if (curTime >= endTime) {
                    listener.onResponse(overallBucketsProcessor.finish());
                    return;
                }
                client.search(nextSearch(), ActionListener.wrap(searchResponse -> {
                    Histogram histogram = searchResponse.getAggregations().get(Result.TIMESTAMP.getPreferredName());
                    overallBucketsProcessor.process(overallBucketsProvider.computeOverallBuckets(histogram));
                    if (overallBucketsProcessor.size() > MAX_RESULT_COUNT) {
                        listener.onFailure(ExceptionsHelper.badRequestException("Unable to return more than [{}] results; please use " +
                                "parameters [{}] and [{}] to limit the time range", MAX_RESULT_COUNT, Request.START, Request.END));
                        return;
                    }
                    searchAndComputeOverallBuckets(listener);
                }, listener::onFailure));
            }

            SearchRequest nextSearch() {
                long curEnd = Math.min(curTime + chunkMillis, endTime);
                logger.debug("Search for buckets in: [{}, {})", curTime, curEnd);
                SearchRequest searchRequest = buildSearchRequest(curTime, curEnd, excludeInterim, maxBucketSpanMillis, indices);
                searchRequest.source().aggregation(aggs);
                curTime += chunkMillis;
                return searchRequest;
            }
        }

        private static SearchRequest buildSearchRequest(Long start, Long end, boolean excludeInterim, long bucketSpanMillis,
                                                        String[] indices) {
            String startTime = start == null ? null : String.valueOf(Intervals.alignToCeil(start, bucketSpanMillis));
            String endTime = end == null ? null : String.valueOf(Intervals.alignToFloor(end, bucketSpanMillis));

            SearchSourceBuilder searchSourceBuilder = new BucketsQueryBuilder()
                    .size(0)
                    .includeInterim(excludeInterim == false)
                    .start(startTime)
                    .end(endTime)
                    .build();

            SearchRequest searchRequest = new SearchRequest(indices);
            searchRequest.indicesOptions(JobProvider.addIgnoreUnavailable(SearchRequest.DEFAULT_INDICES_OPTIONS));
            searchRequest.source(searchSourceBuilder);
            return searchRequest;
        }

        private static AggregationBuilder buildAggregations(long maxBucketSpanMillis, int jobCount) {
            AggregationBuilder overallScoreAgg = AggregationBuilders.max(OverallBucket.OVERALL_SCORE.getPreferredName())
                    .field(Bucket.ANOMALY_SCORE.getPreferredName());
            AggregationBuilder jobsAgg = AggregationBuilders.terms(Job.ID.getPreferredName())
                    .field(Job.ID.getPreferredName()).size(jobCount).subAggregation(overallScoreAgg);
            AggregationBuilder interimAgg = AggregationBuilders.max(Result.IS_INTERIM.getPreferredName())
                    .field(Result.IS_INTERIM.getPreferredName());
            return AggregationBuilders.dateHistogram(Result.TIMESTAMP.getPreferredName())
                    .field(Result.TIMESTAMP.getPreferredName())
                    .interval(maxBucketSpanMillis)
                    .subAggregation(jobsAgg)
                    .subAggregation(interimAgg);
        }
    }
}
