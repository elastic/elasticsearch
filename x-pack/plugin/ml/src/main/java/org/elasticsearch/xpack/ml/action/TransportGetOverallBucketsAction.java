/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.GetOverallBucketsAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.OverallBucket;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.Intervals;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.BucketsQueryBuilder;
import org.elasticsearch.xpack.ml.job.persistence.overallbuckets.OverallBucketsAggregator;
import org.elasticsearch.xpack.ml.job.persistence.overallbuckets.OverallBucketsCollector;
import org.elasticsearch.xpack.ml.job.persistence.overallbuckets.OverallBucketsProcessor;
import org.elasticsearch.xpack.ml.job.persistence.overallbuckets.OverallBucketsProvider;
import org.elasticsearch.xpack.ml.utils.MlIndicesUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportGetOverallBucketsAction extends HandledTransportAction<GetOverallBucketsAction.Request,
        GetOverallBucketsAction.Response> {

    private static final String EARLIEST_TIME = "earliest_time";
    private static final String LATEST_TIME = "latest_time";

    private final ThreadPool threadPool;
    private final Client client;
    private final ClusterService clusterService;
    private final JobManager jobManager;

    @Inject
    public TransportGetOverallBucketsAction(ThreadPool threadPool, TransportService transportService, ActionFilters actionFilters,
                                            ClusterService clusterService, JobManager jobManager, Client client) {
        super(GetOverallBucketsAction.NAME, transportService, actionFilters, GetOverallBucketsAction.Request::new);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.client = client;
        this.jobManager = jobManager;
    }

    @Override
    protected void doExecute(Task task, GetOverallBucketsAction.Request request,
                             ActionListener<GetOverallBucketsAction.Response> listener) {
        jobManager.expandJobs(request.getJobId(), request.allowNoJobs(), ActionListener.wrap(
                jobPage -> {
                    if (jobPage.count() == 0) {
                        listener.onResponse(new GetOverallBucketsAction.Response(
                            new QueryPage<>(Collections.emptyList(), 0, Job.RESULTS_FIELD)));
                        return;
                    }

                    // As computing and potentially aggregating overall buckets might take a while,
                    // we run in a different thread to avoid blocking the network thread.
                    threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> {
                        try {
                            getOverallBuckets(request, jobPage.results(), listener);
                        } catch (Exception e) {
                            listener.onFailure(e);
                        }
                    });
                },
                listener::onFailure
        ));
    }

    private void getOverallBuckets(GetOverallBucketsAction.Request request, List<Job> jobs,
                                   ActionListener<GetOverallBucketsAction.Response> listener) {
        JobsContext jobsContext = JobsContext.build(jobs, request);

        ActionListener<List<OverallBucket>> overallBucketsListener = ActionListener.wrap(overallBuckets -> {
            listener.onResponse(new GetOverallBucketsAction.Response(new QueryPage<>(overallBuckets, overallBuckets.size(),
                    OverallBucket.RESULTS_FIELD)));
        }, listener::onFailure);

        ActionListener<ChunkedBucketSearcher> chunkedBucketSearcherListener = ActionListener.wrap(searcher -> {
            if (searcher == null) {
                listener.onResponse(new GetOverallBucketsAction.Response(new QueryPage<>(Collections.emptyList(), 0, Job.RESULTS_FIELD)));
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

    private static boolean requiresAggregation(GetOverallBucketsAction.Request request, TimeValue maxBucketSpan) {
        return request.getBucketSpan() != null && !request.getBucketSpan().equals(maxBucketSpan);
    }

    private static void checkValidBucketSpan(TimeValue bucketSpan, TimeValue maxBucketSpan) {
        if (bucketSpan != null && bucketSpan.compareTo(maxBucketSpan) < 0) {
            throw ExceptionsHelper.badRequestException("Param [{}] must be greater or equal to the max bucket_span [{}]",
                    GetOverallBucketsAction.Request.BUCKET_SPAN, maxBucketSpan.getStringRep());
        }
    }

    private void initChunkedBucketSearcher(GetOverallBucketsAction.Request request, JobsContext jobsContext,
                                           OverallBucketsProvider overallBucketsProvider,
                                           OverallBucketsProcessor overallBucketsProcessor,
                                           ActionListener<ChunkedBucketSearcher> listener) {
        long maxBucketSpanMillis = jobsContext.maxBucketSpan.millis();
        SearchRequest searchRequest = buildSearchRequest(request.getStart(), request.getEnd(), request.isExcludeInterim(),
                maxBucketSpanMillis, jobsContext.indices);
        searchRequest.source().aggregation(AggregationBuilders.min(EARLIEST_TIME).field(Result.TIMESTAMP.getPreferredName()));
        searchRequest.source().aggregation(AggregationBuilders.max(LATEST_TIME).field(Result.TIMESTAMP.getPreferredName()));
        executeAsyncWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN, searchRequest,
                ActionListener.<SearchResponse>wrap(searchResponse -> {
                    long totalHits = searchResponse.getHits().getTotalHits().value;
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
                }, listener::onFailure),
                client::search);
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

        private static JobsContext build(List<Job> jobs, GetOverallBucketsAction.Request request) {
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
            executeAsyncWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN, nextSearch(),
                    ActionListener.<SearchResponse>wrap(searchResponse -> {
                        Histogram histogram = searchResponse.getAggregations().get(Result.TIMESTAMP.getPreferredName());
                        overallBucketsProcessor.process(overallBucketsProvider.computeOverallBuckets(histogram));
                        if (overallBucketsProcessor.size() > MAX_RESULT_COUNT) {
                            listener.onFailure(
                                    ExceptionsHelper.badRequestException("Unable to return more than [{}] results; please use " +
                                    "parameters [{}] and [{}] to limit the time range", MAX_RESULT_COUNT,
                                            GetOverallBucketsAction.Request.START, GetOverallBucketsAction.Request.END));
                            return;
                        }
                        searchAndComputeOverallBuckets(listener);
                    }, listener::onFailure),
                    client::search);
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
        searchSourceBuilder.trackTotalHits(true);

        SearchRequest searchRequest = new SearchRequest(indices);
        searchRequest.indicesOptions(MlIndicesUtils.addIgnoreUnavailable(SearchRequest.DEFAULT_INDICES_OPTIONS));
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
                .fixedInterval(new DateHistogramInterval(maxBucketSpanMillis + "ms"))
                .subAggregation(jobsAgg)
                .subAggregation(interimAgg);
    }
}
