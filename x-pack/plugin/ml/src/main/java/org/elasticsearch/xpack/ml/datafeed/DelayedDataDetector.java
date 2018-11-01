package org.elasticsearch.xpack.ml.datafeed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.ml.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.ExtractorUtils;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.Intervals;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * This class will search the buckets and indices over a given window to determine if any data is missing
 */
public class DelayedDataDetector {
    
    private static final String DATE_BUCKETS = "date_buckets";
    private final long bucketSpan;
    private final long window;
    private final DatafeedConfig datafeedConfig;
    private final Client client;
    private final Job job;

    public DelayedDataDetector(Job job, DatafeedConfig datafeedConfig, TimeValue window, Client client) {
        this.job = job;
        this.bucketSpan = job.getAnalysisConfig().getBucketSpan().millis();
        this.datafeedConfig = datafeedConfig;
        long windowMillis = window.millis();
        if (windowMillis < bucketSpan) {
            throw new IllegalArgumentException("[window] must be greater or equal to the [bucket_span]");
        }
        this.window = windowMillis;
        this.client = client;
    }

    public void missingData(long latestFinalizedBucket, ActionListener<Long> listener) {
        final long end = Intervals.alignToFloor(latestFinalizedBucket, bucketSpan);
        final long start = Intervals.alignToFloor(latestFinalizedBucket - window, bucketSpan);
        checkBucketEvents(start, end, ActionListener.wrap(
            finalizedBuckets -> checkTrueData(start, end, ActionListener.wrap(
                    indexedData -> listener.onResponse(finalizedBuckets.entrySet()
                        .stream()
                        .map((entry) -> indexedData.getOrDefault(entry.getKey(), 0L) - entry.getValue())
                        .filter(v -> v > 0)
                        .collect(Collectors.summingLong(Long::longValue))),
                    listener::onFailure)
            ),
            listener::onFailure
        ));
    }

    private void checkBucketEvents(long start, long end, ActionListener<Map<Long, Long>> listener) {
        GetBucketsAction.Request request = new GetBucketsAction.Request(job.getId());
        request.setStart(Long.toString(start));
        request.setEnd(Long.toString(end));
        request.setExcludeInterim(true);
        request.setPageParams(new PageParams(0, (int)((end - start)/bucketSpan)));

        ClientHelper.executeAsyncWithOrigin(client, ClientHelper.ML_ORIGIN, GetBucketsAction.INSTANCE, request, ActionListener.wrap(
            response -> {
                Map<Long, Long> map = new HashMap<>((int)response.getBuckets().count());
                response.getBuckets().results().forEach(bucket -> map.put(bucket.getEpoch() * 1000, bucket.getEventCount()));
                listener.onResponse(map);
            },
            listener::onFailure
        ));
    }

    private void checkTrueData(long start, long end, ActionListener<Map<Long, Long>> listener) {
        String timeField = job.getDataDescription().getTimeField();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .size(0)
            .aggregation(new DateHistogramAggregationBuilder(DATE_BUCKETS).interval(bucketSpan).field(timeField))
            .query(ExtractorUtils.wrapInTimeRangeQuery(datafeedConfig.getQuery(), timeField, start, end));

        SearchRequest searchRequest = new SearchRequest(datafeedConfig.getIndices().toArray(new String[0])).source(searchSourceBuilder);
        ClientHelper.executeAsyncWithOrigin(client, ClientHelper.ML_ORIGIN, SearchAction.INSTANCE, searchRequest, ActionListener.wrap(
            response -> {
                List<? extends Histogram.Bucket> buckets = ((Histogram)response.getAggregations().get(DATE_BUCKETS)).getBuckets();
                Map<Long, Long> hashMap = new HashMap<>(buckets.size());
                for (Histogram.Bucket bucket : buckets) {
                    long bucketTime = toHistogramKeyToEpoch(bucket.getKey());
                    if (bucketTime < 0) {
                        listener.onFailure(
                            new IllegalStateException("Histogram key [" + bucket.getKey() + "] cannot be converted to a timestamp"));
                        return;
                    }
                    hashMap.put(bucketTime, bucket.getDocCount());
                }
                listener.onResponse(hashMap);
            },
            listener::onFailure
        ));
    }

    private static long toHistogramKeyToEpoch(Object key) {
        if (key instanceof DateTime) {
            return ((DateTime)key).getMillis();
        } else if (key instanceof Double) {
            return ((Double)key).longValue();
        } else if (key instanceof Long){
            return (Long)key;
        } else {
            return -1L;
        }
    }
}
