/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.delayeddatacheck;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.utils.Intervals;
import org.elasticsearch.xpack.ml.datafeed.delayeddatacheck.DelayedDataDetectorFactory.BucketWithMissingData;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

/**
 * Delayed data detector for datafeeds that use an ES|QL query.
 *
 * <p>The {@link DatafeedDelayedDataDetector} cannot be used for ES|QL datafeeds because they have no DSL query or aggregation to feed a
 * date-histogram, and because the unit of a bucket's {@code event_count} is "ES|QL output rows" rather than raw documents. Instead, this
 * detector answers "what's in the index now" by re-running the datafeed's own ES|QL query over the check window (via the datafeed's
 * {@link DataExtractorFactory}) and summing the job's {@code summary_count_field_name} per bucket. Because the job folds that same field
 * into each bucket's {@code event_count} as a count multiplier, both sides of the comparison speak in the same unit, giving a true
 * apples-to-apples check of what the job saw versus what it would see if re-run now.
 *
 * <p>Reusing the datafeed's extractor means the re-run inherits the exact query, time filtering and chunking the datafeed uses, so a wide
 * window does not silently truncate at the ES|QL row cap.
 */
public class EsqlDelayedDataDetector implements DelayedDataDetector {

    private final long bucketSpan;
    private final long window;
    private final String jobId;
    private final String timeField;
    private final String summaryCountFieldName;
    private final DataExtractorFactory dataExtractorFactory;
    private final Client client;

    EsqlDelayedDataDetector(
        long bucketSpan,
        long window,
        String jobId,
        String timeField,
        String summaryCountFieldName,
        DataExtractorFactory dataExtractorFactory,
        Client client
    ) {
        this.bucketSpan = bucketSpan;
        this.window = window;
        this.jobId = jobId;
        this.timeField = timeField;
        this.summaryCountFieldName = Objects.requireNonNull(summaryCountFieldName);
        this.dataExtractorFactory = Objects.requireNonNull(dataExtractorFactory);
        this.client = client;
    }

    /**
     * Compares the job's finalized bucket event counts against a re-run of the datafeed's ES|QL query over the window
     * {@code [latestFinalizedBucketMs - window, latestFinalizedBucketMs]}.
     *
     * It is done synchronously, and can block for a considerable amount of time, it should only be executed within the appropriate
     * thread pool.
     *
     * @param latestFinalizedBucketMs The latest finalized bucket timestamp in milliseconds, signifies the end of the time window check
     * @return A List of {@link BucketWithMissingData} objects that contain each bucket with the current number of missing docs
     */
    @Override
    public List<BucketWithMissingData> detectMissingData(long latestFinalizedBucketMs) {
        final long end = Intervals.alignToFloor(latestFinalizedBucketMs, bucketSpan);
        final long start = Intervals.alignToFloor(latestFinalizedBucketMs - window, bucketSpan);

        if (end <= start) {
            return Collections.emptyList();
        }

        List<Bucket> finalizedBuckets = checkBucketEvents(start, end);
        Map<Long, Long> indexedData = checkCurrentBucketEventCount(start, end);
        return finalizedBuckets.stream()
            // We only care about the situation when data is added to the indices
            // Older data could have been removed from the indices, and should not be considered "missing data"
            .filter(bucket -> calculateMissing(indexedData, bucket) > 0)
            .map(bucket -> BucketWithMissingData.fromMissingAndBucket(calculateMissing(indexedData, bucket), bucket))
            .collect(Collectors.toList());
    }

    @Override
    public long getWindow() {
        return window;
    }

    private List<Bucket> checkBucketEvents(long start, long end) {
        GetBucketsAction.Request request = new GetBucketsAction.Request(jobId);
        request.setStart(Long.toString(start));
        request.setEnd(Long.toString(end));
        request.setSort("timestamp");
        request.setDescending(false);
        request.setExcludeInterim(true);
        request.setPageParams(new PageParams(0, (int) ((end - start) / bucketSpan)));

        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(ML_ORIGIN)) {
            GetBucketsAction.Response response = client.execute(GetBucketsAction.INSTANCE, request).actionGet();
            return response.getBuckets().results();
        }
    }

    /**
     * Re-runs the datafeed's ES|QL query over {@code [start, end)} and returns, per bucket-span aligned bucket, the sum of the
     * {@code summary_count_field_name} across the rows whose time field falls in that bucket. This mirrors how the autodetect engine
     * assigns and sums each record's summary count into buckets at ingest, so the result is directly comparable with bucket event counts.
     */
    private Map<Long, Long> checkCurrentBucketEventCount(long start, long end) {
        Map<Long, Long> bucketCounts = new HashMap<>();
        DataExtractor dataExtractor = dataExtractorFactory.newExtractor(start, end);
        try {
            while (dataExtractor.hasNext()) {
                DataExtractor.Result result = dataExtractor.next();
                Optional<InputStream> data = result.data();
                if (data.isPresent()) {
                    try (InputStream in = data.get()) {
                        accumulateBucketCounts(bucketCounts, in);
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("[" + jobId + "] Delayed data check failed while re-running the ES|QL query", e);
        } finally {
            dataExtractor.destroy();
        }
        return bucketCounts;
    }

    private void accumulateBucketCounts(Map<Long, Long> bucketCounts, InputStream in) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                Map<String, Object> doc = parseRecord(line);
                Object countValue = doc.get(summaryCountFieldName);
                if (countValue == null) {
                    // The ES|QL extractor omits null fields, so an absent value means the configured summary count field is either not a
                    // column in the query's output or is null for this row. Either way the comparison would be meaningless, so fail loudly.
                    throw new IllegalStateException(
                        "["
                            + jobId
                            + "] Delayed data check requires the configured summary_count_field_name ["
                            + summaryCountFieldName
                            + "] to be a non-null column produced by the esql_query"
                    );
                }
                Object timeValue = doc.get(timeField);
                if (timeValue == null) {
                    continue;
                }
                long bucketStart = Intervals.alignToFloor(((Number) timeValue).longValue(), bucketSpan);
                bucketCounts.merge(bucketStart, ((Number) countValue).longValue(), Long::sum);
            }
        }
    }

    private static Map<String, Object> parseRecord(String json) throws IOException {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json)) {
            return parser.map();
        }
    }

    private static long calculateMissing(Map<Long, Long> indexedData, Bucket bucket) {
        return indexedData.getOrDefault(bucket.getEpoch() * 1000, 0L) - bucket.getEventCount();
    }
}
