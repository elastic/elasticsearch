/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.delayeddatacheck;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

/**
 * Delayed data detector for datafeeds that use an ES|QL query.
 *
 * Users with ES|QL datafeeds wanting to use the delayed data check feature must configure the {@code summary_count_field_name} mapping to
 * a field that tracks the number of documents processed by their ES|QL query. This detector will re-run the ES|QL query against current
 * the data in the index and will compare the sum of the summary_count_field_name per bucket with the sum's of the
 * summary_count_field_name values that were seen when the datafeed originally ran.
 */
public class EsqlDelayedDataDetector implements DelayedDataDetector {

    private static final Logger logger = LogManager.getLogger(EsqlDelayedDataDetector.class);

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
        this.jobId = Objects.requireNonNull(jobId);
        this.timeField = Objects.requireNonNull(timeField);
        this.summaryCountFieldName = Objects.requireNonNull(summaryCountFieldName);
        this.dataExtractorFactory = Objects.requireNonNull(dataExtractorFactory);
        this.client = Objects.requireNonNull(client);
    }

    @Override
    public List<BucketWithMissingData> detectMissingData(long latestFinalizedBucketMs) {
        final long end = Intervals.alignToFloor(latestFinalizedBucketMs, bucketSpan);
        final long start = Intervals.alignToFloor(latestFinalizedBucketMs - window, bucketSpan);

        if (end <= start) {
            return Collections.emptyList();
        }

        List<Bucket> finalizedBuckets = getBucketEvents(start, end);
        Map<Long, Long> indexedData = getCurrentBucketEventCount(start, end);
        List<BucketWithMissingData> result = new ArrayList<>();
        for (Bucket bucket : finalizedBuckets) {
            long missing = calculateMissing(indexedData, bucket);
            if (missing > 0) {
                result.add(BucketWithMissingData.fromMissingAndBucket(missing, bucket));
            }
        }
        return result;
    }

    @Override
    public long getWindow() {
        return window;
    }

    private List<Bucket> getBucketEvents(long start, long end) {
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

    private Map<Long, Long> getCurrentBucketEventCount(long start, long end) {
        Map<Long, Long> bucketCounts = new HashMap<>();
        long nullCountRows = 0;
        DataExtractor dataExtractor = dataExtractorFactory.newExtractor(start, end);
        try {
            while (dataExtractor.hasNext()) {
                DataExtractor.Result result = dataExtractor.next();
                Optional<InputStream> data = result.data();
                if (data.isPresent()) {
                    try (InputStream in = data.get()) {
                        nullCountRows += accumulateBucketCounts(bucketCounts, in);
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("[" + jobId + "] Delayed data check failed while re-running the ES|QL query", e);
        } finally {
            dataExtractor.destroy();
        }
        if (nullCountRows > 0) {
            logger.warn(
                "[{}] Delayed data check skipped {} row(s) where summary_count_field_name [{}] was null. "
                    + "These rows contribute nothing to the delayed-data count (under-reporting, not false positives).",
                jobId,
                nullCountRows,
                summaryCountFieldName
            );
        }
        return bucketCounts;
    }

    /**
     * Accumulates per-bucket event counts from one NDJSON batch. Returns the number of rows that
     * were skipped because their {@code summaryCountFieldName} value was null.
     */
    private long accumulateBucketCounts(Map<Long, Long> bucketCounts, InputStream in) throws IOException {
        long skipped = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                Map<String, Object> doc = parseRecord(line);
                Object countValue = doc.get(summaryCountFieldName);
                if (countValue == null) {
                    // A null aggregate (e.g. SUM over an all-null group) is legal ES|QL. The same
                    // null was present when the datafeed originally ran, so autodetect already
                    // counted this row as contributing nothing. Skip for symmetry rather than throw.
                    skipped++;
                    continue;
                }
                if (countValue instanceof Number == false) {
                    throw new IllegalArgumentException(
                        "["
                            + jobId
                            + "] Delayed data check: summary_count_field_name ["
                            + summaryCountFieldName
                            + "] must be a numeric column but got ["
                            + countValue.getClass().getSimpleName()
                            + "]. Check that the ES|QL query produces a numeric value for this field."
                    );
                }
                Object timeValue = doc.get(timeField);
                if (timeValue == null) {
                    continue;
                }
                if (timeValue instanceof Number == false) {
                    throw new IllegalArgumentException(
                        "["
                            + jobId
                            + "] Delayed data check: time field ["
                            + timeField
                            + "] must be a numeric (epoch-ms) column but got ["
                            + timeValue.getClass().getSimpleName()
                            + "]. Check that the ES|QL query produces a numeric timestamp for this field."
                    );
                }
                long bucketStart = Intervals.alignToFloor(((Number) timeValue).longValue(), bucketSpan);
                bucketCounts.merge(bucketStart, ((Number) countValue).longValue(), Long::sum);
            }
        }
        return skipped;
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
