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
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.utils.Intervals;
import org.elasticsearch.xpack.ml.datafeed.delayeddatacheck.DelayedDataDetectorFactory.BucketWithMissingData;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.esql.EsqlDatafeedQueryValidator;

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
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

/**
 * Delayed data detector for datafeeds that use an ES|QL query.
 * <p>
 * The datafeed's ES|QL query must emit the job's {@code summary_count_field_name} as a numeric column. The detector re-runs the query
 * over the check window through the datafeed's {@link DataExtractorFactory} and compares the sum of that column per bucket with the
 * event count of the corresponding finalized bucket. The check is only meaningful when the grouping interval equals the bucket span,
 * which datafeed validation enforces for ES|QL datafeeds.
 */
public class EsqlDelayedDataDetector implements DelayedDataDetector {

    private static final Pattern DECIMAL = Pattern.compile("[+-]?(\\d+\\.?\\d*|\\.\\d+)([eE][+-]?\\d+)?");

    private static final Logger logger = LogManager.getLogger(EsqlDelayedDataDetector.class);

    private final long bucketSpan;
    private final long window;
    private final String jobId;
    private final String timeField;
    private final long groupingInterval;
    private final String summaryCountFieldName;
    private final DataExtractorFactory dataExtractorFactory;
    private final Client client;
    // Sticky: a check that has not yet published its extractor must still observe a cancel().
    private volatile boolean cancelled;
    private volatile DataExtractor activeExtractor;

    EsqlDelayedDataDetector(
        long bucketSpan,
        long window,
        String jobId,
        String timeField,
        long groupingInterval,
        String summaryCountFieldName,
        DataExtractorFactory dataExtractorFactory,
        Client client
    ) {
        this.bucketSpan = bucketSpan;
        this.window = window;
        this.jobId = Objects.requireNonNull(jobId);
        this.timeField = Objects.requireNonNull(timeField);
        this.groupingInterval = groupingInterval;
        this.summaryCountFieldName = Objects.requireNonNull(summaryCountFieldName);
        this.dataExtractorFactory = Objects.requireNonNull(dataExtractorFactory);
        this.client = Objects.requireNonNull(client);
    }

    /**
     * Compares the current result of the datafeed's ES|QL query over {@code [latestFinalizedBucket - window, latestFinalizedBucket)}
     * with the event counts of the job's finalized buckets.
     * <p>
     * This blocks on the buckets request and the ES|QL query, so like the DSL detector it must only run on the datafeed's own
     * thread pool.
     *
     * @param latestFinalizedBucketMs The latest finalized bucket timestamp in milliseconds, the end of the checked window
     * @return the buckets with more documents now than when they were finalized; empty if the re-query was truncated
     */
    @Override
    public List<BucketWithMissingData> detectMissingData(long latestFinalizedBucketMs) {
        // The ES|QL extractor floors its source range to the grouping interval, so the re-query uses the same boundaries.
        final long end = Intervals.alignToFloor(latestFinalizedBucketMs, groupingInterval);
        final long start = Intervals.alignToFloor(latestFinalizedBucketMs - window, groupingInterval);

        if (end <= start || cancelled) {
            return Collections.emptyList();
        }

        List<Bucket> finalizedBuckets = getFinalizedBuckets(start, end);
        Optional<Map<Long, Long>> currentCounts = cancelled ? Optional.empty() : getCurrentBucketEventCounts(start, end);
        if (currentCounts.isEmpty()) {
            return Collections.emptyList();
        }
        List<BucketWithMissingData> result = new ArrayList<>();
        for (Bucket bucket : finalizedBuckets) {
            // Only data added since the bucket was finalized counts as missing: data that was deleted since then is not.
            long missing = currentCounts.get().getOrDefault(bucket.getEpoch() * 1000, 0L) - bucket.getEventCount();
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

    @Override
    public void cancel() {
        cancelled = true;
        // Single read: the checking thread nulls this field when it finishes.
        DataExtractor extractor = activeExtractor;
        if (extractor != null) {
            extractor.cancel();
        }
    }

    private List<Bucket> getFinalizedBuckets(long start, long end) {
        GetBucketsAction.Request request = new GetBucketsAction.Request(jobId);
        request.setStart(Long.toString(start));
        request.setEnd(Long.toString(end));
        request.setSort("timestamp");
        request.setDescending(false);
        request.setExcludeInterim(true);
        // The window is validated to span at most MAX_NUMBER_SPANABLE_BUCKETS buckets, which fits in a single page.
        request.setPageParams(new PageParams(0, (int) ((end - start) / bucketSpan)));

        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(ML_ORIGIN)) {
            return client.execute(GetBucketsAction.INSTANCE, request).actionGet().getBuckets().results();
        }
    }

    /**
     * @return the sum of {@code summaryCountFieldName} per bucket start, or empty if the re-query was truncated and its counts
     *         cannot be trusted
     */
    private Optional<Map<Long, Long>> getCurrentBucketEventCounts(long start, long end) {
        Map<Long, Long> bucketCounts = new HashMap<>();
        long ignoredRows = 0;
        Optional<SearchInterval> incompleteSearchInterval;
        DataExtractor dataExtractor = dataExtractorFactory.newExtractor(start, end);
        activeExtractor = dataExtractor;
        if (cancelled) {
            // cancel() may have run before the extractor was published.
            dataExtractor.cancel();
        }
        try {
            while (cancelled == false && dataExtractor.hasNext()) {
                Optional<InputStream> data = dataExtractor.next().data();
                if (data.isPresent()) {
                    try (InputStream in = data.get()) {
                        ignoredRows += accumulateBucketCounts(bucketCounts, in, start, end);
                    }
                }
            }
            if (cancelled || dataExtractor.isCancelled()) {
                // A cancelled extractor stops early, so its counts are partial.
                return Optional.empty();
            }
            incompleteSearchInterval = dataExtractor.getIncompleteSearchInterval();
        } catch (IOException | RuntimeException e) {
            if (cancelled) {
                // Cancelling an extractor mid-query may surface as an exception; a stopped datafeed does not report it.
                return Optional.empty();
            }
            if (e instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new UncheckedIOException("[" + jobId + "] Delayed data check failed while re-running the ES|QL query", (IOException) e);
        } finally {
            activeExtractor = null;
            dataExtractor.destroy();
        }
        if (incompleteSearchInterval.isPresent()) {
            // Counts from a truncated result would report false missing data or mask real missing data. This repeats on every check
            // for as long as the window is too large for the row limit, so it stays at debug.
            SearchInterval interval = incompleteSearchInterval.get();
            logger.debug(
                "[{}] Delayed data check re-query for window [{}, {}) was truncated at [{}, {}]; skipping missing data reporting "
                    + "for this check",
                jobId,
                start,
                end,
                interval.startMs(),
                interval.endMs()
            );
            return Optional.empty();
        }
        if (ignoredRows > 0) {
            logger.debug(
                "[{}] Delayed data check ignored [{}] row(s) with an ignored (null, unparseable, negative or zero) [{}]",
                jobId,
                ignoredRows,
                summaryCountFieldName
            );
        }
        return Optional.of(bucketCounts);
    }

    /**
     * Adds the per-bucket counts of one NDJSON batch to {@code bucketCounts}.
     *
     * @return the number of rows ignored because autodetect ignores their summary count
     */
    private long accumulateBucketCounts(Map<Long, Long> bucketCounts, InputStream in, long sourceWindowStart, long sourceWindowEnd)
        throws IOException {
        long ignoredRows = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                Map<String, Object> row = parseRow(line);
                // Validate the time of every row, including those without a count, as the datafeed does when it extracts them.
                long emittedTime = EsqlDatafeedQueryValidator.validateEmittedTimeValueInSourceWindow(
                    jobId,
                    timeField,
                    row.get(timeField),
                    sourceWindowStart,
                    sourceWindowEnd
                );
                long count = parseSummaryCount(row.get(summaryCountFieldName));
                if (count == 0) {
                    ignoredRows++;
                    continue;
                }
                bucketCounts.merge(Intervals.alignToFloor(emittedTime, groupingInterval), count, Long::sum);
            }
        }
        return ignoredRows;
    }

    /**
     * Mirrors autodetect's {@code CDataGatherer::extractCountFromField}: the value is read as a double and rounded half up.
     *
     * @return the count, or 0 if autodetect ignores the record (null, unparseable, negative or rounds to zero)
     */
    static long parseSummaryCount(Object value) {
        double parsed;
        if (value == null) {
            return 0;
        } else if (value instanceof Number number) {
            parsed = number.doubleValue();
        } else {
            // Autodetect uses strtod and consumes the whole trimmed string. Check the grammar first so Java-only forms ("3d") fail;
            // strtod's hex and inf/nan forms are intentionally unsupported (a string column of hex counts is not a supported shape).
            String text = value.toString().trim();
            if (DECIMAL.matcher(text).matches() == false) {
                return 0;
            }
            try {
                parsed = Double.parseDouble(text);
            } catch (NumberFormatException e) {
                return 0;
            }
        }
        return Double.isNaN(parsed) || Double.isInfinite(parsed) || parsed < 0.0 ? 0 : (long) (parsed + 0.5);
    }

    private static Map<String, Object> parseRow(String json) throws IOException {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json)) {
            return parser.map();
        }
    }
}
