/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.delayeddatacheck;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
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

/** Delayed-data detector that re-runs the datafeed ES|QL query over the check window. */
public class EsqlDelayedDataDetector implements DelayedDataDetector {

    private static final Pattern DECIMAL = Pattern.compile("[+-]?(\\d+\\.?\\d*|\\.\\d+)([eE][+-]?\\d+)?");

    private static final Logger logger = LogManager.getLogger(EsqlDelayedDataDetector.class);

    /** Autodetect ignores null, unparseable, negative, and zero-rounded summary counts. */
    static final long IGNORED_SUMMARY_COUNT = 0L;

    private final long bucketSpan;
    private final long window;
    private final String jobId;
    private final String timeField;
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
        String summaryCountFieldName,
        DataExtractorFactory dataExtractorFactory,
        Client client
    ) {
        assert bucketSpan > 0 : "bucketSpan must be positive";
        this.bucketSpan = bucketSpan;
        this.window = window;
        this.jobId = Objects.requireNonNull(jobId);
        this.timeField = Objects.requireNonNull(timeField);
        this.summaryCountFieldName = Objects.requireNonNull(summaryCountFieldName);
        this.dataExtractorFactory = Objects.requireNonNull(dataExtractorFactory);
        this.client = Objects.requireNonNull(client);
    }

    /** Returns finalized buckets whose current ES|QL counts exceed their event counts, or empty when the re-query is not trustworthy. */
    @Override
    public List<BucketWithMissingData> detectMissingData(long latestFinalizedBucketMs) {
        // The ES|QL extractor floors its source range to the bucket span, so the re-query uses the same boundaries.
        final long end = Intervals.alignToFloor(latestFinalizedBucketMs, bucketSpan);
        final long start = Intervals.alignToFloor(latestFinalizedBucketMs - window, bucketSpan);

        if (end <= start || cancelled) {
            return Collections.emptyList();
        }

        List<Bucket> finalizedBuckets = DelayedDataDetectorFactory.getFinalizedBuckets(client, jobId, bucketSpan, start, end);
        Optional<Map<Long, Long>> currentCounts = cancelled ? Optional.empty() : getCurrentBucketEventCounts(start, end);
        if (currentCounts.isEmpty()) {
            return Collections.emptyList();
        }
        List<BucketWithMissingData> result = new ArrayList<>();
        for (Bucket bucket : finalizedBuckets) {
            // Only data added since the bucket was finalized counts as missing. Data that was deleted since then is not.
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

    private Optional<Map<Long, Long>> getCurrentBucketEventCounts(long start, long end) {
        DataExtractor dataExtractor = dataExtractorFactory.newExtractor(start, end);
        activeExtractor = dataExtractor;
        if (cancelled) {
            // cancel() may have run before the extractor was published.
            dataExtractor.cancel();
        }
        try {
            Map<Long, Long> bucketCounts = new HashMap<>();
            long ignoredRows = readRequeryBatchesIntoCounts(dataExtractor, bucketCounts, start, end);
            if (cancelled || dataExtractor.isCancelled()) {
                // A cancelled extractor stops early, so its counts are partial.
                return Optional.empty();
            }
            Optional<SearchInterval> incompleteSearchInterval = dataExtractor.getIncompleteSearchInterval();
            if (incompleteSearchInterval.isPresent()) {
                // Counts from a truncated result would report false missing data or mask real missing data.
                SearchInterval interval = incompleteSearchInterval.get();
                logger.debug(
                    "[{}] Delayed data check re-query for window [{}, {}) was truncated at [{}, {}). "
                        + "Skipping missing data reporting for this check.",
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
        } catch (UncheckedIOException e) {
            if (cancelled) {
                // Cancelling an extractor mid-query may surface as an exception. A stopped datafeed does not report it.
                logger.debug("[{}] Delayed data check re-query cancelled", jobId, e);
                return Optional.empty();
            }
            throw e;
        } catch (IllegalStateException e) {
            if (cancelled) {
                logger.debug("[{}] Delayed data check re-query cancelled", jobId, e);
                return Optional.empty();
            }
            throw e;
        } finally {
            activeExtractor = null;
            dataExtractor.destroy();
        }
    }

    private long readRequeryBatchesIntoCounts(
        DataExtractor dataExtractor,
        Map<Long, Long> bucketCounts,
        long sourceWindowStart,
        long sourceWindowEnd
    ) {
        long ignoredRows = 0;
        try {
            while (cancelled == false && dataExtractor.hasNext()) {
                Optional<InputStream> data = dataExtractor.next().data();
                if (data.isPresent()) {
                    try (InputStream in = data.get()) {
                        ignoredRows += accumulateBucketCounts(bucketCounts, in, sourceWindowStart, sourceWindowEnd);
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("[" + jobId + "] Delayed data check failed while re-running the ES|QL query", e);
        }
        return ignoredRows;
    }

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
                if (count == IGNORED_SUMMARY_COUNT) {
                    ignoredRows++;
                    continue;
                }
                bucketCounts.merge(Intervals.alignToFloor(emittedTime, bucketSpan), count, Long::sum);
            }
        }
        return ignoredRows;
    }

    /** Mirrors autodetect {@code CDataGatherer::extractCountFromField}: double parsed, half-up rounded, negatives ignored. */
    static long parseSummaryCount(Object value) {
        double parsed;
        if (value == null) {
            return IGNORED_SUMMARY_COUNT;
        } else if (value instanceof Number number) {
            parsed = number.doubleValue();
        } else {
            // Autodetect uses strtod and consumes the whole trimmed string. Check the grammar first so Java-only forms ("3d") fail.
            // strtod hex and inf/nan forms are intentionally unsupported.
            String text = value.toString().trim();
            if (DECIMAL.matcher(text).matches() == false) {
                return IGNORED_SUMMARY_COUNT;
            }
            // DECIMAL matches only strings Double.parseDouble accepts. Overflow becomes infinite and is handled below.
            parsed = Double.parseDouble(text);
        }
        return Double.isNaN(parsed) || Double.isInfinite(parsed) || parsed < 0.0 ? IGNORED_SUMMARY_COUNT : (long) (parsed + 0.5);
    }

    private static Map<String, Object> parseRow(String json) throws IOException {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json)) {
            return parser.map();
        }
    }
}
