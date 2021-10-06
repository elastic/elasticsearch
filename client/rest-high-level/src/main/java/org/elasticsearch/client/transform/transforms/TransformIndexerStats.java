/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TransformIndexerStats {
    public static final String NAME = "transform_indexer_stats";

    static ParseField EXPONENTIAL_AVG_CHECKPOINT_DURATION_MS = new ParseField("exponential_avg_checkpoint_duration_ms");
    static ParseField EXPONENTIAL_AVG_DOCUMENTS_INDEXED = new ParseField("exponential_avg_documents_indexed");
    static ParseField EXPONENTIAL_AVG_DOCUMENTS_PROCESSED = new ParseField("exponential_avg_documents_processed");
    static ParseField PAGES_PROCESSED = new ParseField("pages_processed");
    static ParseField DOCUMENTS_PROCESSED = new ParseField("documents_processed");
    static ParseField DOCUMENTS_INDEXED = new ParseField("documents_indexed");
    static ParseField DOCUMENTS_DELETED = new ParseField("documents_deleted");
    static ParseField TRIGGER_COUNT = new ParseField("trigger_count");
    static ParseField INDEX_TIME_IN_MS = new ParseField("index_time_in_ms");
    static ParseField SEARCH_TIME_IN_MS = new ParseField("search_time_in_ms");
    static ParseField PROCESSING_TIME_IN_MS = new ParseField("processing_time_in_ms");
    static ParseField DELETE_TIME_IN_MS = new ParseField("delete_time_in_ms");
    static ParseField INDEX_TOTAL = new ParseField("index_total");
    static ParseField SEARCH_TOTAL = new ParseField("search_total");
    static ParseField PROCESSING_TOTAL = new ParseField("processing_total");
    static ParseField SEARCH_FAILURES = new ParseField("search_failures");
    static ParseField INDEX_FAILURES = new ParseField("index_failures");

    public static final ConstructingObjectParser<TransformIndexerStats, Void> LENIENT_PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        args -> new TransformIndexerStats(
            unboxSafe(args[0], 0L),
            unboxSafe(args[1], 0L),
            unboxSafe(args[2], 0L),
            unboxSafe(args[3], 0L),
            unboxSafe(args[4], 0L),
            unboxSafe(args[5], 0L),
            unboxSafe(args[6], 0L),
            unboxSafe(args[7], 0L),
            unboxSafe(args[8], 0L),
            unboxSafe(args[9], 0L),
            unboxSafe(args[10], 0L),
            unboxSafe(args[11], 0L),
            unboxSafe(args[12], 0L),
            unboxSafe(args[13], 0L),
            unboxSafe(args[14], 0.0),
            unboxSafe(args[15], 0.0),
            unboxSafe(args[16], 0.0)
        )
    );

    static {
        LENIENT_PARSER.declareLong(optionalConstructorArg(), PAGES_PROCESSED);
        LENIENT_PARSER.declareLong(optionalConstructorArg(), DOCUMENTS_PROCESSED);
        LENIENT_PARSER.declareLong(optionalConstructorArg(), DOCUMENTS_INDEXED);
        LENIENT_PARSER.declareLong(optionalConstructorArg(), DOCUMENTS_DELETED);
        LENIENT_PARSER.declareLong(optionalConstructorArg(), TRIGGER_COUNT);
        LENIENT_PARSER.declareLong(optionalConstructorArg(), INDEX_TIME_IN_MS);
        LENIENT_PARSER.declareLong(optionalConstructorArg(), SEARCH_TIME_IN_MS);
        LENIENT_PARSER.declareLong(optionalConstructorArg(), PROCESSING_TIME_IN_MS);
        LENIENT_PARSER.declareLong(optionalConstructorArg(), DELETE_TIME_IN_MS);
        LENIENT_PARSER.declareLong(optionalConstructorArg(), INDEX_TOTAL);
        LENIENT_PARSER.declareLong(optionalConstructorArg(), SEARCH_TOTAL);
        LENIENT_PARSER.declareLong(optionalConstructorArg(), PROCESSING_TOTAL);
        LENIENT_PARSER.declareLong(optionalConstructorArg(), INDEX_FAILURES);
        LENIENT_PARSER.declareLong(optionalConstructorArg(), SEARCH_FAILURES);
        LENIENT_PARSER.declareDouble(optionalConstructorArg(), EXPONENTIAL_AVG_CHECKPOINT_DURATION_MS);
        LENIENT_PARSER.declareDouble(optionalConstructorArg(), EXPONENTIAL_AVG_DOCUMENTS_INDEXED);
        LENIENT_PARSER.declareDouble(optionalConstructorArg(), EXPONENTIAL_AVG_DOCUMENTS_PROCESSED);
    }

    public static TransformIndexerStats fromXContent(XContentParser parser) throws IOException {
        return LENIENT_PARSER.parse(parser, null);
    }

    private final double expAvgCheckpointDurationMs;
    private final double expAvgDocumentsIndexed;
    private final double expAvgDocumentsProcessed;
    private final long pagesProcessed;
    private final long documentsProcessed;
    private final long documentsIndexed;
    private final long documentsDeleted;
    private final long triggerCount;
    private final long indexTime;
    private final long indexTotal;
    private final long searchTime;
    private final long searchTotal;
    private final long processingTime;
    private final long deleteTime;
    private final long processingTotal;
    private final long indexFailures;
    private final long searchFailures;

    public TransformIndexerStats(
        long pagesProcessed,
        long documentsProcessed,
        long documentsIndexed,
        long documentsDeleted,
        long triggerCount,
        long indexTime,
        long searchTime,
        long processingTime,
        long deleteTime,
        long indexTotal,
        long searchTotal,
        long processingTotal,
        long indexFailures,
        long searchFailures,
        double expAvgCheckpointDurationMs,
        double expAvgDocumentsIndexed,
        double expAvgDocumentsProcessed
    ) {
        this.pagesProcessed = pagesProcessed;
        this.documentsProcessed = documentsProcessed;
        this.documentsIndexed = documentsIndexed;
        this.documentsDeleted = documentsDeleted;
        this.triggerCount = triggerCount;
        this.indexTime = indexTime;
        this.indexTotal = indexTotal;
        this.searchTime = searchTime;
        this.deleteTime = deleteTime;
        this.searchTotal = searchTotal;
        this.processingTime = processingTime;
        this.processingTotal = processingTotal;
        this.indexFailures = indexFailures;
        this.searchFailures = searchFailures;
        this.expAvgCheckpointDurationMs = expAvgCheckpointDurationMs;
        this.expAvgDocumentsIndexed = expAvgDocumentsIndexed;
        this.expAvgDocumentsProcessed = expAvgDocumentsProcessed;
    }

    public double getExpAvgCheckpointDurationMs() {
        return expAvgCheckpointDurationMs;
    }

    public double getExpAvgDocumentsIndexed() {
        return expAvgDocumentsIndexed;
    }

    public double getExpAvgDocumentsProcessed() {
        return expAvgDocumentsProcessed;
    }

    /**
     * The number of pages read from the input indices
     */
    public long getPagesProcessed() {
        return pagesProcessed;
    }

    /**
     * The number of documents read from the input indices
     */
    public long getDocumentsProcessed() {
        return documentsProcessed;
    }

    /**
     * Number of times that the job woke up to write documents
     */
    public long getTriggerCount() {
        return triggerCount;
    }

    /**
     * Number of documents written
     */
    public long getDocumentsIndexed() {
        return documentsIndexed;
    }

    /**
     * The number of pages read from the input indices
     * Deprecated, use {@link TransformIndexerStats#getPagesProcessed()} instead
     */
    @Deprecated
    public long getNumPages() {
        return getPagesProcessed();
    }

    /**
     * The number of documents read from the input indices
     * Deprecated, use {@link TransformIndexerStats#getDocumentsProcessed()} instead
     */
    @Deprecated
    public long getNumDocuments() {
        return getDocumentsProcessed();
    }

    /**
     * Number of times that the job woke up to write documents
     * Deprecated, use {@link TransformIndexerStats#getTriggerCount()} instead
     */
    @Deprecated
    public long getNumInvocations() {
        return getTriggerCount();
    }

    /**
     * Number of documents written
     * Deprecated, use {@link TransformIndexerStats#getDocumentsIndexed()} instead
     */
    @Deprecated
    public long getOutputDocuments() {
        return getDocumentsIndexed();
    }

    /**
     * Number of documents deleted
     */
    public long getDocumentsDeleted() {
        return documentsDeleted;
    }

    /**
     * Number of index failures that have occurred
     */
    public long getIndexFailures() {
        return indexFailures;
    }

    /**
     * Number of failures that have occurred
     */
    public long getSearchFailures() {
        return searchFailures;
    }

    /**
     * Returns the time spent indexing (cumulative) in milliseconds
     */
    public long getIndexTime() {
        return indexTime;
    }

    /**
     * Returns the time spent searching (cumulative) in milliseconds
     */
    public long getSearchTime() {
        return searchTime;
    }

    /**
     * Returns the time spent processing (cumulative) in milliseconds
     */
    public long getProcessingTime() {
        return processingTime;
    }

    /**
     * Returns the time spent deleting (cumulative) in milliseconds
     */
    public long getDeleteTime() {
        return deleteTime;
    }

    /**
     * Returns the total number of indexing requests that have been processed
     * (Note: this is not the number of _documents_ that have been indexed)
     */
    public long getIndexTotal() {
        return indexTotal;
    }

    /**
     * Returns the total number of search requests that have been made
     */
    public long getSearchTotal() {
        return searchTotal;
    }

    /**
     * Returns the total number of processing runs that have been made
     */
    public long getProcessingTotal() {
        return processingTotal;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        TransformIndexerStats that = (TransformIndexerStats) other;

        return Objects.equals(this.pagesProcessed, that.pagesProcessed)
            && Objects.equals(this.documentsProcessed, that.documentsProcessed)
            && Objects.equals(this.documentsIndexed, that.documentsIndexed)
            && Objects.equals(this.documentsDeleted, that.documentsDeleted)
            && Objects.equals(this.triggerCount, that.triggerCount)
            && Objects.equals(this.indexTime, that.indexTime)
            && Objects.equals(this.searchTime, that.searchTime)
            && Objects.equals(this.processingTime, that.processingTime)
            && Objects.equals(this.deleteTime, that.deleteTime)
            && Objects.equals(this.indexFailures, that.indexFailures)
            && Objects.equals(this.searchFailures, that.searchFailures)
            && Objects.equals(this.indexTotal, that.indexTotal)
            && Objects.equals(this.searchTotal, that.searchTotal)
            && Objects.equals(this.processingTotal, that.processingTotal)
            && Objects.equals(this.expAvgCheckpointDurationMs, that.expAvgCheckpointDurationMs)
            && Objects.equals(this.expAvgDocumentsIndexed, that.expAvgDocumentsIndexed)
            && Objects.equals(this.expAvgDocumentsProcessed, that.expAvgDocumentsProcessed);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            pagesProcessed,
            documentsProcessed,
            documentsIndexed,
            documentsDeleted,
            triggerCount,
            indexTime,
            searchTime,
            processingTime,
            deleteTime,
            indexFailures,
            searchFailures,
            indexTotal,
            searchTotal,
            processingTotal,
            expAvgCheckpointDurationMs,
            expAvgDocumentsIndexed,
            expAvgDocumentsProcessed
        );
    }

    @SuppressWarnings("unchecked")
    private static <T> T unboxSafe(Object l, T default_value) {
        if (l == null) {
            return default_value;
        } else {
            return (T) l;
        }
    }
}
