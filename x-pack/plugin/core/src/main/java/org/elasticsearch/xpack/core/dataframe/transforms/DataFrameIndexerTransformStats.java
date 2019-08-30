/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.indexing.IndexerJobStats;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DataFrameIndexerTransformStats extends IndexerJobStats {

    private static final String DEFAULT_TRANSFORM_ID = "_all";  // TODO remove when no longer needed for wire BWC

    public static final String NAME = "data_frame_indexer_transform_stats";
    public static ParseField NUM_PAGES = new ParseField("pages_processed");
    public static ParseField NUM_INPUT_DOCUMENTS = new ParseField("documents_processed");
    public static ParseField NUM_OUTPUT_DOCUMENTS = new ParseField("documents_indexed");
    public static ParseField NUM_INVOCATIONS = new ParseField("trigger_count");
    public static ParseField INDEX_TIME_IN_MS = new ParseField("index_time_in_ms");
    public static ParseField SEARCH_TIME_IN_MS = new ParseField("search_time_in_ms");
    public static ParseField INDEX_TOTAL = new ParseField("index_total");
    public static ParseField SEARCH_TOTAL = new ParseField("search_total");
    public static ParseField SEARCH_FAILURES = new ParseField("search_failures");
    public static ParseField INDEX_FAILURES = new ParseField("index_failures");
    public static ParseField EXPONENTIAL_AVG_CHECKPOINT_DURATION_MS =
        new ParseField("exponential_avg_checkpoint_duration_ms");
    public static ParseField EXPONENTIAL_AVG_DOCUMENTS_INDEXED =
        new ParseField("exponential_avg_documents_indexed");
    public static ParseField EXPONENTIAL_AVG_DOCUMENTS_PROCESSED =
        new ParseField("exponential_avg_documents_processed");

    // This changes how much "weight" past calculations have.
    // The shorter the window, the less "smoothing" will occur.
    private static final int EXP_AVG_WINDOW = 10;
    private static final double ALPHA = 2.0/(EXP_AVG_WINDOW + 1);

    private static final ConstructingObjectParser<DataFrameIndexerTransformStats, Void> LENIENT_PARSER = new ConstructingObjectParser<>(
            NAME, true,
            args -> new DataFrameIndexerTransformStats(
               (long) args[0], (long) args[1], (long) args[2], (long) args[3], (long) args[4], (long) args[5], (long) args[6],
               (long) args[7], (long) args[8], (long) args[9], (Double) args[10], (Double) args[11], (Double) args[12]));

    static {
        LENIENT_PARSER.declareLong(constructorArg(), NUM_PAGES);
        LENIENT_PARSER.declareLong(constructorArg(), NUM_INPUT_DOCUMENTS);
        LENIENT_PARSER.declareLong(constructorArg(), NUM_OUTPUT_DOCUMENTS);
        LENIENT_PARSER.declareLong(constructorArg(), NUM_INVOCATIONS);
        LENIENT_PARSER.declareLong(constructorArg(), INDEX_TIME_IN_MS);
        LENIENT_PARSER.declareLong(constructorArg(), SEARCH_TIME_IN_MS);
        LENIENT_PARSER.declareLong(constructorArg(), INDEX_TOTAL);
        LENIENT_PARSER.declareLong(constructorArg(), SEARCH_TOTAL);
        LENIENT_PARSER.declareLong(constructorArg(), INDEX_FAILURES);
        LENIENT_PARSER.declareLong(constructorArg(), SEARCH_FAILURES);
        LENIENT_PARSER.declareDouble(optionalConstructorArg(), EXPONENTIAL_AVG_CHECKPOINT_DURATION_MS);
        LENIENT_PARSER.declareDouble(optionalConstructorArg(), EXPONENTIAL_AVG_DOCUMENTS_INDEXED);
        LENIENT_PARSER.declareDouble(optionalConstructorArg(), EXPONENTIAL_AVG_DOCUMENTS_PROCESSED);
    }

    private double expAvgCheckpointDurationMs;
    private double expAvgDocumentsIndexed;
    private double expAvgDocumentsProcessed;
    /**
     * Create with all stats set to zero
     */
    public DataFrameIndexerTransformStats() {
        super();
    }

    public DataFrameIndexerTransformStats(long numPages, long numInputDocuments, long numOutputDocuments,
                                          long numInvocations, long indexTime, long searchTime, long indexTotal, long searchTotal,
                                          long indexFailures, long searchFailures, Double expAvgCheckpointDurationMs,
                                          Double expAvgDocumentsIndexed, Double expAvgDocumentsProcessed ) {
        super(numPages, numInputDocuments, numOutputDocuments, numInvocations, indexTime, searchTime, indexTotal, searchTotal,
            indexFailures, searchFailures);
        this.expAvgCheckpointDurationMs = expAvgCheckpointDurationMs == null ? 0.0 : expAvgCheckpointDurationMs;
        this.expAvgDocumentsIndexed = expAvgDocumentsIndexed == null ? 0.0 : expAvgDocumentsIndexed;
        this.expAvgDocumentsProcessed = expAvgDocumentsProcessed == null ? 0.0 : expAvgDocumentsProcessed;
    }

    public DataFrameIndexerTransformStats(long numPages, long numInputDocuments, long numOutputDocuments,
                                          long numInvocations, long indexTime, long searchTime, long indexTotal, long searchTotal,
                                          long indexFailures, long searchFailures) {
        this(numPages, numInputDocuments, numOutputDocuments, numInvocations, indexTime, searchTime, indexTotal, searchTotal,
            indexFailures, searchFailures, 0.0, 0.0, 0.0);
    }

    public DataFrameIndexerTransformStats(DataFrameIndexerTransformStats other) {
        this(other.numPages, other.numInputDocuments, other.numOuputDocuments, other.numInvocations,
            other.indexTime, other.searchTime, other.indexTotal, other.searchTotal, other.indexFailures, other.searchFailures);
        this.expAvgCheckpointDurationMs = other.expAvgCheckpointDurationMs;
        this.expAvgDocumentsIndexed = other.expAvgDocumentsIndexed;
        this.expAvgDocumentsProcessed = other.expAvgDocumentsProcessed;
    }

    public DataFrameIndexerTransformStats(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().before(Version.V_7_4_0)) {
            in.readString(); // was transformId
        }
        if (in.getVersion().onOrAfter(Version.V_7_4_0)) {
            this.expAvgCheckpointDurationMs = in.readDouble();
            this.expAvgDocumentsIndexed = in.readDouble();
            this.expAvgDocumentsProcessed = in.readDouble();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().before(Version.V_7_4_0)) {
            out.writeString(DEFAULT_TRANSFORM_ID);
        }
        if (out.getVersion().onOrAfter(Version.V_7_4_0)) {
            out.writeDouble(this.expAvgCheckpointDurationMs);
            out.writeDouble(this.expAvgDocumentsIndexed);
            out.writeDouble(this.expAvgDocumentsProcessed);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NUM_PAGES.getPreferredName(), numPages);
        builder.field(NUM_INPUT_DOCUMENTS.getPreferredName(), numInputDocuments);
        builder.field(NUM_OUTPUT_DOCUMENTS.getPreferredName(), numOuputDocuments);
        builder.field(NUM_INVOCATIONS.getPreferredName(), numInvocations);
        builder.field(INDEX_TIME_IN_MS.getPreferredName(), indexTime);
        builder.field(INDEX_TOTAL.getPreferredName(), indexTotal);
        builder.field(INDEX_FAILURES.getPreferredName(), indexFailures);
        builder.field(SEARCH_TIME_IN_MS.getPreferredName(), searchTime);
        builder.field(SEARCH_TOTAL.getPreferredName(), searchTotal);
        builder.field(SEARCH_FAILURES.getPreferredName(), searchFailures);
        builder.field(EXPONENTIAL_AVG_CHECKPOINT_DURATION_MS.getPreferredName(), this.expAvgCheckpointDurationMs);
        builder.field(EXPONENTIAL_AVG_DOCUMENTS_INDEXED.getPreferredName(), this.expAvgDocumentsIndexed);
        builder.field(EXPONENTIAL_AVG_DOCUMENTS_PROCESSED.getPreferredName(), this.expAvgDocumentsProcessed);
        builder.endObject();
        return builder;
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

    public void incrementCheckpointExponentialAverages(long checkpointDurationMs, long docsIndexed, long docsProcessed) {
        assert checkpointDurationMs >= 0;
        assert docsIndexed >= 0;
        assert docsProcessed >= 0;
        // If all our exp averages are 0.0, just assign the new values.
        if (expAvgCheckpointDurationMs == 0.0 && expAvgDocumentsIndexed == 0.0 && expAvgDocumentsProcessed == 0.0) {
            expAvgCheckpointDurationMs = checkpointDurationMs;
            expAvgDocumentsIndexed = docsIndexed;
            expAvgDocumentsProcessed = docsProcessed;
        } else {
            expAvgCheckpointDurationMs = calculateExpAvg(expAvgCheckpointDurationMs, ALPHA, checkpointDurationMs);
            expAvgDocumentsIndexed = calculateExpAvg(expAvgDocumentsIndexed, ALPHA, docsIndexed);
            expAvgDocumentsProcessed = calculateExpAvg(expAvgDocumentsProcessed, ALPHA, docsProcessed);
        }
    }

    private double calculateExpAvg(double previousExpValue, double alpha, long observedValue) {
        return alpha * observedValue + (1-alpha) * previousExpValue;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        DataFrameIndexerTransformStats that = (DataFrameIndexerTransformStats) other;

        return Objects.equals(this.numPages, that.numPages)
            && Objects.equals(this.numInputDocuments, that.numInputDocuments)
            && Objects.equals(this.numOuputDocuments, that.numOuputDocuments)
            && Objects.equals(this.numInvocations, that.numInvocations)
            && Objects.equals(this.indexTime, that.indexTime)
            && Objects.equals(this.searchTime, that.searchTime)
            && Objects.equals(this.indexFailures, that.indexFailures)
            && Objects.equals(this.searchFailures, that.searchFailures)
            && Objects.equals(this.indexTotal, that.indexTotal)
            && Objects.equals(this.searchTotal, that.searchTotal)
            && Objects.equals(this.expAvgCheckpointDurationMs, that.expAvgCheckpointDurationMs)
            && Objects.equals(this.expAvgDocumentsIndexed, that.expAvgDocumentsIndexed)
            && Objects.equals(this.expAvgDocumentsProcessed, that.expAvgDocumentsProcessed);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numPages, numInputDocuments, numOuputDocuments, numInvocations,
            indexTime, searchTime, indexFailures, searchFailures, indexTotal, searchTotal,
            expAvgCheckpointDurationMs, expAvgDocumentsIndexed, expAvgDocumentsProcessed);
    }

    public static DataFrameIndexerTransformStats fromXContent(XContentParser parser) {
        try {
            return LENIENT_PARSER.parse(parser, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
