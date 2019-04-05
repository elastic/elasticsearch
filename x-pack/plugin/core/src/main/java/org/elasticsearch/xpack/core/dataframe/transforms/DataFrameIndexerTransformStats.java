/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.indexing.IndexerJobStats;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DataFrameIndexerTransformStats extends IndexerJobStats {
    public static final String DEFAULT_TRANSFORM_ID = "_all";

    public static final String NAME = "data_frame_indexer_transform_stats";
    public static final String DOCUMENTS_PROCESSED_PERCENTAGE = "documents_processed_percentage";

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
    public static ParseField CURRENT_RUN_DOCUMENTS_PROCESSED = new ParseField("current_run_documents_processed");
    public static ParseField CURRENT_RUN_TOTAL_DOCUMENTS_TO_PROCESS = new ParseField("current_run_total_documents_to_process");
    private static final ConstructingObjectParser<DataFrameIndexerTransformStats, Void> LENIENT_PARSER = new ConstructingObjectParser<>(
            NAME, true,
            args -> new DataFrameIndexerTransformStats(args[0] != null ? (String) args[0] : DEFAULT_TRANSFORM_ID,
                    (long) args[1], (long) args[2], (long) args[3], (long) args[4], (long) args[5], (long) args[6], (long) args[7],
                    (long) args[8], (long) args[9], (long) args[10], (long) args[11], (long) args[12]));

    static {
        LENIENT_PARSER.declareString(optionalConstructorArg(), DataFrameField.ID);
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
        LENIENT_PARSER.declareLong(constructorArg(), CURRENT_RUN_DOCUMENTS_PROCESSED);
        LENIENT_PARSER.declareLong(constructorArg(), CURRENT_RUN_TOTAL_DOCUMENTS_TO_PROCESS);
        LENIENT_PARSER.declareString(optionalConstructorArg(), DataFrameField.INDEX_DOC_TYPE);
    }

    private final String transformId;
    private long currentRunDocsProcessed;
    private long currentRunTotalDocsToProcess;

    /**
     * Certain situations call for a default transform ID, e.g. when merging many different transforms for statistics gather.
     *
     * The returned stats object cannot be stored in the index as the transformId does not refer to a real transform configuration
     *
     * @return new DataFrameIndexerTransformStats with empty stats and a default transform ID
     */
    public static DataFrameIndexerTransformStats withDefaultTransformId() {
        return new DataFrameIndexerTransformStats(DEFAULT_TRANSFORM_ID);
    }

    public static DataFrameIndexerTransformStats withDefaultTransformId(long numPages, long numInputDocuments, long numOutputDocuments,
                                                                        long numInvocations, long indexTime, long searchTime,
                                                                        long indexTotal, long searchTotal, long indexFailures,
                                                                        long searchFailures, long currentRunDocsProcessed,
                                                                        long currentRunTotalDocsToProcess) {
        return new DataFrameIndexerTransformStats(DEFAULT_TRANSFORM_ID, numPages, numInputDocuments,
            numOutputDocuments, numInvocations, indexTime, searchTime, indexTotal, searchTotal,
            indexFailures, searchFailures, currentRunDocsProcessed, currentRunTotalDocsToProcess);
    }

    public DataFrameIndexerTransformStats(String transformId) {
        super();
        this.transformId = Objects.requireNonNull(transformId, "parameter transformId must not be null");
        this.currentRunDocsProcessed = 0;
        this.currentRunTotalDocsToProcess = 0;
    }

    public DataFrameIndexerTransformStats(String transformId, long numPages, long numInputDocuments, long numOutputDocuments,
                                          long numInvocations, long indexTime, long searchTime, long indexTotal, long searchTotal,
                                          long indexFailures, long searchFailures, long currentRunDocsProcessed,
                                          long currentRunTotalDocsToProcess) {
        super(numPages, numInputDocuments, numOutputDocuments, numInvocations, indexTime, searchTime, indexTotal, searchTotal,
            indexFailures, searchFailures);
        this.transformId = Objects.requireNonNull(transformId, "parameter transformId must not be null");
        this.currentRunDocsProcessed = currentRunDocsProcessed;
        this.currentRunTotalDocsToProcess = currentRunTotalDocsToProcess;
    }

    public DataFrameIndexerTransformStats(DataFrameIndexerTransformStats other) {
        this(other.transformId, other.numPages, other.numInputDocuments, other.numOuputDocuments, other.numInvocations,
            other.indexTime, other.searchTime, other.indexTotal, other.searchTotal, other.indexFailures, other.searchFailures,
            other.currentRunDocsProcessed, other.currentRunTotalDocsToProcess);
    }

    public DataFrameIndexerTransformStats(StreamInput in) throws IOException {
        super(in);
        transformId = in.readString();
        currentRunDocsProcessed = in.readLong();
        currentRunTotalDocsToProcess = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(transformId);
        out.writeLong(currentRunDocsProcessed);
        out.writeLong(currentRunTotalDocsToProcess);
    }

    /**
     * Get the persisted stats document name from the Data Frame Transformer Id.
     *
     * @return The id of document the where the transform stats are persisted
     */
    public static String documentId(String transformId) {
        return NAME + "-" + transformId;
    }

    @Nullable
    public String getTransformId() {
        return transformId;
    }

    public long getCurrentRunDocsProcessed() {
        return currentRunDocsProcessed;
    }

    public double getCurrentPercentageComplete() {
        if (currentRunTotalDocsToProcess == 0) {
            return 1.0;
        } else if(currentRunDocsProcessed >= currentRunTotalDocsToProcess) {
            return 1.0;
        }
        return (double)currentRunDocsProcessed/currentRunTotalDocsToProcess;
    }

    public long getCurrentRunTotalDocsToProcess() {
        return currentRunTotalDocsToProcess;
    }

    public void setCurrentRunTotalDocumentsToProcess(long documentsToProcess) {
        this.currentRunTotalDocsToProcess = documentsToProcess;
    }

    public void resetCurrentRunDocsProcessed() {
        this.currentRunDocsProcessed = 0;
    }

    public void incrementCurrentRunDocsProcessed(long currentRunDocsProcessed) {
        this.currentRunDocsProcessed += currentRunDocsProcessed;
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
        builder.field(CURRENT_RUN_DOCUMENTS_PROCESSED.getPreferredName(), currentRunDocsProcessed);
        builder.field(CURRENT_RUN_TOTAL_DOCUMENTS_TO_PROCESS.getPreferredName(), currentRunTotalDocsToProcess);
        if (params.paramAsBoolean(DataFrameField.FOR_INTERNAL_STORAGE, false)) {
            // If we are storing something, it should have a valid transform ID.
            if (transformId.equals(DEFAULT_TRANSFORM_ID)) {
                throw new IllegalArgumentException("when storing transform statistics, a valid transform id must be provided");
            }
            builder.field(DataFrameField.ID.getPreferredName(), transformId);
            builder.field(DataFrameField.INDEX_DOC_TYPE.getPreferredName(), NAME);
        } else {
            builder.field(DOCUMENTS_PROCESSED_PERCENTAGE, getCurrentPercentageComplete());
        }
        builder.endObject();
        return builder;
    }

    public DataFrameIndexerTransformStats merge(DataFrameIndexerTransformStats other) {
        // We should probably not merge two sets of stats unless one is an accumulation object (i.e. with the default transform id)
        // or the stats are referencing the same transform
        assert transformId.equals(DEFAULT_TRANSFORM_ID) || this.transformId.equals(other.transformId);
        numPages += other.numPages;
        numInputDocuments += other.numInputDocuments;
        numOuputDocuments += other.numOuputDocuments;
        numInvocations += other.numInvocations;
        indexTime += other.indexTime;
        searchTime += other.searchTime;
        indexTotal += other.indexTotal;
        searchTotal += other.searchTotal;
        indexFailures += other.indexFailures;
        searchFailures += other.searchFailures;
        currentRunTotalDocsToProcess += other.currentRunTotalDocsToProcess;
        currentRunDocsProcessed += other.currentRunDocsProcessed;
        return this;
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

        return Objects.equals(this.transformId, that.transformId)
            && Objects.equals(this.numPages, that.numPages)
            && Objects.equals(this.numInputDocuments, that.numInputDocuments)
            && Objects.equals(this.numOuputDocuments, that.numOuputDocuments)
            && Objects.equals(this.numInvocations, that.numInvocations)
            && Objects.equals(this.indexTime, that.indexTime)
            && Objects.equals(this.searchTime, that.searchTime)
            && Objects.equals(this.indexFailures, that.indexFailures)
            && Objects.equals(this.searchFailures, that.searchFailures)
            && Objects.equals(this.indexTotal, that.indexTotal)
            && Objects.equals(this.searchTotal, that.searchTotal);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transformId, numPages, numInputDocuments, numOuputDocuments, numInvocations,
            indexTime, searchTime, indexFailures, searchFailures, indexTotal, searchTotal);
    }

    public static DataFrameIndexerTransformStats fromXContent(XContentParser parser) {
        try {
            return LENIENT_PARSER.parse(parser, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
