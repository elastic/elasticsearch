/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.dataframe.transforms;

import org.elasticsearch.client.core.IndexerJobStats;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DataFrameIndexerTransformStats extends IndexerJobStats {

    static ParseField EXPONENTIAL_AVG_CHECKPOINT_DURATION_MS = new ParseField("exponential_avg_checkpoint_duration_ms");
    static ParseField EXPONENTIAL_AVG_DOCUMENTS_INDEXED = new ParseField("exponential_avg_documents_indexed");
    static ParseField EXPONENTIAL_AVG_DOCUMENTS_PROCESSED = new ParseField("exponential_avg_documents_processed");

    public static final ConstructingObjectParser<DataFrameIndexerTransformStats, Void> LENIENT_PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        args -> new DataFrameIndexerTransformStats((long) args[0], (long) args[1], (long) args[2],
            (long) args[3], (long) args[4], (long) args[5], (long) args[6], (long) args[7], (long) args[8], (long) args[9],
            (Double) args[10], (Double) args[11], (Double) args[12]));

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

    public static DataFrameIndexerTransformStats fromXContent(XContentParser parser) throws IOException {
        return LENIENT_PARSER.parse(parser, null);
    }

    private final double expAvgCheckpointDurationMs;
    private final double expAvgDocumentsIndexed;
    private final double expAvgDocumentsProcessed;

    public DataFrameIndexerTransformStats(long numPages, long numInputDocuments, long numOuputDocuments,
                                          long numInvocations, long indexTime, long searchTime,
                                          long indexTotal, long searchTotal, long indexFailures, long searchFailures,
                                          Double expAvgCheckpointDurationMs, Double expAvgDocumentsIndexed,
                                          Double expAvgDocumentsProcessed) {
        super(numPages, numInputDocuments, numOuputDocuments, numInvocations, indexTime, searchTime,
                indexTotal, searchTotal, indexFailures, searchFailures);
        this.expAvgCheckpointDurationMs = expAvgCheckpointDurationMs == null ? 0.0 : expAvgCheckpointDurationMs;
        this.expAvgDocumentsIndexed = expAvgDocumentsIndexed == null ? 0.0 : expAvgDocumentsIndexed;
        this.expAvgDocumentsProcessed = expAvgDocumentsProcessed == null ? 0.0 : expAvgDocumentsProcessed;
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
}
