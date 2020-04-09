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

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.client.core.IndexerJobStats;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TransformIndexerStats extends IndexerJobStats {

    static ParseField EXPONENTIAL_AVG_CHECKPOINT_DURATION_MS = new ParseField("exponential_avg_checkpoint_duration_ms");
    static ParseField EXPONENTIAL_AVG_DOCUMENTS_INDEXED = new ParseField("exponential_avg_documents_indexed");
    static ParseField EXPONENTIAL_AVG_DOCUMENTS_PROCESSED = new ParseField("exponential_avg_documents_processed");

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
            unboxSafe(args[12], 0.0),
            unboxSafe(args[13], 0.0),
            unboxSafe(args[14], 0.0)
        )
    );

    static {
        LENIENT_PARSER.declareLong(optionalConstructorArg(), NUM_PAGES);
        LENIENT_PARSER.declareLong(optionalConstructorArg(), NUM_INPUT_DOCUMENTS);
        LENIENT_PARSER.declareLong(optionalConstructorArg(), NUM_OUTPUT_DOCUMENTS);
        LENIENT_PARSER.declareLong(optionalConstructorArg(), NUM_INVOCATIONS);
        LENIENT_PARSER.declareLong(optionalConstructorArg(), INDEX_TIME_IN_MS);
        LENIENT_PARSER.declareLong(optionalConstructorArg(), SEARCH_TIME_IN_MS);
        LENIENT_PARSER.declareLong(optionalConstructorArg(), PROCESSING_TIME_IN_MS);
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

    public TransformIndexerStats(
        long numPages,
        long numInputDocuments,
        long numOuputDocuments,
        long numInvocations,
        long indexTime,
        long searchTime,
        long processingTime,
        long indexTotal,
        long searchTotal,
        long processingTotal,
        long indexFailures,
        long searchFailures,
        double expAvgCheckpointDurationMs,
        double expAvgDocumentsIndexed,
        double expAvgDocumentsProcessed
    ) {
        super(
            numPages,
            numInputDocuments,
            numOuputDocuments,
            numInvocations,
            indexTime,
            searchTime,
            processingTime,
            indexTotal,
            searchTotal,
            processingTotal,
            indexFailures,
            searchFailures
        );
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

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        TransformIndexerStats that = (TransformIndexerStats) other;

        return Objects.equals(this.numPages, that.numPages)
            && Objects.equals(this.numInputDocuments, that.numInputDocuments)
            && Objects.equals(this.numOuputDocuments, that.numOuputDocuments)
            && Objects.equals(this.numInvocations, that.numInvocations)
            && Objects.equals(this.indexTime, that.indexTime)
            && Objects.equals(this.searchTime, that.searchTime)
            && Objects.equals(this.processingTime, that.processingTime)
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
            numPages,
            numInputDocuments,
            numOuputDocuments,
            numInvocations,
            indexTime,
            searchTime,
            processingTime,
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
