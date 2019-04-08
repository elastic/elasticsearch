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
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DataFrameIndexerTransformStats extends IndexerJobStats {

    public static ParseField CURRENT_RUN_DOCUMENTS_PROCESSED = new ParseField("current_run_documents_processed");
    public static ParseField CURRENT_RUN_TOTAL_DOCUMENTS_TO_PROCESS = new ParseField("current_run_total_documents_to_process");
    public static ParseField CURRENT_RUN_START_TIME = new ParseField("current_run_start_time");

    public static final ConstructingObjectParser<DataFrameIndexerTransformStats, Void> LENIENT_PARSER = new ConstructingObjectParser<>(
        NAME, true, args -> new DataFrameIndexerTransformStats((long) args[0], (long) args[1], (long) args[2],
        (long) args[3], (long) args[4], (long) args[5], (long) args[6], (long) args[7], (long) args[8], (long) args[9],
        (Long) args[10], (Long) args[11], (Date) args[12]));

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
        LENIENT_PARSER.declareLong(optionalConstructorArg(), CURRENT_RUN_DOCUMENTS_PROCESSED);
        LENIENT_PARSER.declareLong(optionalConstructorArg(), CURRENT_RUN_TOTAL_DOCUMENTS_TO_PROCESS);
        LENIENT_PARSER.declareField(optionalConstructorArg(),
            p -> {
                if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                    return new Date(p.longValue());
                }
                throw new IllegalArgumentException(
                    "unexpected token [" + p.currentToken() + "] for [" + CURRENT_RUN_START_TIME.getPreferredName() + "]");
            }, CURRENT_RUN_START_TIME, ObjectParser.ValueType.VALUE);
    }

    public static DataFrameIndexerTransformStats fromXContent(XContentParser parser) throws IOException {
        return LENIENT_PARSER.parse(parser, null);
    }

    private final Long currentRunDocsProcessed;
    private final Long currentRunTotalDocsToProcess;
    private final Date currentRunStartTime;

    public DataFrameIndexerTransformStats(long numPages, long numInputDocuments, long numOuputDocuments,
                                          long numInvocations, long indexTime, long searchTime,
                                          long indexTotal, long searchTotal, long indexFailures, long searchFailures,
                                          Long currentRunDocsProcessed, Long currentRunTotalDocsToProcess,
                                          Date currentRunStartTime) {
        super(numPages, numInputDocuments, numOuputDocuments, numInvocations, indexTime, searchTime,
                indexTotal, searchTotal, indexFailures, searchFailures);
        this.currentRunTotalDocsToProcess = currentRunTotalDocsToProcess;
        this.currentRunDocsProcessed = currentRunDocsProcessed;
        this.currentRunStartTime = currentRunStartTime;
    }

    public Long getCurrentRunDocsProcessed() {
        return currentRunDocsProcessed;
    }

    public double getCurrentPercentageComplete() {
        if (currentRunTotalDocsToProcess == null || currentRunDocsProcessed == null) {
            return 0.0;
        } else if(currentRunDocsProcessed >= currentRunTotalDocsToProcess) {
            return 1.0;
        }
        return (double)currentRunDocsProcessed/currentRunTotalDocsToProcess;
    }

    public Long getCurrentRunTotalDocsToProcess() {
        return currentRunTotalDocsToProcess;
    }

    public Date getCurrentRunStartTime() {
        return currentRunStartTime;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof DataFrameIndexerTransformStats == false) {
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
            && Objects.equals(this.searchTotal, that.searchTotal)
            && Objects.equals(this.indexTotal, that.indexTotal)
            && Objects.equals(this.currentRunDocsProcessed, that.currentRunDocsProcessed)
            && Objects.equals(this.currentRunTotalDocsToProcess, that.currentRunTotalDocsToProcess)
            && Objects.equals(this.currentRunStartTime, that.currentRunStartTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numPages, numInputDocuments, numOuputDocuments, numInvocations,
            indexTime, searchTime, indexFailures, searchFailures, searchTotal, indexTotal, currentRunDocsProcessed,
            currentRunTotalDocsToProcess, currentRunStartTime);
    }

}
