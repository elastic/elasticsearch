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

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class DataFrameIndexerTransformStats extends IndexerJobStats {

    public static ParseField CURRENT_RUN_DOCUMENTS_PROCESSED = new ParseField("current_run_documents_processed");
    public static ParseField CURRENT_RUN_TOTAL_DOCUMENTS_TO_PROCESS = new ParseField("current_run_total_documents_to_process");

    public static final ConstructingObjectParser<DataFrameIndexerTransformStats, Void> LENIENT_PARSER = new ConstructingObjectParser<>(
        NAME, true, args -> new DataFrameIndexerTransformStats((long) args[0], (long) args[1], (long) args[2],
        (long) args[3], (long) args[4], (long) args[5], (long) args[6], (long) args[7], (long) args[8], (long) args[9],
        (long) args[10], (long) args[11]));

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
        LENIENT_PARSER.declareLong(constructorArg(), CURRENT_RUN_DOCUMENTS_PROCESSED);
        LENIENT_PARSER.declareLong(constructorArg(), CURRENT_RUN_TOTAL_DOCUMENTS_TO_PROCESS);
    }

    public static DataFrameIndexerTransformStats fromXContent(XContentParser parser) throws IOException {
        return LENIENT_PARSER.parse(parser, null);
    }

    private final long currentRunDocsProcessed;
    private final long currentRunTotalDocsToProcess;

    public DataFrameIndexerTransformStats(long numPages, long numInputDocuments, long numOuputDocuments,
                                          long numInvocations, long indexTime, long searchTime,
                                          long indexTotal, long searchTotal, long indexFailures, long searchFailures,
                                          long currentRunDocsProcessed, long currentRunTotalDocsToProcess) {
        super(numPages, numInputDocuments, numOuputDocuments, numInvocations, indexTime, searchTime,
                indexTotal, searchTotal, indexFailures, searchFailures);
        this.currentRunTotalDocsToProcess = currentRunTotalDocsToProcess;
        this.currentRunDocsProcessed = currentRunDocsProcessed;
    }

    public long getCurrentRunDocsProcessed() {
        return currentRunDocsProcessed;
    }

    public double getCurrentPercentageComplete() {
        if (currentRunTotalDocsToProcess == 0) {
            return 0.0;
        } else if(currentRunDocsProcessed >= currentRunTotalDocsToProcess) {
            return 1.0;
        }
        return (double)currentRunDocsProcessed/currentRunTotalDocsToProcess;
    }

    public long getCurrentRunTotalDocsToProcess() {
        return currentRunTotalDocsToProcess;
    }
}
