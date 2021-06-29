/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.indexing.IndexerJobStats;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * The Rollup specialization of stats for the AsyncTwoPhaseIndexer.
 * Note: instead of `documents_indexed`, this XContent show `rollups_indexed`
 */
public class RollupIndexerJobStats extends IndexerJobStats {
    private static ParseField NUM_PAGES = new ParseField("pages_processed");
    private static ParseField NUM_INPUT_DOCUMENTS = new ParseField("documents_processed");
    private static ParseField NUM_OUTPUT_DOCUMENTS = new ParseField("rollups_indexed");
    private static ParseField NUM_INVOCATIONS = new ParseField("trigger_count");
    private static ParseField INDEX_TIME_IN_MS = new ParseField("index_time_in_ms");
    private static ParseField SEARCH_TIME_IN_MS = new ParseField("search_time_in_ms");
    private static ParseField PROCESSING_TIME_IN_MS = new ParseField("processing_time_in_ms");
    private static ParseField INDEX_TOTAL = new ParseField("index_total");
    private static ParseField SEARCH_TOTAL = new ParseField("search_total");
    private static ParseField PROCESSING_TOTAL = new ParseField("processing_total");
    private static ParseField SEARCH_FAILURES = new ParseField("search_failures");
    private static ParseField INDEX_FAILURES = new ParseField("index_failures");

    public static final ConstructingObjectParser<RollupIndexerJobStats, Void> PARSER =
        new ConstructingObjectParser<>(NAME.getPreferredName(),
            args -> new RollupIndexerJobStats((long) args[0], (long) args[1], (long) args[2], (long) args[3],
                (long) args[4], (long) args[5], (long) args[6], (long) args[7],  (long) args[8], (long) args[9],
                (long) args[10], (long) args[11]));

    static {
        PARSER.declareLong(constructorArg(), NUM_PAGES);
        PARSER.declareLong(constructorArg(), NUM_INPUT_DOCUMENTS);
        PARSER.declareLong(constructorArg(), NUM_OUTPUT_DOCUMENTS);
        PARSER.declareLong(constructorArg(), NUM_INVOCATIONS);
        PARSER.declareLong(constructorArg(), INDEX_TIME_IN_MS);
        PARSER.declareLong(constructorArg(), SEARCH_TIME_IN_MS);
        PARSER.declareLong(constructorArg(), PROCESSING_TIME_IN_MS);
        PARSER.declareLong(constructorArg(), INDEX_TOTAL);
        PARSER.declareLong(constructorArg(), SEARCH_TOTAL);
        PARSER.declareLong(constructorArg(), PROCESSING_TOTAL);
        PARSER.declareLong(constructorArg(), INDEX_FAILURES);
        PARSER.declareLong(constructorArg(), SEARCH_FAILURES);
    }

    public RollupIndexerJobStats() {
        super();
    }

    public RollupIndexerJobStats(long numPages, long numInputDocuments, long numOuputDocuments, long numInvocations,
                                 long indexTime, long searchTime, long processingTime, long indexTotal, long searchTotal,
                                 long processingTotal, long indexFailures, long searchFailures) {
        super(numPages, numInputDocuments, numOuputDocuments, numInvocations, indexTime, searchTime, processingTime,
            indexTotal, searchTotal, processingTotal, indexFailures, searchFailures);
    }

    public RollupIndexerJobStats(StreamInput in) throws IOException {
        super(in);
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
        builder.field(PROCESSING_TIME_IN_MS.getPreferredName(), processingTime);
        builder.field(PROCESSING_TOTAL.getPreferredName(), processingTotal);
        builder.endObject();
        return builder;
    }

    public static RollupIndexerJobStats fromXContent(XContentParser parser) {
        try {
            return PARSER.parse(parser, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
