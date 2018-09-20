/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.common.ParseField;
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

    public static final ConstructingObjectParser<RollupIndexerJobStats, Void> PARSER =
        new ConstructingObjectParser<>(NAME.getPreferredName(),
            args -> new RollupIndexerJobStats((long) args[0], (long) args[1], (long) args[2], (long) args[3]));

    static {
        PARSER.declareLong(constructorArg(), NUM_PAGES);
        PARSER.declareLong(constructorArg(), NUM_INPUT_DOCUMENTS);
        PARSER.declareLong(constructorArg(), NUM_OUTPUT_DOCUMENTS);
        PARSER.declareLong(constructorArg(), NUM_INVOCATIONS);
    }

    public RollupIndexerJobStats() {
        super();
    }

    public RollupIndexerJobStats(long numPages, long numInputDocuments, long numOuputDocuments, long numInvocations) {
        super(numPages, numInputDocuments, numOuputDocuments, numInvocations);
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
