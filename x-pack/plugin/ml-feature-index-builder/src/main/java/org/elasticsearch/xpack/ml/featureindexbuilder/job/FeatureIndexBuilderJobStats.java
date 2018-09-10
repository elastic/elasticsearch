/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.job;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.indexing.IndexerJobStats;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class FeatureIndexBuilderJobStats extends IndexerJobStats {
    private static ParseField NUM_PAGES = new ParseField("pages_processed");
    private static ParseField NUM_INPUT_DOCUMENTS = new ParseField("documents_processed");
    private static ParseField NUM_OUTPUT_DOCUMENTS = new ParseField("documents_indexed");
    private static ParseField NUM_INVOCATIONS = new ParseField("trigger_count");

    public static final ConstructingObjectParser<FeatureIndexBuilderJobStats, Void> PARSER = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            args -> new FeatureIndexBuilderJobStats((long) args[0], (long) args[1], (long) args[2], (long) args[3]));

    static {
        PARSER.declareLong(constructorArg(), NUM_PAGES);
        PARSER.declareLong(constructorArg(), NUM_INPUT_DOCUMENTS);
        PARSER.declareLong(constructorArg(), NUM_OUTPUT_DOCUMENTS);
        PARSER.declareLong(constructorArg(), NUM_INVOCATIONS);
    }

    public FeatureIndexBuilderJobStats() {
        super();
    }

    public FeatureIndexBuilderJobStats(long numPages, long numInputDocuments, long numOuputDocuments, long numInvocations) {
        super(numPages, numInputDocuments, numOuputDocuments, numInvocations);
    }

    public FeatureIndexBuilderJobStats(StreamInput in) throws IOException {
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

    public static FeatureIndexBuilderJobStats fromXContent(XContentParser parser) {
        try {
            return PARSER.parse(parser, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
