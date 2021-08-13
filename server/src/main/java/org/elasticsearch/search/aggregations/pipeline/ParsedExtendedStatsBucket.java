/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.metrics.ParsedExtendedStats;


public class ParsedExtendedStatsBucket extends ParsedExtendedStats implements ExtendedStatsBucket {

    @Override
    public String getType() {
        return ExtendedStatsBucketPipelineAggregationBuilder.NAME;
    }

    private static final ObjectParser<ParsedExtendedStatsBucket, Void> PARSER = new ObjectParser<>(
            ParsedExtendedStatsBucket.class.getSimpleName(), true, ParsedExtendedStatsBucket::new);

    static {
        declareExtendedStatsFields(PARSER);
    }

    public static ParsedExtendedStatsBucket fromXContent(XContentParser parser, final String name) {
        ParsedExtendedStatsBucket parsedStatsBucket = PARSER.apply(parser, null);
        parsedStatsBucket.setName(name);
        return parsedStatsBucket;
    }
}
