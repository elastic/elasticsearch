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
import org.elasticsearch.search.aggregations.metrics.ParsedStats;

public class ParsedStatsBucket extends ParsedStats implements StatsBucket {

    @Override
    public String getType() {
        return StatsBucketPipelineAggregationBuilder.NAME;
    }

    private static final ObjectParser<ParsedStatsBucket, Void> PARSER = new ObjectParser<>(
        ParsedStatsBucket.class.getSimpleName(),
        true,
        ParsedStatsBucket::new
    );

    static {
        declareStatsFields(PARSER);
    }

    public static ParsedStatsBucket fromXContent(XContentParser parser, final String name) {
        ParsedStatsBucket parsedStatsBucket = PARSER.apply(parser, null);
        parsedStatsBucket.setName(name);
        return parsedStatsBucket;
    }
}
