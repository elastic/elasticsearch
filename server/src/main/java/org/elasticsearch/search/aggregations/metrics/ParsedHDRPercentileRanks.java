/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class ParsedHDRPercentileRanks extends ParsedPercentileRanks {

    @Override
    public String getType() {
        return InternalHDRPercentileRanks.NAME;
    }

    private static final ObjectParser<ParsedHDRPercentileRanks, Void> PARSER = new ObjectParser<>(
        ParsedHDRPercentileRanks.class.getSimpleName(),
        true,
        ParsedHDRPercentileRanks::new
    );
    static {
        ParsedPercentiles.declarePercentilesFields(PARSER);
    }

    public static ParsedHDRPercentileRanks fromXContent(XContentParser parser, String name) throws IOException {
        ParsedHDRPercentileRanks aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }
}
