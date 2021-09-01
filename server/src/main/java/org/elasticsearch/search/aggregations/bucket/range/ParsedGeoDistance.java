/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class ParsedGeoDistance extends ParsedRange {

    @Override
    public String getType() {
        return GeoDistanceAggregationBuilder.NAME;
    }

    private static final ObjectParser<ParsedGeoDistance, Void> PARSER = new ObjectParser<>(
        ParsedGeoDistance.class.getSimpleName(),
        true,
        ParsedGeoDistance::new
    );
    static {
        declareParsedRangeFields(
            PARSER,
            parser -> ParsedBucket.fromXContent(parser, false),
            parser -> ParsedBucket.fromXContent(parser, true)
        );
    }

    public static ParsedGeoDistance fromXContent(XContentParser parser, String name) throws IOException {
        ParsedGeoDistance aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

    public static class ParsedBucket extends ParsedRange.ParsedBucket {

        static ParsedBucket fromXContent(final XContentParser parser, final boolean keyed) throws IOException {
            return parseRangeBucketXContent(parser, ParsedBucket::new, keyed);
        }
    }
}
