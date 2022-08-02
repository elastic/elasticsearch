/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class ParsedDateRange extends ParsedRange {

    @Override
    public String getType() {
        return DateRangeAggregationBuilder.NAME;
    }

    private static final ObjectParser<ParsedDateRange, Void> PARSER = new ObjectParser<>(
        ParsedDateRange.class.getSimpleName(),
        true,
        ParsedDateRange::new
    );
    static {
        declareParsedRangeFields(
            PARSER,
            parser -> ParsedBucket.fromXContent(parser, false),
            parser -> ParsedBucket.fromXContent(parser, true)
        );
    }

    public static ParsedDateRange fromXContent(XContentParser parser, String name) throws IOException {
        ParsedDateRange aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

    public static class ParsedBucket extends ParsedRange.ParsedBucket {

        @Override
        public Object getFrom() {
            return doubleAsDateTime(from);
        }

        @Override
        public Object getTo() {
            return doubleAsDateTime(to);
        }

        private static ZonedDateTime doubleAsDateTime(Double d) {
            if (d == null || Double.isInfinite(d)) {
                return null;
            }
            return Instant.ofEpochMilli(d.longValue()).atZone(ZoneOffset.UTC);
        }

        static ParsedBucket fromXContent(final XContentParser parser, final boolean keyed) throws IOException {
            return parseRangeBucketXContent(parser, ParsedBucket::new, keyed);
        }
    }
}
