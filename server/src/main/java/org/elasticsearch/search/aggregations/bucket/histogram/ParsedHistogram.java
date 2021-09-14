/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;

import java.io.IOException;
import java.util.List;

public class ParsedHistogram extends ParsedMultiBucketAggregation<ParsedHistogram.ParsedBucket> implements Histogram {

    @Override
    public String getType() {
        return HistogramAggregationBuilder.NAME;
    }

    @Override
    public List<? extends Histogram.Bucket> getBuckets() {
        return buckets;
    }

    private static final ObjectParser<ParsedHistogram, Void> PARSER = new ObjectParser<>(
        ParsedHistogram.class.getSimpleName(),
        true,
        ParsedHistogram::new
    );
    static {
        declareMultiBucketAggregationFields(
            PARSER,
            parser -> ParsedBucket.fromXContent(parser, false),
            parser -> ParsedBucket.fromXContent(parser, true)
        );
    }

    public static ParsedHistogram fromXContent(XContentParser parser, String name) throws IOException {
        ParsedHistogram aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

    static class ParsedBucket extends ParsedMultiBucketAggregation.ParsedBucket implements Histogram.Bucket {

        private Double key;

        @Override
        public Object getKey() {
            return key;
        }

        @Override
        public String getKeyAsString() {
            String keyAsString = super.getKeyAsString();
            if (keyAsString != null) {
                return keyAsString;
            }
            if (key != null) {
                return Double.toString(key);
            }
            return null;
        }

        static ParsedBucket fromXContent(XContentParser parser, boolean keyed) throws IOException {
            return parseXContent(parser, keyed, ParsedBucket::new, (p, bucket) -> bucket.key = p.doubleValue());
        }
    }
}
