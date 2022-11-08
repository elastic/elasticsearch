/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations.bucket.timeseries;

import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ParsedTimeSeries extends ParsedMultiBucketAggregation<ParsedTimeSeries.ParsedBucket> {

    private transient Map<String, ParsedTimeSeries.ParsedBucket> bucketMap;

    @Override
    public String getType() {
        return TimeSeriesAggregationBuilder.NAME;
    }

    @Override
    public List<? extends ParsedBucket> getBuckets() {
        return buckets;
    }

    public ParsedBucket getBucketByKey(String key) {
        if (bucketMap == null) {
            bucketMap = new HashMap<>(buckets.size());
            for (ParsedTimeSeries.ParsedBucket bucket : buckets) {
                bucketMap.put(bucket.getKeyAsString(), bucket);
            }
        }
        return bucketMap.get(key);
    }

    private static final ObjectParser<ParsedTimeSeries, Void> PARSER = new ObjectParser<>(
        ParsedTimeSeries.class.getSimpleName(),
        true,
        ParsedTimeSeries::new
    );
    static {
        declareMultiBucketAggregationFields(
            PARSER,
            parser -> ParsedTimeSeries.ParsedBucket.fromXContent(parser, false),
            parser -> ParsedTimeSeries.ParsedBucket.fromXContent(parser, true)
        );
    }

    public static ParsedTimeSeries fromXContent(XContentParser parser, String name) throws IOException {
        ParsedTimeSeries aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

    static class ParsedBucket extends ParsedMultiBucketAggregation.ParsedBucket {

        private Map<String, Object> key;

        @Override
        public Object getKey() {
            return key;
        }

        @Override
        public String getKeyAsString() {
            return key.toString();
        }

        static ParsedTimeSeries.ParsedBucket fromXContent(XContentParser parser, boolean keyed) throws IOException {
            return parseXContent(parser, keyed, ParsedTimeSeries.ParsedBucket::new, (p, bucket) -> bucket.key = new TreeMap<>(p.map()));
        }
    }

}
