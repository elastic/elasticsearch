/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations.bucket.adjacency;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ParsedAdjacencyMatrix extends ParsedMultiBucketAggregation<ParsedAdjacencyMatrix.ParsedBucket> implements AdjacencyMatrix {

    private Map<String, ParsedBucket> bucketMap;

    @Override
    public String getType() {
        return AdjacencyMatrixAggregationBuilder.NAME;
    }

    @Override
    public List<? extends AdjacencyMatrix.Bucket> getBuckets() {
        return buckets;
    }

    @Override
    public ParsedBucket getBucketByKey(String key) {
        if (bucketMap == null) {
            bucketMap = Maps.newMapWithExpectedSize(buckets.size());
            for (ParsedBucket bucket : buckets) {
                bucketMap.put(bucket.getKey(), bucket);
            }
        }
        return bucketMap.get(key);
    }

    private static final ObjectParser<ParsedAdjacencyMatrix, Void> PARSER = new ObjectParser<>(
        ParsedAdjacencyMatrix.class.getSimpleName(),
        true,
        ParsedAdjacencyMatrix::new
    );
    static {
        declareMultiBucketAggregationFields(
            PARSER,
            parser -> ParsedBucket.fromXContent(parser),
            parser -> ParsedBucket.fromXContent(parser)
        );
    }

    public static ParsedAdjacencyMatrix fromXContent(XContentParser parser, String name) throws IOException {
        ParsedAdjacencyMatrix aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

    public static class ParsedBucket extends ParsedMultiBucketAggregation.ParsedBucket implements AdjacencyMatrix.Bucket {

        private String key;

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public String getKeyAsString() {
            return key;
        }

        static ParsedBucket fromXContent(XContentParser parser) throws IOException {
            return parseXContent(parser, false, ParsedBucket::new, (p, bucket) -> bucket.key = p.text());
        }
    }
}
