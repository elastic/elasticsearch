/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

public abstract class ParsedGeoGrid extends ParsedMultiBucketAggregation<ParsedGeoGridBucket> implements GeoGrid {

    @Override
    public List<? extends GeoGrid.Bucket> getBuckets() {
        return buckets;
    }

    public static ObjectParser<ParsedGeoGrid, Void> createParser(Supplier<ParsedGeoGrid> supplier,
            CheckedFunction<XContentParser, ParsedBucket, IOException> bucketParser,
            CheckedFunction<XContentParser, ParsedBucket, IOException> keyedBucketParser) {
        ObjectParser<ParsedGeoGrid, Void> parser =  new ObjectParser<>(ParsedGeoGrid.class.getSimpleName(), true, supplier);
        declareMultiBucketAggregationFields(parser, bucketParser, keyedBucketParser);
        return parser;
    }

    protected void setName(String name) {
        super.setName(name);
    }
}
