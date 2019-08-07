/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.CheckedFunction;
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
