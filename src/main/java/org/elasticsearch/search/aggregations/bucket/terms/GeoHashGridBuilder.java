/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import java.io.IOException;

/**
 * Creates an aggregation based on bucketing points into GeoHashes
 *
 */
public class GeoHashGridBuilder extends AggregationBuilder<GeoHashGridBuilder> {


    private String field;
    private int precision=GeoHashGridParser.DEFAULT_PRECISION;
    private int maxNumCells=GeoHashGridParser.DEFAULT_MAX_NUM_CELLS;
    private int shardMaxNumCells=0;

    public GeoHashGridBuilder(String name) {
        super(name, GeoHashGridParser.aggTypeName);
    }

    public GeoHashGridBuilder field(String field) {
        this.field = field;
        return this;
    }

    public GeoHashGridBuilder precision(int precision) {
        if((precision<1)||(precision>12))
        {
            throw new IllegalArgumentException("Invalid geohash aggregation precision of "+precision
                    +"must be between 1 and 12");
        }
        this.precision = precision;
        return this;
    }
    public GeoHashGridBuilder maxNumCells(int maxNumCells) {
        this.maxNumCells = maxNumCells;
        return this;
    }
    public GeoHashGridBuilder shardMaxNumCells(int shardMaxNumCells) {
        this.shardMaxNumCells = shardMaxNumCells;
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (field != null) {
            builder.field("field", field);
        }
        if (precision != GeoHashGridParser.DEFAULT_PRECISION) {
            builder.field("precision", precision);
        }
        if (maxNumCells != GeoHashGridParser.DEFAULT_MAX_NUM_CELLS) {
            builder.field("maxNumCells", maxNumCells);
        }
        if (shardMaxNumCells != 0) {
            builder.field("shardMaxNumCells", shardMaxNumCells);
        }

        return builder.endObject();
    }

}
