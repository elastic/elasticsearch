/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.range.geodistance;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.range.AbstractRangeBase;
import org.elasticsearch.search.aggregations.support.numeric.ValueFormatter;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class InternalGeoDistance extends AbstractRangeBase<GeoDistance.Bucket> implements GeoDistance {

    public static final Type TYPE = new Type("geo_distance", "gdist");

    public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalGeoDistance readResult(StreamInput in) throws IOException {
            InternalGeoDistance geoDistance = new InternalGeoDistance();
            geoDistance.readFrom(in);
            return geoDistance;
        }
    };

    public static void registerStream() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    public static final Factory FACTORY = new Factory();

    static class Bucket extends AbstractRangeBase.Bucket implements GeoDistance.Bucket {

        Bucket(String key, double from, double to, long docCount, List<InternalAggregation> aggregations, ValueFormatter formatter) {
            this(key, from, to, docCount, new InternalAggregations(aggregations), formatter);
        }

        Bucket(String key, double from, double to, long docCount, InternalAggregations aggregations, ValueFormatter formatter) {
            super(key, from, to, docCount, aggregations, formatter);
        }

    }

    private static class Factory implements AbstractRangeBase.Factory<GeoDistance.Bucket> {

        @Override
        public String type() {
            return TYPE.name();
        }

        @Override
        public AbstractRangeBase<GeoDistance.Bucket> create(String name, List<GeoDistance.Bucket> buckets, ValueFormatter formatter, boolean keyed) {
            return new InternalGeoDistance(name, buckets, formatter, keyed);
        }

        @Override
        public Bucket createBucket(String key, double from, double to, long docCount, InternalAggregations aggregations, ValueFormatter formatter) {
            return new Bucket(key, from, to, docCount, aggregations, formatter);
        }
    }

    InternalGeoDistance() {} // for serialization

    public InternalGeoDistance(String name, List<GeoDistance.Bucket> ranges, ValueFormatter formatter, boolean keyed) {
        super(name, ranges, formatter, keyed);
    }

    @Override
    protected Bucket createBucket(String key, double from, double to, long docCount, InternalAggregations aggregations, ValueFormatter formatter) {
        return new Bucket(key, from, to, docCount, aggregations, formatter);
    }

    @Override
    public Type type() {
        return TYPE;
    }
}