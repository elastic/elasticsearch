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
package org.elasticsearch.search.aggregations.bucket.range.geodistance;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class InternalGeoDistance extends InternalRange<InternalGeoDistance.Bucket> implements GeoDistance {

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

    static class Bucket extends InternalRange.Bucket implements GeoDistance.Bucket {

        Bucket(String key, double from, double to, long docCount, List<InternalAggregation> aggregations, ValueFormatter formatter) {
            this(key, from, to, docCount, new InternalAggregations(aggregations), formatter);
        }

        Bucket(String key, double from, double to, long docCount, InternalAggregations aggregations, ValueFormatter formatter) {
            super(key, from, to, docCount, aggregations, formatter);
        }

        @Override
        protected InternalRange.Factory<Bucket, ?> getFactory() {
            return FACTORY;
        }
    }

    private static class Factory extends InternalRange.Factory<InternalGeoDistance.Bucket, InternalGeoDistance> {

        @Override
        public String type() {
            return TYPE.name();
        }

        @Override
        public InternalGeoDistance create(String name, List<Bucket> ranges, @Nullable ValueFormatter formatter, boolean keyed) {
            return new InternalGeoDistance(name, ranges, formatter, keyed);
        }

        @Override
        public Bucket createBucket(String key, double from, double to, long docCount, InternalAggregations aggregations, @Nullable ValueFormatter formatter) {
            return new Bucket(key, from, to, docCount, aggregations, formatter);
        }
    }

    InternalGeoDistance() {} // for serialization

    public InternalGeoDistance(String name, List<Bucket> ranges, @Nullable ValueFormatter formatter, boolean keyed) {
        super(name, ranges, formatter, keyed);
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    protected InternalRange.Factory<Bucket, ?> getFactory() {
        return FACTORY;
    }
}