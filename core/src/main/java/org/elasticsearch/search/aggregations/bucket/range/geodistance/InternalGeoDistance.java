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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.BucketStreamContext;
import org.elasticsearch.search.aggregations.bucket.BucketStreams;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class InternalGeoDistance extends InternalRange<InternalGeoDistance.Bucket, InternalGeoDistance> {

    public static final Type TYPE = new Type("geo_distance", "gdist");

    public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalGeoDistance readResult(StreamInput in) throws IOException {
            InternalGeoDistance geoDistance = new InternalGeoDistance();
            geoDistance.readFrom(in);
            return geoDistance;
        }
    };

    private final static BucketStreams.Stream<Bucket> BUCKET_STREAM = new BucketStreams.Stream<Bucket>() {
        @Override
        public Bucket readResult(StreamInput in, BucketStreamContext context) throws IOException {
            Bucket buckets = new Bucket(context.keyed(), context.formatter());
            buckets.readFrom(in);
            return buckets;
        }

        @Override
        public BucketStreamContext getBucketStreamContext(Bucket bucket) {
            BucketStreamContext context = new BucketStreamContext();
            context.formatter(bucket.formatter());
            context.keyed(bucket.keyed());
            return context;
        }
    };

    public static void registerStream() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
        BucketStreams.registerStream(BUCKET_STREAM, TYPE.stream());
    }

    public static final Factory FACTORY = new Factory();

    static class Bucket extends InternalRange.Bucket {

        Bucket(boolean keyed, ValueFormatter formatter) {
            super(keyed, formatter);
        }

        Bucket(String key, double from, double to, long docCount, List<InternalAggregation> aggregations, boolean keyed,
                ValueFormatter formatter) {
            this(key, from, to, docCount, new InternalAggregations(aggregations), keyed, formatter);
        }

        Bucket(String key, double from, double to, long docCount, InternalAggregations aggregations, boolean keyed, ValueFormatter formatter) {
            super(key, from, to, docCount, aggregations, keyed, formatter);
        }

        @Override
        protected InternalRange.Factory<Bucket, ?> getFactory() {
            return FACTORY;
        }

        boolean keyed() {
            return keyed;
        }

        ValueFormatter formatter() {
            return formatter;
        }
    }

    public static class Factory extends InternalRange.Factory<InternalGeoDistance.Bucket, InternalGeoDistance> {

        @Override
        public String type() {
            return TYPE.name();
        }

        @Override
        public InternalGeoDistance create(String name, List<Bucket> ranges, ValueFormatter formatter, boolean keyed,
                List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
            return new InternalGeoDistance(name, ranges, formatter, keyed, pipelineAggregators, metaData);
        }

        @Override
        public InternalGeoDistance create(List<Bucket> ranges, InternalGeoDistance prototype) {
            return new InternalGeoDistance(prototype.name, ranges, prototype.formatter, prototype.keyed, prototype.pipelineAggregators(),
                    prototype.metaData);
        }

        @Override
        public Bucket createBucket(String key, double from, double to, long docCount, InternalAggregations aggregations, boolean keyed,
                ValueFormatter formatter) {
            return new Bucket(key, from, to, docCount, aggregations, keyed, formatter);
        }

        @Override
        public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
            return new Bucket(prototype.getKey(), ((Number) prototype.getFrom()).doubleValue(), ((Number) prototype.getTo()).doubleValue(),
                    prototype.getDocCount(), aggregations, prototype.getKeyed(), prototype.getFormatter());
        }
    }

    InternalGeoDistance() {} // for serialization

    public InternalGeoDistance(String name, List<Bucket> ranges, ValueFormatter formatter, boolean keyed,
            List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        super(name, ranges, formatter, keyed, pipelineAggregators, metaData);
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalRange.Factory<Bucket, InternalGeoDistance> getFactory() {
        return FACTORY;
    }
}