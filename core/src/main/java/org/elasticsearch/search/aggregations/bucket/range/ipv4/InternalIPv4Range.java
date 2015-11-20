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
package org.elasticsearch.search.aggregations.bucket.range.ipv4;

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

import static org.elasticsearch.index.mapper.ip.IpFieldMapper.MAX_IP;

/**
 *
 */
public class InternalIPv4Range extends InternalRange<InternalIPv4Range.Bucket, InternalIPv4Range> {

    public final static Type TYPE = new Type("ip_range", "iprange");

    private final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalIPv4Range readResult(StreamInput in) throws IOException {
            InternalIPv4Range range = new InternalIPv4Range();
            range.readFrom(in);
            return range;
        }
    };

    private final static BucketStreams.Stream<Bucket> BUCKET_STREAM = new BucketStreams.Stream<Bucket>() {
        @Override
        public Bucket readResult(StreamInput in, BucketStreamContext context) throws IOException {
            Bucket buckets = new Bucket(context.keyed());
            buckets.readFrom(in);
            return buckets;
        }

        @Override
        public BucketStreamContext getBucketStreamContext(Bucket bucket) {
            BucketStreamContext context = new BucketStreamContext();
            context.keyed(bucket.keyed());
            return context;
        }
    };

    public static void registerStream() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
        BucketStreams.registerStream(BUCKET_STREAM, TYPE.stream());
    }

    public static final Factory FACTORY = new Factory();

    public static class Bucket extends InternalRange.Bucket {

        public Bucket(boolean keyed) {
            super(keyed, ValueFormatter.IPv4);
        }

        public Bucket(String key, double from, double to, long docCount, List<InternalAggregation> aggregations, boolean keyed) {
            super(key, from, to, docCount, new InternalAggregations(aggregations), keyed, ValueFormatter.IPv4);
        }

        public Bucket(String key, double from, double to, long docCount, InternalAggregations aggregations, boolean keyed) {
            super(key, from, to, docCount, aggregations, keyed, ValueFormatter.IPv4);
        }

        @Override
        public String getFromAsString() {
            double from = ((Number) this.from).doubleValue();
            return Double.isInfinite(from) ? null : from == 0 ? null : ValueFormatter.IPv4.format(from);
        }

        @Override
        public String getToAsString() {
            double to = ((Number) this.to).doubleValue();
            return Double.isInfinite(to) ? null : MAX_IP == to ? null : ValueFormatter.IPv4.format(to);
        }

        @Override
        protected InternalRange.Factory<Bucket, ?> getFactory() {
            return FACTORY;
        }

        boolean keyed() {
            return keyed;
        }
    }

    public static class Factory extends InternalRange.Factory<InternalIPv4Range.Bucket, InternalIPv4Range> {

        @Override
        public String type() {
            return TYPE.name();
        }

        @Override
        public InternalIPv4Range create(String name, List<Bucket> ranges, ValueFormatter formatter, boolean keyed,
                List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
            return new InternalIPv4Range(name, ranges, keyed, pipelineAggregators, metaData);
        }

        @Override
        public InternalIPv4Range create(List<Bucket> ranges, InternalIPv4Range prototype) {
            return new InternalIPv4Range(prototype.name, ranges, prototype.keyed, prototype.pipelineAggregators(), prototype.metaData);
        }

        @Override
        public Bucket createBucket(String key, double from, double to, long docCount, InternalAggregations aggregations, boolean keyed,
                ValueFormatter formatter) {
            return new Bucket(key, from, to, docCount, aggregations, keyed);
        }

        @Override
        public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
            return new Bucket(prototype.getKey(), ((Number) prototype.getFrom()).doubleValue(), ((Number) prototype.getTo()).doubleValue(),
                    prototype.getDocCount(), aggregations, prototype.getKeyed());
        }
    }

    public InternalIPv4Range() {} // for serialization

    public InternalIPv4Range(String name, List<InternalIPv4Range.Bucket> ranges, boolean keyed, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        super(name, ranges, ValueFormatter.IPv4, keyed, pipelineAggregators, metaData);
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalRange.Factory<Bucket, InternalIPv4Range> getFactory() {
        return FACTORY;
    }
}
