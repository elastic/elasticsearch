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
package org.elasticsearch.search.aggregations.bucket.range.date;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.BucketStreamContext;
import org.elasticsearch.search.aggregations.bucket.BucketStreams;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class InternalDateRange extends InternalRange<InternalDateRange.Bucket, InternalDateRange> {

    public final static Type TYPE = new Type("date_range", "drange");

    private final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalDateRange readResult(StreamInput in) throws IOException {
            InternalDateRange ranges = new InternalDateRange();
            ranges.readFrom(in);
            return ranges;
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

    public static class Bucket extends InternalRange.Bucket {

        public Bucket(boolean keyed, ValueFormatter formatter) {
            super(keyed, formatter);
        }

        public Bucket(String key, double from, double to, long docCount, List<InternalAggregation> aggregations, boolean keyed, ValueFormatter formatter) {
            super(key, from, to, docCount, new InternalAggregations(aggregations), keyed, formatter);
        }

        public Bucket(String key, double from, double to, long docCount, InternalAggregations aggregations, boolean keyed, ValueFormatter formatter) {
            super(key, from, to, docCount, aggregations, keyed, formatter);
        }

        @Override
        public Object getFrom() {
            return Double.isInfinite(((Number) from).doubleValue()) ? null : new DateTime(((Number) from).longValue(), DateTimeZone.UTC);
        }

        @Override
        public Object getTo() {
            return Double.isInfinite(((Number) to).doubleValue()) ? null : new DateTime(((Number) to).longValue(), DateTimeZone.UTC);
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

    public static class Factory extends InternalRange.Factory<InternalDateRange.Bucket, InternalDateRange> {

        @Override
        public String type() {
            return TYPE.name();
        }

        @Override
        public InternalDateRange create(String name, List<InternalDateRange.Bucket> ranges, ValueFormatter formatter, boolean keyed,
                List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
            return new InternalDateRange(name, ranges, formatter, keyed, pipelineAggregators, metaData);
        }

        @Override
        public InternalDateRange create(List<Bucket> ranges, InternalDateRange prototype) {
            return new InternalDateRange(prototype.name, ranges, prototype.formatter, prototype.keyed, prototype.pipelineAggregators(),
                    prototype.metaData);
        }

        @Override
        public Bucket createBucket(String key, double from, double to, long docCount, InternalAggregations aggregations, boolean keyed, ValueFormatter formatter) {
            return new Bucket(key, from, to, docCount, aggregations, keyed, formatter);
        }

        @Override
        public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
            return new Bucket(prototype.getKey(), ((Number) prototype.getFrom()).doubleValue(), ((Number) prototype.getTo()).doubleValue(),
                    prototype.getDocCount(), aggregations, prototype.getKeyed(), prototype.getFormatter());
        }
    }

    InternalDateRange() {} // for serialization

    InternalDateRange(String name, List<InternalDateRange.Bucket> ranges, ValueFormatter formatter, boolean keyed,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, ranges, formatter, keyed, pipelineAggregators, metaData);
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalRange.Factory<Bucket, InternalDateRange> getFactory() {
        return FACTORY;
    }
}
