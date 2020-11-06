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
package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.support.ValueType;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

public class InternalDateRange extends InternalRange<InternalDateRange.Bucket, InternalDateRange> {
    public static final Factory FACTORY = new Factory();

    public static class Bucket extends InternalRange.Bucket {

        public Bucket(String key, double from, double to, long docCount, List<InternalAggregation> aggregations, boolean keyed,
                DocValueFormat formatter) {
            super(key, from, to, docCount, InternalAggregations.from(aggregations), keyed, formatter);
        }

        public Bucket(String key, double from, double to, long docCount, InternalAggregations aggregations, boolean keyed,
                DocValueFormat formatter) {
            super(key, from, to, docCount, aggregations, keyed, formatter);
        }

        @Override
        public Object getFrom() {
            return Double.isInfinite(((Number) from).doubleValue()) ? null :
                Instant.ofEpochMilli(((Number) from).longValue()).atZone(ZoneOffset.UTC);
        }

        @Override
        public Object getTo() {
            return Double.isInfinite(((Number) to).doubleValue()) ? null :
                Instant.ofEpochMilli(((Number) to).longValue()).atZone(ZoneOffset.UTC);
        }

        private Double internalGetFrom() {
            return from;
        }

        private Double internalGetTo() {
            return to;
        }

        @Override
        protected InternalRange.Factory<Bucket, ?> getFactory() {
            return FACTORY;
        }

        boolean keyed() {
            return keyed;
        }

        DocValueFormat format() {
            return format;
        }
    }

    public static class Factory extends InternalRange.Factory<InternalDateRange.Bucket, InternalDateRange> {
        @Override
        public ValueType getValueType() {
            return ValueType.DATE;
        }

        @Override
        public InternalDateRange create(String name, List<InternalDateRange.Bucket> ranges, DocValueFormat formatter, boolean keyed,
                Map<String, Object> metadata) {
            return new InternalDateRange(name, ranges, formatter, keyed, metadata);
        }

        @Override
        public InternalDateRange create(List<Bucket> ranges, InternalDateRange prototype) {
            return new InternalDateRange(prototype.name, ranges, prototype.format, prototype.keyed, prototype.metadata);

        }

        @Override
        public Bucket createBucket(String key, double from, double to, long docCount, InternalAggregations aggregations, boolean keyed,
                DocValueFormat formatter) {
            return new Bucket(key, from, to, docCount, aggregations, keyed, formatter);
        }

        @Override
        public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
            return new Bucket(prototype.getKey(), prototype.internalGetFrom(), prototype.internalGetTo(),
                prototype.getDocCount(), aggregations, prototype.getKeyed(), prototype.getFormat());
        }
    }

    InternalDateRange(String name, List<InternalDateRange.Bucket> ranges, DocValueFormat formatter, boolean keyed,
            Map<String, Object> metadata) {
        super(name, ranges, formatter, keyed, metadata);
    }

    /**
     * Read from a stream.
     */
    public InternalDateRange(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return DateRangeAggregationBuilder.NAME;
    }

    @Override
    public InternalRange.Factory<Bucket, InternalDateRange> getFactory() {
        return FACTORY;
    }
}
