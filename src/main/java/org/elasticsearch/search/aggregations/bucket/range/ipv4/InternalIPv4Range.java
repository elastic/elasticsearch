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
import org.elasticsearch.search.aggregations.bucket.range.AbstractRangeBase;
import org.elasticsearch.search.aggregations.support.numeric.ValueFormatter;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class InternalIPv4Range extends AbstractRangeBase<IPv4Range.Bucket> implements IPv4Range {

    public static final long MAX_IP = 4294967296l;

    public final static Type TYPE = new Type("ip_range", "iprange");

    private final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public AbstractRangeBase<?> readResult(StreamInput in) throws IOException {
            InternalIPv4Range range = new InternalIPv4Range();
            range.readFrom(in);
            return range;
        }
    };

    public static void registerStream() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    public static final Factory FACTORY = new Factory();

    public static class Bucket extends AbstractRangeBase.Bucket implements IPv4Range.Bucket {

        public Bucket(String key, double from, double to, long docCount, List<InternalAggregation> aggregations, ValueFormatter formatter) {
            super(key, from, to, docCount, new InternalAggregations(aggregations), formatter);
        }

        public Bucket(String key, double from, double to, long docCount, InternalAggregations aggregations, ValueFormatter formatter) {
            super(key, from, to, docCount, aggregations, formatter);
        }

        @Override
        public String getFromAsString() {
            return Double.isInfinite(getFrom()) ? null : getFrom() == 0 ? null : ValueFormatter.IPv4.format(getFrom());
        }

        @Override
        public String getToAsString() {
            return Double.isInfinite(getTo()) ? null : MAX_IP == getTo() ? null : ValueFormatter.IPv4.format(getTo());
        }
    }

    private static class Factory implements AbstractRangeBase.Factory<IPv4Range.Bucket> {

        @Override
        public String type() {
            return TYPE.name();
        }

        @Override
        public AbstractRangeBase<IPv4Range.Bucket> create(String name, List<IPv4Range.Bucket> buckets, ValueFormatter formatter, boolean keyed) {
            return new InternalIPv4Range(name, buckets, keyed);
        }

        @Override
        public Bucket createBucket(String key, double from, double to, long docCount, InternalAggregations aggregations, ValueFormatter formatter) {
            return new Bucket(key, from, to, docCount, aggregations, formatter);
        }
    }

    public InternalIPv4Range() {
    }

    public InternalIPv4Range(String name, List<IPv4Range.Bucket> ranges, boolean keyed) {
        super(name, ranges, ValueFormatter.IPv4, keyed);
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
