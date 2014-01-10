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
import org.elasticsearch.search.aggregations.bucket.range.AbstractRangeBase;
import org.elasticsearch.search.aggregations.support.numeric.ValueFormatter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class InternalDateRange extends AbstractRangeBase<DateRange.Bucket> implements DateRange {

    public final static Type TYPE = new Type("date_range", "drange");

    private final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public AbstractRangeBase<?> readResult(StreamInput in) throws IOException {
            InternalDateRange ranges = new InternalDateRange();
            ranges.readFrom(in);
            return ranges;
        }
    };

    public static void registerStream() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    public static final Factory FACTORY = new Factory();

    public static class Bucket extends AbstractRangeBase.Bucket implements DateRange.Bucket {

        public Bucket(String key, double from, double to, long docCount, List<InternalAggregation> aggregations, ValueFormatter formatter) {
            super(key, from, to, docCount, new InternalAggregations(aggregations), formatter);
        }

        public Bucket(String key, double from, double to, long docCount, InternalAggregations aggregations, ValueFormatter formatter) {
            super(key, from, to, docCount, aggregations, formatter);
        }

        @Override
        public DateTime getFromAsDate() {
            return Double.isInfinite(getFrom()) ? null : new DateTime((long) getFrom(), DateTimeZone.UTC);
        }

        @Override
        public DateTime getToAsDate() {
            return Double.isInfinite(getTo()) ? null : new DateTime((long) getTo(), DateTimeZone.UTC);
        }
    }

    private static class Factory implements AbstractRangeBase.Factory<DateRange.Bucket> {

        @Override
        public String type() {
            return TYPE.name();
        }

        @Override
        public AbstractRangeBase<DateRange.Bucket> create(String name, List<DateRange.Bucket> buckets, ValueFormatter formatter, boolean keyed) {
            return new InternalDateRange(name, buckets, formatter, keyed);
        }

        @Override
        public Bucket createBucket(String key, double from, double to, long docCount, InternalAggregations aggregations, ValueFormatter formatter) {
            return new Bucket(key, from, to, docCount, aggregations, formatter);
        }
    }

    public InternalDateRange() {
    }

    public InternalDateRange(String name, List<DateRange.Bucket> ranges, ValueFormatter formatter, boolean keyed) {
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
