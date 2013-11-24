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

package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.support.numeric.ValueFormatter;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class InternalRange extends AbstractRangeBase<Range.Bucket> implements Range {

    static final Factory FACTORY = new Factory();

    public final static Type TYPE = new Type("range");

    private final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public AbstractRangeBase readResult(StreamInput in) throws IOException {
            InternalRange ranges = new InternalRange();
            ranges.readFrom(in);
            return ranges;
        }
    };

    public static void registerStream() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    public static class Bucket extends AbstractRangeBase.Bucket implements Range.Bucket {

        public Bucket(String key, double from, double to, long docCount, InternalAggregations aggregations, ValueFormatter formatter) {
            super(key, from, to, docCount, aggregations, formatter);
        }
    }


    public static class Factory implements AbstractRangeBase.Factory<Range.Bucket> {

        @Override
        public String type() {
            return TYPE.name();
        }

        @Override
        public AbstractRangeBase<Range.Bucket> create(String name, List<Range.Bucket> ranges, ValueFormatter formatter, boolean keyed) {
            return new InternalRange(name, ranges, formatter, keyed);
        }


        public Range.Bucket createBucket(String key, double from, double to, long docCount, InternalAggregations aggregations, ValueFormatter formatter) {
            return new Bucket(key, from, to, docCount, aggregations, formatter);
        }
    }

    protected Range.Bucket createBucket(String key, double from, double to, long docCount, InternalAggregations aggregations, ValueFormatter formatter) {
        return new Bucket(key, from, to, docCount, aggregations, formatter);
    }

    public InternalRange() {} // for serialization

    public InternalRange(String name, List<Range.Bucket> ranges, ValueFormatter formatter, boolean keyed) {
        super(name, ranges, formatter, keyed);
    }

    @Override
    public Type type() {
        return TYPE;
    }

}
