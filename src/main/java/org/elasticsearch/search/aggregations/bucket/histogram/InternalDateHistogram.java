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
package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.support.numeric.ValueFormatter;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class InternalDateHistogram extends InternalHistogram<InternalDateHistogram.Bucket> implements DateHistogram {

    final static Type TYPE = new Type("date_histogram", "dhisto");
    final static Factory FACTORY = new Factory();

    private final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalDateHistogram readResult(StreamInput in) throws IOException {
            InternalDateHistogram histogram = new InternalDateHistogram();
            histogram.readFrom(in);
            return histogram;
        }
    };

    public static void registerStream() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    static class Bucket extends InternalHistogram.Bucket implements DateHistogram.Bucket {

        private final ValueFormatter formatter;

        Bucket(long key, long docCount, InternalAggregations aggregations, ValueFormatter formatter) {
            super(key, docCount, aggregations);
            this.formatter = formatter;
        }

        @Override
        public String getKey() {
            return formatter != null ? formatter.format(key) : DateFieldMapper.Defaults.DATE_TIME_FORMATTER.printer().print(key);
        }

        @Override
        public DateTime getKeyAsDate() {
            return new DateTime(key);
        }
    }

    static class Factory extends InternalHistogram.Factory<InternalDateHistogram.Bucket> {

        private Factory() {
        }

        @Override
        public String type() {
            return TYPE.name();
        }

        @Override
        public InternalDateHistogram create(String name, List<InternalDateHistogram.Bucket> buckets, InternalOrder order,
                                            long minDocCount, EmptyBucketInfo emptyBucketInfo, ValueFormatter formatter, boolean keyed) {
            return new InternalDateHistogram(name, buckets, order, minDocCount, emptyBucketInfo, formatter, keyed);
        }

        @Override
        public InternalDateHistogram.Bucket createBucket(long key, long docCount, InternalAggregations aggregations, ValueFormatter formatter) {
            return new Bucket(key, docCount, aggregations, formatter);
        }
    }

    InternalDateHistogram() {} // for serialization

    InternalDateHistogram(String name, List<InternalDateHistogram.Bucket> buckets, InternalOrder order, long minDocCount,
                          EmptyBucketInfo emptyBucketInfo, ValueFormatter formatter, boolean keyed) {
        super(name, buckets, order, minDocCount, emptyBucketInfo, formatter, keyed);
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public DateHistogram.Bucket getBucketByKey(DateTime key) {
        return getBucketByKey(key.getMillis());
    }

    @Override
    protected InternalDateHistogram.Bucket createBucket(long key, long docCount, InternalAggregations aggregations, ValueFormatter formatter) {
        return new Bucket(key, docCount, aggregations, formatter);
    }
}
