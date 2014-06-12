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

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

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

        Bucket(long key, long docCount, InternalAggregations aggregations, @Nullable ValueFormatter formatter) {
            super(key, docCount, formatter, aggregations);
        }

        @Override
        protected InternalHistogram.Factory<Bucket> getFactory() {
            return FACTORY;
        }

        @Override
        public String getKey() {
            return formatter != null ? formatter.format(key) : ValueFormatter.DateTime.DEFAULT.format(key);
        }

        @Override
        public DateTime getKeyAsDate() {
            return new DateTime(key, DateTimeZone.UTC);
        }

        @Override
        public String toString() {
            return getKey();
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
                                            long minDocCount, EmptyBucketInfo emptyBucketInfo, @Nullable ValueFormatter formatter, boolean keyed) {
            return new InternalDateHistogram(name, buckets, order, minDocCount, emptyBucketInfo, formatter, keyed);
        }

        @Override
        public InternalDateHistogram.Bucket createBucket(long key, long docCount, InternalAggregations aggregations, @Nullable ValueFormatter formatter) {
            return new Bucket(key, docCount, aggregations, formatter);
        }
    }

    private ObjectObjectOpenHashMap<String, InternalDateHistogram.Bucket> bucketsMap;

    InternalDateHistogram() {} // for serialization

    InternalDateHistogram(String name, List<InternalDateHistogram.Bucket> buckets, InternalOrder order, long minDocCount,
                          EmptyBucketInfo emptyBucketInfo, @Nullable ValueFormatter formatter, boolean keyed) {
        super(name, buckets, order, minDocCount, emptyBucketInfo, formatter, keyed);
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    protected InternalHistogram.Factory<Bucket> getFactory() {
        return FACTORY;
    }

    @Override
    public Bucket getBucketByKey(String key) {
        try {
            long time = Long.parseLong(key);
            return super.getBucketByKey(time);
        } catch (NumberFormatException nfe) {
            // it's not a number, so lets try to parse it as a date using the formatter.
        }
        if (bucketsMap == null) {
            bucketsMap = new ObjectObjectOpenHashMap<>();
            for (InternalDateHistogram.Bucket bucket : buckets) {
                bucketsMap.put(bucket.getKey(), bucket);
            }
        }
        return bucketsMap.get(key);
    }

    @Override
    public DateHistogram.Bucket getBucketByKey(DateTime key) {
        return getBucketByKey(key.getMillis());
    }

    @Override
    protected InternalDateHistogram.Bucket createBucket(long key, long docCount, InternalAggregations aggregations, ValueFormatter formatter) {
        return new Bucket(key, docCount, aggregations, formatter);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        bucketsMap = null; // we need to reset this on read (as it's lazily created on demand)
    }

}
