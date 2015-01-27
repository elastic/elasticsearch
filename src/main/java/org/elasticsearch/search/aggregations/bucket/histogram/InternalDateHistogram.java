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

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.BucketStreamContext;
import org.elasticsearch.search.aggregations.bucket.BucketStreams;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class InternalDateHistogram extends InternalHistogram<InternalDateHistogram.Bucket> {

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
            context.formatter(bucket.formatter);
            return context;
        }
    };

    public static void registerStream() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
        BucketStreams.registerStream(BUCKET_STREAM, TYPE.stream());
    }

    static class Bucket extends InternalHistogram.Bucket {

        Bucket(boolean keyed, @Nullable ValueFormatter formatter) {
            super(keyed, formatter);
        }

        Bucket(long key, long docCount, InternalAggregations aggregations, boolean keyed, @Nullable ValueFormatter formatter) {
            super(key, docCount, keyed, formatter, aggregations);
        }

        @Override
        protected InternalHistogram.Factory<Bucket> getFactory() {
            return FACTORY;
        }

        @Override
        public String getKeyAsString() {
            return formatter != null ? formatter.format(key) : ValueFormatter.DateTime.DEFAULT.format(key);
        }

        @Override
        public DateTime getKey() {
            return new DateTime(key, DateTimeZone.UTC);
        }

        @Override
        public String toString() {
            return getKeyAsString();
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
                                            long minDocCount, EmptyBucketInfo emptyBucketInfo, @Nullable ValueFormatter formatter, boolean keyed, Map<String, Object> metaData) {
            return new InternalDateHistogram(name, buckets, order, minDocCount, emptyBucketInfo, formatter, keyed, metaData);
        }

        @Override
        public InternalDateHistogram.Bucket createBucket(Object key, long docCount, InternalAggregations aggregations, boolean keyed,
                @Nullable ValueFormatter formatter) {
            if (key instanceof Number) {
                return new Bucket(((Number) key).longValue(), docCount, aggregations, keyed, formatter);
            } else if (key instanceof DateTime) {
                return new Bucket(((DateTime) key).getMillis(), docCount, aggregations, keyed, formatter);
            } else {
                throw new ElasticsearchIllegalArgumentException("Bucket key is not a Number [" + key.toString() + "]");
            }
        }

        @Override
        public Bucket createEmptyBucket(boolean keyed, @Nullable ValueFormatter formatter) {
            return new Bucket(keyed, formatter);
    }
    }

    private ObjectObjectOpenHashMap<String, InternalDateHistogram.Bucket> bucketsMap;

    InternalDateHistogram() {} // for serialization

    InternalDateHistogram(String name, List<InternalDateHistogram.Bucket> buckets, InternalOrder order, long minDocCount,
                          EmptyBucketInfo emptyBucketInfo, @Nullable ValueFormatter formatter, boolean keyed, Map<String, Object> metaData) {
        super(name, buckets, order, minDocCount, emptyBucketInfo, formatter, keyed, metaData);
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalHistogram.Factory<Bucket> getFactory() {
        return FACTORY;
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        super.doReadFrom(in);
        bucketsMap = null; // we need to reset this on read (as it's lazily created on demand)
    }

}
