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

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.google.common.collect.Lists;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.support.numeric.ValueFormatter;
import org.elasticsearch.search.aggregations.support.numeric.ValueFormatterStreams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * An internal implementation of {@link HistogramBase}
 */
abstract class AbstractHistogramBase<B extends HistogramBase.Bucket> extends InternalAggregation implements HistogramBase<B>, ToXContent, Streamable {

    public static class Bucket implements HistogramBase.Bucket {

        private long key;
        private long docCount;
        private InternalAggregations aggregations;

        public Bucket(long key, long docCount, InternalAggregations aggregations) {
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        @Override
        public long getKey() {
            return key;
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        Bucket reduce(List<Bucket> buckets, CacheRecycler cacheRecycler) {
            if (buckets.size() == 1) {
                // we only need to reduce the sub aggregations
                Bucket bucket = buckets.get(0);
                bucket.aggregations.reduce(cacheRecycler);
                return bucket;
            }
            List<InternalAggregations> aggregations = new ArrayList<InternalAggregations>(buckets.size());
            Bucket reduced = null;
            for (Bucket bucket : buckets) {
                if (reduced == null) {
                    reduced = bucket;
                } else {
                    reduced.docCount += bucket.docCount;
                }
                aggregations.add((InternalAggregations) bucket.getAggregations());
            }
            reduced.aggregations = InternalAggregations.reduce(aggregations, cacheRecycler);
            return reduced;
        }
    }

    static class EmptyBucketInfo {
        final Rounding rounding;
        final InternalAggregations subAggregations;

        EmptyBucketInfo(Rounding rounding, InternalAggregations subAggregations) {
            this.rounding = rounding;
            this.subAggregations = subAggregations;
        }

        public static EmptyBucketInfo readFrom(StreamInput in) throws IOException {
            return new EmptyBucketInfo(Rounding.Streams.read(in), InternalAggregations.readAggregations(in));
        }

        public static void writeTo(EmptyBucketInfo info, StreamOutput out) throws IOException {
            Rounding.Streams.write(info.rounding, out);
            info.subAggregations.writeTo(out);
        }
    }

    public static interface Factory<B extends HistogramBase.Bucket> {

        String type();

        AbstractHistogramBase create(String name, List<B> buckets, InternalOrder order, long minDocCount, EmptyBucketInfo emptyBucketInfo, ValueFormatter formatter, boolean keyed);

        Bucket createBucket(long key, long docCount, InternalAggregations aggregations);

    }

    private List<B> buckets;
    private LongObjectOpenHashMap<HistogramBase.Bucket> bucketsMap;
    private InternalOrder order;
    private ValueFormatter formatter;
    private boolean keyed;
    private long minDocCount;
    private EmptyBucketInfo emptyBucketInfo;

    protected AbstractHistogramBase() {} // for serialization

    protected AbstractHistogramBase(String name, List<B> buckets, InternalOrder order, long minDocCount, EmptyBucketInfo emptyBucketInfo, ValueFormatter formatter, boolean keyed) {
        super(name);
        this.buckets = buckets;
        this.order = order;
        assert (minDocCount == 0) == (emptyBucketInfo != null);
        this.minDocCount = minDocCount;
        this.emptyBucketInfo = emptyBucketInfo;
        this.formatter = formatter;
        this.keyed = keyed;
    }

    @Override
    public Iterator<B> iterator() {
        return buckets.iterator();
    }

    @Override
    public List<B> buckets() {
        return buckets;
    }

    @Override
    public B getByKey(long key) {
        if (bucketsMap == null) {
            bucketsMap = new LongObjectOpenHashMap<HistogramBase.Bucket>(buckets.size());
            for (HistogramBase.Bucket bucket : buckets) {
                bucketsMap.put(bucket.getKey(), bucket);
            }
        }
        return (B) bucketsMap.get(key);
    }

    // TODO extract the reduce logic to a strategy class and have it configurable at request time (two possible strategies - total & delta)

    @Override
    public InternalAggregation reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        if (aggregations.size() == 1) {

            AbstractHistogramBase<B> histo = (AbstractHistogramBase<B>) aggregations.get(0);

            if (minDocCount == 1) {
                for (B bucket : histo.buckets) {
                    ((Bucket) bucket).aggregations.reduce(reduceContext.cacheRecycler());
                }
                return histo;
            }


            CollectionUtil.introSort(histo.buckets, order.asc ? InternalOrder.KEY_ASC.comparator() : InternalOrder.KEY_DESC.comparator());
            List<B> list = order.asc ? histo.buckets : Lists.reverse(histo.buckets);
            HistogramBase.Bucket prevBucket = null;
            ListIterator<B> iter = list.listIterator();
            if (minDocCount == 0) {
                // we need to fill the gaps with empty buckets
                while (iter.hasNext()) {
                    // look ahead on the next bucket without advancing the iter
                    // so we'll be able to insert elements at the right position
                    HistogramBase.Bucket nextBucket = list.get(iter.nextIndex());
                    ((Bucket) nextBucket).aggregations.reduce(reduceContext.cacheRecycler());
                    if (prevBucket != null) {
                        long key = emptyBucketInfo.rounding.nextRoundingValue(prevBucket.getKey());
                        while (key != nextBucket.getKey()) {
                            iter.add(createBucket(key, 0, emptyBucketInfo.subAggregations));
                            key = emptyBucketInfo.rounding.nextRoundingValue(key);
                        }
                    }
                    prevBucket = iter.next();
                }
            } else {
                while (iter.hasNext()) {
                    Bucket bucket = (Bucket) iter.next();
                    if (bucket.getDocCount() < minDocCount) {
                        iter.remove();
                    } else {
                        bucket.aggregations.reduce(reduceContext.cacheRecycler());
                    }
                }
            }

            if (order != InternalOrder.KEY_ASC && order != InternalOrder.KEY_DESC) {
                CollectionUtil.introSort(histo.buckets, order.comparator());
            }

            return histo;

        }

        AbstractHistogramBase reduced = (AbstractHistogramBase) aggregations.get(0);

        Recycler.V<LongObjectOpenHashMap<List<Bucket>>> bucketsByKey = reduceContext.cacheRecycler().longObjectMap(-1);
        for (InternalAggregation aggregation : aggregations) {
            AbstractHistogramBase<B> histogram = (AbstractHistogramBase) aggregation;
            for (B bucket : histogram.buckets) {
                List<Bucket> bucketList = bucketsByKey.v().get(((Bucket) bucket).key);
                if (bucketList == null) {
                    bucketList = new ArrayList<Bucket>(aggregations.size());
                    bucketsByKey.v().put(((Bucket) bucket).key, bucketList);
                }
                bucketList.add((Bucket) bucket);
            }
        }

        List<HistogramBase.Bucket> reducedBuckets = new ArrayList<HistogramBase.Bucket>(bucketsByKey.v().size());
        Object[] buckets = bucketsByKey.v().values;
        boolean[] allocated = bucketsByKey.v().allocated;
        for (int i = 0; i < allocated.length; i++) {
            if (allocated[i]) {
                Bucket bucket = ((List<Bucket>) buckets[i]).get(0).reduce(((List<Bucket>) buckets[i]), reduceContext.cacheRecycler());
                if (bucket.getDocCount() >= minDocCount) {
                    reducedBuckets.add(bucket);
                }
            }
        }
        bucketsByKey.release();



        // adding empty buckets in needed
        if (minDocCount == 0) {
            CollectionUtil.introSort(reducedBuckets, order.asc ? InternalOrder.KEY_ASC.comparator() : InternalOrder.KEY_DESC.comparator());
            List<HistogramBase.Bucket> list = order.asc ? reducedBuckets : Lists.reverse(reducedBuckets);
            HistogramBase.Bucket prevBucket = null;
            ListIterator<HistogramBase.Bucket> iter = list.listIterator();
            while (iter.hasNext()) {
                HistogramBase.Bucket nextBucket = list.get(iter.nextIndex());
                if (prevBucket != null) {
                    long key = emptyBucketInfo.rounding.nextRoundingValue(prevBucket.getKey());
                    while (key != nextBucket.getKey()) {
                        iter.add(createBucket(key, 0, emptyBucketInfo.subAggregations));
                        key = emptyBucketInfo.rounding.nextRoundingValue(key);
                    }
                }
                prevBucket = iter.next();
            }

            if (order != InternalOrder.KEY_ASC && order != InternalOrder.KEY_DESC) {
                CollectionUtil.introSort(reducedBuckets, order.comparator());
            }

        } else {
            CollectionUtil.introSort(reducedBuckets, order.comparator());
        }


        reduced.buckets = reducedBuckets;
        return reduced;
    }

    protected B createBucket(long key, long docCount, InternalAggregations aggregations) {
        return (B) new Bucket(key, docCount, aggregations);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        order = InternalOrder.Streams.readOrder(in);
        minDocCount = in.readVLong();
        if (minDocCount == 0) {
            emptyBucketInfo = EmptyBucketInfo.readFrom(in);
        }
        formatter = ValueFormatterStreams.readOptional(in);
        keyed = in.readBoolean();
        int size = in.readVInt();
        List<B> buckets = new ArrayList<B>(size);
        for (int i = 0; i < size; i++) {
            buckets.add(createBucket(in.readLong(), in.readVLong(), InternalAggregations.readAggregations(in)));
        }
        this.buckets = buckets;
        this.bucketsMap = null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        InternalOrder.Streams.writeOrder(order, out);
        out.writeVLong(minDocCount);
        if (minDocCount == 0) {
            EmptyBucketInfo.writeTo(emptyBucketInfo, out);
        }
        ValueFormatterStreams.writeOptional(formatter, out);
        out.writeBoolean(keyed);
        out.writeVInt(buckets.size());
        for (HistogramBase.Bucket bucket : buckets) {
            out.writeLong(((Bucket) bucket).key);
            out.writeVLong(((Bucket) bucket).docCount);
            ((Bucket) bucket).aggregations.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(name);
        } else {
            builder.startArray(name);
        }

        for (HistogramBase.Bucket bucket : buckets) {
            if (formatter != null) {
                Text keyTxt = new StringText(formatter.format(bucket.getKey()));
                if (keyed) {
                    builder.startObject(keyTxt.string());
                } else {
                    builder.startObject();
                }
                builder.field(CommonFields.KEY_AS_STRING, keyTxt);
            } else {
                if (keyed) {
                    builder.startObject(String.valueOf(bucket.getKey()));
                } else {
                    builder.startObject();
                }
            }
            builder.field(CommonFields.KEY, ((Bucket) bucket).key);
            builder.field(CommonFields.DOC_COUNT, ((Bucket) bucket).docCount);
            ((Bucket) bucket).aggregations.toXContentInternal(builder, params);
            builder.endObject();
        }

        if (keyed) {
            builder.endObject();
        } else {
            builder.endArray();
        }
        return builder;
    }
}
