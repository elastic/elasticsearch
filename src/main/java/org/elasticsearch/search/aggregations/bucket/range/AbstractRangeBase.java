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

import com.google.common.collect.Lists;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.support.numeric.ValueFormatter;
import org.elasticsearch.search.aggregations.support.numeric.ValueFormatterStreams;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public abstract class AbstractRangeBase<B extends RangeBase.Bucket> extends InternalAggregation implements RangeBase<B> {

    public abstract static class Bucket implements RangeBase.Bucket {

        private double from = Double.NEGATIVE_INFINITY;
        private double to = Double.POSITIVE_INFINITY;
        private long docCount;
        private InternalAggregations aggregations;
        private String key;
        private boolean explicitKey;

        public Bucket(String key, double from, double to, long docCount, InternalAggregations aggregations, ValueFormatter formatter) {
            if (key != null) {
                this.key = key;
                explicitKey = true;
            } else {
                this.key = key(from, to, formatter);
                explicitKey = false;
            }
            this.from = from;
            this.to = to;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        public String getKey() {
            return key;
        }

        @Override
        public double getFrom() {
            return from;
        }

        @Override
        public double getTo() {
            return to;
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        Bucket reduce(List<Bucket> ranges, CacheRecycler cacheRecycler) {
            if (ranges.size() == 1) {
                // we stil need to call reduce on all the sub aggregations
                Bucket bucket = ranges.get(0);
                bucket.aggregations.reduce(cacheRecycler);
                return bucket;
            }
            Bucket reduced = null;
            List<InternalAggregations> aggregationsList = Lists.newArrayListWithCapacity(ranges.size());
            for (Bucket range : ranges) {
                if (reduced == null) {
                    reduced = range;
                } else {
                    reduced.docCount += range.docCount;
                }
                aggregationsList.add(range.aggregations);
            }
            reduced.aggregations = InternalAggregations.reduce(aggregationsList, cacheRecycler);
            return reduced;
        }

        void toXContent(XContentBuilder builder, Params params, ValueFormatter formatter, boolean keyed) throws IOException {
            if (keyed) {
                builder.startObject(key);
            } else {
                builder.startObject();
                if (explicitKey) {
                    builder.field(CommonFields.KEY, key);
                }
            }
            if (!Double.isInfinite(from)) {
                builder.field(CommonFields.FROM, from);
                if (formatter != null) {
                    builder.field(CommonFields.FROM_AS_STRING, formatter.format(from));
                }
            }
            if (!Double.isInfinite(to)) {
                builder.field(CommonFields.TO, to);
                if (formatter != null) {
                    builder.field(CommonFields.TO_AS_STRING, formatter.format(to));
                }
            }
            builder.field(CommonFields.DOC_COUNT, docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
        }

        private static String key(double from, double to, ValueFormatter formatter) {
            StringBuilder sb = new StringBuilder();
            sb.append(Double.isInfinite(from) ? "*" : formatter != null ? formatter.format(from) : from);
            sb.append("-");
            sb.append(Double.isInfinite(to) ? "*" : formatter != null ? formatter.format(to) : to);
            return sb.toString();
        }

    }

    public static interface Factory<B extends RangeBase.Bucket> {

        public String type();

        public AbstractRangeBase<B> create(String name, List<B> buckets, ValueFormatter formatter, boolean keyed);

        public B createBucket(String key, double from, double to, long docCount, InternalAggregations aggregations, ValueFormatter formatter);

    }

    private List<B> ranges;
    private Map<String, B> rangeMap;
    private ValueFormatter formatter;
    private boolean keyed;

    private boolean unmapped;

    public AbstractRangeBase() {} // for serialization

    public AbstractRangeBase(String name, List<B> ranges, ValueFormatter formatter, boolean keyed) {
        this(name, ranges, formatter, keyed, false);
    }

    public AbstractRangeBase(String name, List<B> ranges, ValueFormatter formatter, boolean keyed, boolean unmapped) {
        super(name);
        this.ranges = ranges;
        this.formatter = formatter;
        this.keyed = keyed;
        this.unmapped = unmapped;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<B> iterator() {
        Object iter = ranges.iterator();
        return (Iterator<B>) iter;
    }

    @Override
    public B getByKey(String key) {
        if (rangeMap == null) {
            rangeMap = new HashMap<String, B>();
            for (RangeBase.Bucket bucket : ranges) {
                rangeMap.put(bucket.getKey(), (B) bucket);
            }
        }
        return (B) rangeMap.get(key);
    }

    @Override
    public List<B> buckets() {
        return ranges;
    }

    @Override
    public AbstractRangeBase reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        if (aggregations.size() == 1) {
            AbstractRangeBase<B> reduced = (AbstractRangeBase<B>) aggregations.get(0);
            for (B bucket : reduced.buckets()) {
                ((Bucket) bucket).aggregations.reduce(reduceContext.cacheRecycler());
            }
            return reduced;
        }
        List<List<Bucket>> rangesList = null;
        for (InternalAggregation aggregation : aggregations) {
            AbstractRangeBase<Bucket> ranges = (AbstractRangeBase) aggregation;
            if (ranges.unmapped) {
                continue;
            }
            if (rangesList == null) {
                rangesList = new ArrayList<List<Bucket>>(ranges.ranges.size());
                for (Bucket bucket : ranges.ranges) {
                    List<Bucket> sameRangeList = new ArrayList<Bucket>(aggregations.size());
                    sameRangeList.add(bucket);
                    rangesList.add(sameRangeList);
                }
            } else {
                int i = 0;
                for (Bucket range : ranges.ranges) {
                    rangesList.get(i++).add(range);
                }
            }
        }

        if (rangesList == null) {
            // unmapped, we can just take the first one
            return (AbstractRangeBase<?>) aggregations.get(0);
        }

        AbstractRangeBase reduced = (AbstractRangeBase) aggregations.get(0);
        int i = 0;
        for (List<Bucket> sameRangeList : rangesList) {
            reduced.ranges.set(i++, (sameRangeList.get(0)).reduce(sameRangeList, reduceContext.cacheRecycler()));
        }
        return reduced;
    }

    protected abstract B createBucket(String key, double from, double to, long docCount, InternalAggregations aggregations, ValueFormatter formatter);

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        formatter = ValueFormatterStreams.readOptional(in);
        keyed = in.readBoolean();
        int size = in.readVInt();
        List<B> ranges = Lists.newArrayListWithCapacity(size);
        for (int i = 0; i < size; i++) {
            String key = in.readOptionalString();
            ranges.add(createBucket(key, in.readDouble(), in.readDouble(), in.readVLong(), InternalAggregations.readAggregations(in), formatter));
        }
        this.ranges = ranges;
        this.rangeMap = null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        ValueFormatterStreams.writeOptional(formatter, out);
        out.writeBoolean(keyed);
        out.writeVInt(ranges.size());
        for (B bucket : ranges) {
            out.writeOptionalString(((Bucket) bucket).key);
            out.writeDouble(((Bucket) bucket).from);
            out.writeDouble(((Bucket) bucket).to);
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
        for (B range : ranges) {
            ((Bucket) range).toXContent(builder, params, formatter, keyed);
        }
        if (keyed) {
            builder.endObject();
        } else {
            builder.endArray();
        }
        return builder;
    }

}
