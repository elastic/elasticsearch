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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public class InternalRange<B extends InternalRange.Bucket> extends InternalAggregation implements Range {

    static final Factory FACTORY = new Factory();

    public final static Type TYPE = new Type("range");

    private final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalRange readResult(StreamInput in) throws IOException {
            InternalRange ranges = new InternalRange();
            ranges.readFrom(in);
            return ranges;
        }
    };

    public static void registerStream() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    public static class Bucket implements Range.Bucket {

        private double from = Double.NEGATIVE_INFINITY;
        private double to = Double.POSITIVE_INFINITY;
        private long docCount;
        InternalAggregations aggregations;
        private String key;

        public Bucket(String key, double from, double to, long docCount, InternalAggregations aggregations, @Nullable ValueFormatter formatter) {
            this.key = key != null ? key : generateKey(from, to, formatter);
            this.from = from;
            this.to = to;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        public String getKey() {
            return key;
        }

        @Override
        public Text getKeyAsText() {
            return new StringText(getKey());
        }

        @Override
        public Number getFrom() {
            return from;
        }

        @Override
        public Number getTo() {
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

        Bucket reduce(List<Bucket> ranges, BigArrays bigArrays) {
            if (ranges.size() == 1) {
                // we stil need to call reduce on all the sub aggregations
                Bucket bucket = ranges.get(0);
                bucket.aggregations.reduce(bigArrays);
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
            reduced.aggregations = InternalAggregations.reduce(aggregationsList, bigArrays);
            return reduced;
        }

        void toXContent(XContentBuilder builder, Params params, @Nullable ValueFormatter formatter, boolean keyed) throws IOException {
            if (keyed) {
                builder.startObject(key);
            } else {
                builder.startObject();
                builder.field(CommonFields.KEY, key);
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

        protected String generateKey(double from, double to, @Nullable ValueFormatter formatter) {
            StringBuilder sb = new StringBuilder();
            sb.append(Double.isInfinite(from) ? "*" : formatter != null ? formatter.format(from) : ValueFormatter.RAW.format(from));
            sb.append("-");
            sb.append(Double.isInfinite(to) ? "*" : formatter != null ? formatter.format(to) : ValueFormatter.RAW.format(to));
            return sb.toString();
        }

    }

    public static class Factory<B extends Bucket, R extends InternalRange<B>> {

        public String type() {
            return TYPE.name();
        }

        public R create(String name, List<B> ranges, @Nullable ValueFormatter formatter, boolean keyed, boolean unmapped, byte[] metaData) {
            return (R) new InternalRange<>(name, ranges, formatter, keyed, unmapped, metaData);
        }


        public B createBucket(String key, double from, double to, long docCount, InternalAggregations aggregations, @Nullable ValueFormatter formatter) {
            return (B) new Bucket(key, from, to, docCount, aggregations, formatter);
        }
    }

    private List<B> ranges;
    private Map<String, B> rangeMap;
    private @Nullable ValueFormatter formatter;
    private boolean keyed;
    private boolean unmapped;

    public InternalRange() {} // for serialization

    public InternalRange(String name, List<B> ranges, @Nullable ValueFormatter formatter, boolean keyed, boolean unmapped, byte[] metaData) {
        super(name, metaData);
        this.ranges = ranges;
        this.formatter = formatter;
        this.keyed = keyed;
        this.unmapped = unmapped;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public Collection<B> getBuckets() {
        return ranges;
    }

    @Override
    public B getBucketByKey(String key) {
        if (rangeMap == null) {
            rangeMap = new HashMap<>(ranges.size());
            for (Range.Bucket bucket : ranges) {
                rangeMap.put(bucket.getKey(), (B) bucket);
            }
        }
        return rangeMap.get(key);
    }

    @Override
    public InternalAggregation reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        if (aggregations.size() == 1) {
            InternalRange<B> reduced = (InternalRange<B>) aggregations.get(0);
            for (B bucket : reduced.ranges) {
                bucket.aggregations.reduce(reduceContext.bigArrays());
            }
            return reduced;
        }
        List<List<Bucket>> rangesList = null;
        for (InternalAggregation aggregation : aggregations) {
            InternalRange<Bucket> ranges = (InternalRange) aggregation;
            if (ranges.unmapped) {
                continue;
            }
            if (rangesList == null) {
                rangesList = new ArrayList<>(ranges.ranges.size());
                for (Bucket bucket : ranges.ranges) {
                    List<Bucket> sameRangeList = new ArrayList<>(aggregations.size());
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
            return aggregations.get(0);
        }

        InternalRange reduced = (InternalRange) aggregations.get(0);
        int i = 0;
        for (List<Bucket> sameRangeList : rangesList) {
            reduced.ranges.set(i++, (sameRangeList.get(0)).reduce(sameRangeList, reduceContext.bigArrays()));
        }
        return reduced;
    }

    protected B createBucket(String key, double from, double to, long docCount, InternalAggregations aggregations, ValueFormatter formatter) {
        return (B) new Bucket(key, from, to, docCount, aggregations, formatter);
    }

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
            bucket.aggregations.writeTo(out);
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(CommonFields.BUCKETS);
        } else {
            builder.startArray(CommonFields.BUCKETS);
        }
        for (B range : ranges) {
            range.toXContent(builder, params, formatter, keyed);
        }
        if (keyed) {
            builder.endObject();
        } else {
            builder.endArray();
        }
    }

}
