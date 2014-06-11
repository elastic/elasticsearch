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
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.support.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class LongTerms extends InternalTerms {

    public static final Type TYPE = new Type("terms", "lterms");

    public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public LongTerms readResult(StreamInput in) throws IOException {
            LongTerms buckets = new LongTerms();
            buckets.readFrom(in);
            return buckets;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }


    static class Bucket extends InternalTerms.Bucket {

        long term;

        public Bucket(long term, long docCount, InternalAggregations aggregations) {
            super(docCount, aggregations);
            this.term = term;
        }

        @Override
        public String getKey() {
            return String.valueOf(term);
        }

        @Override
        public Text getKeyAsText() {
            return new StringText(String.valueOf(term));
        }

        @Override
        public Number getKeyAsNumber() {
            return term;
        }

        @Override
        int compareTerm(Terms.Bucket other) {
            return Long.compare(term, other.getKeyAsNumber().longValue());
        }
    }

    private @Nullable ValueFormatter formatter;

    LongTerms() {} // for serialization

    public LongTerms(String name, InternalOrder order, @Nullable ValueFormatter formatter, int requiredSize, long minDocCount, Collection<InternalTerms.Bucket> buckets) {
        super(name, order, requiredSize, minDocCount, buckets);
        this.formatter = formatter;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalTerms reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        if (aggregations.size() == 1) {
            InternalTerms terms = (InternalTerms) aggregations.get(0);
            terms.trimExcessEntries(reduceContext.bigArrays());
            return terms;
        }
        InternalTerms reduced = null;

        LongObjectPagedHashMap<List<Bucket>> buckets = null;
        for (InternalAggregation aggregation : aggregations) {
            InternalTerms terms = (InternalTerms) aggregation;
            if (terms instanceof UnmappedTerms) {
                continue;
            }
            if (reduced == null) {
                reduced = terms;
            }
            if (buckets == null) {
                buckets = new LongObjectPagedHashMap<>(terms.buckets.size(), reduceContext.bigArrays());
            }
            for (Terms.Bucket bucket : terms.buckets) {
                List<Bucket> existingBuckets = buckets.get(((Bucket) bucket).term);
                if (existingBuckets == null) {
                    existingBuckets = new ArrayList<>(aggregations.size());
                    buckets.put(((Bucket) bucket).term, existingBuckets);
                }
                existingBuckets.add((Bucket) bucket);
            }
        }

        if (reduced == null) {
            // there are only unmapped terms, so we just return the first one (no need to reduce)
            return (UnmappedTerms) aggregations.get(0);
        }

        // TODO: would it be better to sort the backing array buffer of the hppc map directly instead of using a PQ?
        final int size = (int) Math.min(requiredSize, buckets.size());
        BucketPriorityQueue ordered = new BucketPriorityQueue(size, order.comparator(null));
        for (LongObjectPagedHashMap.Cursor<List<LongTerms.Bucket>> cursor : buckets) {
            List<LongTerms.Bucket> sameTermBuckets = cursor.value;
            final InternalTerms.Bucket b = sameTermBuckets.get(0).reduce(sameTermBuckets, reduceContext.bigArrays());
            if (b.getDocCount() >= minDocCount) {
                ordered.insertWithOverflow(b);
            }
        }
        buckets.close();
        InternalTerms.Bucket[] list = new InternalTerms.Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; i--) {
            list[i] = (Bucket) ordered.pop();
        }
        reduced.buckets = Arrays.asList(list);
        return reduced;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.name = in.readString();
        this.order = InternalOrder.Streams.readOrder(in);
        this.formatter = ValueFormatterStreams.readOptional(in);
        this.requiredSize = readSize(in);
        this.minDocCount = in.readVLong();
        int size = in.readVInt();
        List<InternalTerms.Bucket> buckets = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            buckets.add(new Bucket(in.readLong(), in.readVLong(), InternalAggregations.readAggregations(in)));
        }
        this.buckets = buckets;
        this.bucketMap = null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        InternalOrder.Streams.writeOrder(order, out);
        ValueFormatterStreams.writeOptional(formatter, out);
        writeSize(requiredSize, out);
        out.writeVLong(minDocCount);
        out.writeVInt(buckets.size());
        for (InternalTerms.Bucket bucket : buckets) {
            out.writeLong(((Bucket) bucket).term);
            out.writeVLong(bucket.getDocCount());
            ((InternalAggregations) bucket.getAggregations()).writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        this.startAggregationObject(builder);
        builder.startArray(CommonFields.BUCKETS);
        for (InternalTerms.Bucket bucket : buckets) {
            builder.startObject();
            builder.field(CommonFields.KEY, ((Bucket) bucket).term);
            if (formatter != null) {
                builder.field(CommonFields.KEY_AS_STRING, formatter.format(((Bucket) bucket).term));
            }
            builder.field(CommonFields.DOC_COUNT, bucket.getDocCount());
            ((InternalAggregations) bucket.getAggregations()).toXContentInternal(builder, params);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

}
