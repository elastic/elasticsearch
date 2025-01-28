/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.prefix;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.KeyComparable;
import org.elasticsearch.search.aggregations.bucket.BucketReducer;
import org.elasticsearch.search.aggregations.bucket.IteratorAndCurrent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalIpPrefix extends InternalMultiBucketAggregation<InternalIpPrefix, InternalIpPrefix.Bucket> {

    public static class Bucket extends InternalMultiBucketAggregation.InternalBucketWritable
        implements
            IpPrefix.Bucket,
            KeyComparable<InternalIpPrefix.Bucket> {

        private final BytesRef key;
        private final boolean isIpv6;
        private final int prefixLength;
        private final boolean appendPrefixLength;
        private final long docCount;
        private final InternalAggregations aggregations;

        public Bucket(
            BytesRef key,
            boolean isIpv6,
            int prefixLength,
            boolean appendPrefixLength,
            long docCount,
            InternalAggregations aggregations
        ) {
            this.key = key;
            this.isIpv6 = isIpv6;
            this.prefixLength = prefixLength;
            this.appendPrefixLength = appendPrefixLength;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        /**
         * Read from a stream.
         */
        public Bucket(StreamInput in) throws IOException {
            this.key = in.readBytesRef();
            this.isIpv6 = in.readBoolean();
            this.prefixLength = in.readVInt();
            this.appendPrefixLength = in.readBoolean();
            this.docCount = in.readLong();
            this.aggregations = InternalAggregations.readFrom(in);
        }

        private void bucketToXContent(XContentBuilder builder, Params params, boolean keyed) throws IOException {
            String key = DocValueFormat.IP.format(this.key);
            if (appendPrefixLength) {
                key = key + "/" + prefixLength;
            }
            if (keyed) {
                builder.startObject(key);
            } else {
                builder.startObject();
                builder.field(CommonFields.KEY.getPreferredName(), key);
            }
            if (isIpv6 == false) {
                builder.field("netmask", DocValueFormat.IP.format(netmask(prefixLength)));
            }
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            builder.field(IpPrefixAggregationBuilder.IS_IPV6_FIELD.getPreferredName(), isIpv6);
            builder.field(IpPrefixAggregationBuilder.PREFIX_LENGTH_FIELD.getPreferredName(), prefixLength);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
        }

        private static BytesRef netmask(int prefixLength) {
            return IpPrefixAggregationBuilder.extractNetmask(prefixLength, false);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBytesRef(key);
            out.writeBoolean(isIpv6);
            out.writeVInt(prefixLength);
            out.writeBoolean(appendPrefixLength);
            out.writeLong(docCount);
            aggregations.writeTo(out);
        }

        public BytesRef getKey() {
            return key;
        }

        @Override
        public String getKeyAsString() {
            return DocValueFormat.IP.format(key);
        }

        public boolean isIpv6() {
            return isIpv6;
        }

        public int getPrefixLength() {
            return prefixLength;
        }

        public boolean appendPrefixLength() {
            return appendPrefixLength;
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public InternalAggregations getAggregations() {
            return aggregations;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Bucket bucket = (Bucket) o;
            return isIpv6 == bucket.isIpv6
                && prefixLength == bucket.prefixLength
                && appendPrefixLength == bucket.appendPrefixLength
                && docCount == bucket.docCount
                && Objects.equals(key, bucket.key)
                && Objects.equals(aggregations, bucket.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, isIpv6, prefixLength, appendPrefixLength, docCount, aggregations);
        }

        @Override
        public int compareKey(Bucket other) {
            return this.key.compareTo(other.key);
        }
    }

    protected final DocValueFormat format;
    protected final boolean keyed;
    protected final long minDocCount;
    private final List<InternalIpPrefix.Bucket> buckets;

    public InternalIpPrefix(
        String name,
        DocValueFormat format,
        boolean keyed,
        long minDocCount,
        List<Bucket> buckets,
        Map<String, Object> metadata
    ) {
        super(name, metadata);
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.format = format;
        this.buckets = buckets;
    }

    /**
     * Stream from a stream.
     */
    public InternalIpPrefix(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        keyed = in.readBoolean();
        minDocCount = in.readVLong();
        buckets = in.readCollectionAsList(Bucket::new);
    }

    @Override
    public String getWriteableName() {
        return IpPrefixAggregationBuilder.NAME;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeBoolean(keyed);
        out.writeVLong(minDocCount);
        out.writeCollection(buckets);
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        return new AggregatorReducer() {
            private final PriorityQueue<IteratorAndCurrent<Bucket>> pq = new PriorityQueue<>(size) {
                @Override
                protected boolean lessThan(IteratorAndCurrent<Bucket> a, IteratorAndCurrent<Bucket> b) {
                    return a.current().key.compareTo(b.current().key) < 0;
                }
            };

            @Override
            public void accept(InternalAggregation aggregation) {
                final InternalIpPrefix ipPrefix = (InternalIpPrefix) aggregation;
                if (ipPrefix.buckets.isEmpty() == false) {
                    pq.add(new IteratorAndCurrent<>(ipPrefix.buckets.iterator()));
                }
            }

            @Override
            public InternalAggregation get() {
                final List<InternalIpPrefix.Bucket> reducedBuckets = reduceBuckets(pq, reduceContext);
                reduceContext.consumeBucketsAndMaybeBreak(reducedBuckets.size());
                return new InternalIpPrefix(getName(), format, keyed, minDocCount, reducedBuckets, metadata);
            }
        };
    }

    private List<Bucket> reduceBuckets(PriorityQueue<IteratorAndCurrent<Bucket>> pq, AggregationReduceContext reduceContext) {
        List<Bucket> reducedBuckets = new ArrayList<>();
        if (pq.size() > 0) {
            // list of buckets coming from different shards that have the same value
            List<Bucket> currentBuckets = new ArrayList<>();
            BytesRef value = pq.top().current().key;

            do {
                final IteratorAndCurrent<Bucket> top = pq.top();
                if (top.current().key.equals(value) == false) {
                    final Bucket reduced = reduceBucket(currentBuckets, reduceContext);
                    if (false == reduceContext.isFinalReduce() || reduced.getDocCount() >= minDocCount) {
                        reducedBuckets.add(reduced);
                    }
                    currentBuckets.clear();
                    value = top.current().key;
                }

                currentBuckets.add(top.current());

                if (top.hasNext()) {
                    top.next();
                    assert top.current().key.compareTo(value) > 0
                        : "shards must return data sorted by value [" + top.current().key + "] and [" + value + "]";
                    pq.updateTop();
                } else {
                    pq.pop();
                }
            } while (pq.size() > 0);

            if (currentBuckets.isEmpty() == false) {
                final Bucket reduced = reduceBucket(currentBuckets, reduceContext);
                if (false == reduceContext.isFinalReduce() || reduced.getDocCount() >= minDocCount) {
                    reducedBuckets.add(reduced);
                }
            }
        }

        return reducedBuckets;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(CommonFields.BUCKETS.getPreferredName());
        } else {
            builder.startArray(CommonFields.BUCKETS.getPreferredName());
        }
        for (InternalIpPrefix.Bucket bucket : buckets) {
            bucket.bucketToXContent(builder, params, keyed);
        }
        if (keyed) {
            builder.endObject();
        } else {
            builder.endArray();
        }
        return builder;
    }

    @Override
    public InternalIpPrefix create(List<Bucket> buckets) {
        return new InternalIpPrefix(name, format, keyed, minDocCount, buckets, metadata);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(
            prototype.key,
            prototype.isIpv6,
            prototype.prefixLength,
            prototype.appendPrefixLength,
            prototype.docCount,
            prototype.aggregations
        );
    }

    private Bucket createBucket(Bucket prototype, InternalAggregations aggregations, long docCount) {
        return new Bucket(prototype.key, prototype.isIpv6, prototype.prefixLength, prototype.appendPrefixLength, docCount, aggregations);
    }

    private Bucket reduceBucket(List<Bucket> buckets, AggregationReduceContext context) {
        assert buckets.isEmpty() == false;
        try (BucketReducer<Bucket> reducer = new BucketReducer<>(buckets.get(0), context, buckets.size())) {
            for (Bucket bucket : buckets) {
                reducer.accept(bucket);
            }
            return createBucket(reducer.getProto(), reducer.getAggregations(), reducer.getDocCount());
        }
    }

    @Override
    public List<Bucket> getBuckets() {
        return Collections.unmodifiableList(buckets);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        InternalIpPrefix that = (InternalIpPrefix) o;
        return minDocCount == that.minDocCount && Objects.equals(format, that.format) && Objects.equals(buckets, that.buckets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), format, minDocCount, buckets);
    }
}
