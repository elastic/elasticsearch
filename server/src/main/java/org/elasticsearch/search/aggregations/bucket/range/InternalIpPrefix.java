/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.range;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.KeyComparable;
import org.elasticsearch.search.aggregations.bucket.IteratorAndCurrent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalIpPrefix extends InternalMultiBucketAggregation<InternalIpPrefix, InternalIpPrefix.Bucket> {

    public static class Bucket extends InternalMultiBucketAggregation.InternalBucket implements IpPrefix.Bucket, KeyComparable<InternalIpPrefix.Bucket> {

        private final transient DocValueFormat format;
        private final transient boolean keyed;
        private final String key;
        private final BytesRef value;
        private final long docCount;
        private final InternalAggregations aggregations;

        public Bucket(
            DocValueFormat format,
            boolean keyed,
            String key,
            BytesRef value,
            long docCount,
            InternalAggregations aggregations
        ) {
            this.format = format;
            this.keyed = keyed;
            this.key = key != null ? key : DocValueFormat.IP.format(value);
            this.value = value;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        private static InternalIpPrefix.Bucket createFromStream(StreamInput in, DocValueFormat format, boolean keyed) throws IOException {
            String key = in.readString();
            BytesRef value = in.readBytesRef();
            long docCount = in.readLong();
            InternalAggregations aggregations = InternalAggregations.readFrom(in);

            return new InternalIpPrefix.Bucket(format, keyed, key, value, docCount, aggregations);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (keyed) {
                builder.startObject(key);
            } else {
                builder.startObject();
                builder.field(CommonFields.KEY.getPreferredName(), DocValueFormat.IP.format(this.value));
            }
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            aggregations.toXContentInternal(builder, params);
            return builder.endObject();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(key);
            out.writeBytesRef(value);
            out.writeLong(docCount);
            aggregations.writeTo(out);
        }

        public DocValueFormat getFormat() {
            return format;
        }

        public boolean isKeyed() {
            return keyed;
        }

        public String getKey() {
            return key;
        }

        public BytesRef getValue() {
            return value;
        }

        @Override
        public String getKeyAsString() {
            return key;
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
            return keyed == bucket.keyed && docCount == bucket.docCount &&
                Objects.equals(format, bucket.format) && Objects.equals(value, bucket.value) &&
                Objects.equals(aggregations, bucket.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(format, keyed, value, docCount, aggregations);
        }

        @Override
        public int compareKey(Bucket other) {
            return this.value.compareTo(other.value);
        }
    }

    protected final DocValueFormat format;
    protected final long minDocCount;
    protected final boolean keyed;
    private final List<InternalIpPrefix.Bucket> buckets;

    public InternalIpPrefix(
        String name,
        DocValueFormat format,
        long minDocCount,
        List<Bucket> buckets,
        boolean keyed,
        Map<String, Object> metadata
    ) {
        super(name, metadata);
        this.minDocCount = minDocCount;
        this.format = format;
        this.keyed = keyed;
        this.buckets = buckets;
    }

    public InternalIpPrefix(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        minDocCount = in.readVLong();
        keyed = in.readBoolean();
        buckets = in.readList(stream -> InternalIpPrefix.Bucket.createFromStream(stream, format, keyed));
    }

    @Override
    public String getWriteableName() {
        return IpPrefixAggregationBuilder.NAME;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeVLong(minDocCount);
        out.writeBoolean(keyed);
        out.writeList(buckets);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        final PriorityQueue<IteratorAndCurrent<Bucket>> pq = new PriorityQueue<>(aggregations.size()) {
            @Override
            protected boolean lessThan(IteratorAndCurrent<Bucket> a, IteratorAndCurrent<Bucket> b) {
                return a.current().value.compareTo(b.current().value) < 0;
            }
        };
        for (InternalAggregation aggregation : aggregations) {
            InternalIpPrefix ipPrefix = (InternalIpPrefix) aggregation;
            if (ipPrefix.buckets.isEmpty() == false) {
                pq.add(new IteratorAndCurrent<>(ipPrefix.buckets.iterator()));
            }
        }

        List<Bucket> reducedBuckets = new ArrayList<>();
        if (pq.size() > 0) {
            // list of buckets coming from different shards that have the same value
            List<Bucket> currentBuckets = new ArrayList<>();
            BytesRef value = pq.top().current().value;

            do {
                final IteratorAndCurrent<Bucket> top = pq.top();
                if (!top.current().value.equals(value)) {
                    final Bucket reduced = reduceBucket(currentBuckets, reduceContext);
                    if (reduced.getDocCount() >= minDocCount) {
                        reducedBuckets.add(reduced);
                    }
                    currentBuckets.clear();
                    value = top.current().value;
                }

                currentBuckets.add(top.current());

                if (top.hasNext()) {
                    top.next();
                    assert top.current().value.compareTo(value) > 0 :
                        "shards must return data sorted by value [" + top.current().value + "] and [" + value + "]";
                    pq.updateTop();
                } else {
                    pq.pop();
                }
            } while (pq.size() > 0);

            if (currentBuckets.isEmpty() == false) {
                final Bucket reduced = reduceBucket(currentBuckets, reduceContext);
                if (reduced.getDocCount() >= minDocCount) {
                    reducedBuckets.add(reduced);
                }
            }
        }

        return new InternalIpPrefix(
            getName(),
            format,
            minDocCount,
            reducedBuckets,
            keyed,
            metadata
        );
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(CommonFields.BUCKETS.getPreferredName());
        } else {
            builder.startArray(CommonFields.BUCKETS.getPreferredName());
        }
        for (InternalIpPrefix.Bucket bucket : buckets) {
            bucket.toXContent(builder, params);
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
        return new InternalIpPrefix(name, format, minDocCount, buckets, keyed, metadata);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(format, keyed, prototype.key, prototype.value, prototype.docCount, prototype.aggregations);
    }

    @Override
    protected Bucket reduceBucket(List<Bucket> buckets, AggregationReduceContext context) {
        assert buckets.size() > 0;
        List<InternalAggregations> aggregations = new ArrayList<>(buckets.size());
        for (InternalIpPrefix.Bucket bucket : buckets) {
            aggregations.add(bucket.getAggregations());
        }
        InternalAggregations aggs = InternalAggregations.reduce(aggregations, context);
        return createBucket(aggs, buckets.get(0));
    }

    @Override
    public List<Bucket> getBuckets() {
        return Collections.unmodifiableList(buckets);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        InternalIpPrefix that = (InternalIpPrefix) o;
        return minDocCount == that.minDocCount && keyed == that.keyed && Objects.equals(format, that.format) && Objects.equals(buckets, that.buckets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), format, minDocCount, keyed, buckets);
    }
}
