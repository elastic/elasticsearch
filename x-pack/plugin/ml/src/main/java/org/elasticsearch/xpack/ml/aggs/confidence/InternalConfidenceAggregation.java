/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.confidence;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.IteratorAndCurrent;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class InternalConfidenceAggregation extends InternalMultiBucketAggregation<
    InternalConfidenceAggregation,
    InternalConfidenceAggregation.Bucket> {

    static class ConfidenceBucket implements ToXContentObject, Writeable {
        final ConfidenceValue upper;
        final ConfidenceValue lower;
        final ConfidenceValue value;
        final String name;
        final boolean keyed;

        ConfidenceBucket(String name, boolean keyed, ConfidenceValue upper, ConfidenceValue lower, ConfidenceValue value) {
            this.name = name;
            this.upper = upper;
            this.lower = lower;
            this.value = value;
            this.keyed = keyed;
        }

        ConfidenceBucket(StreamInput in) throws IOException {
            this.name = in.readString();
            this.upper = in.readNamedWriteable(ConfidenceValue.class);
            this.lower = in.readNamedWriteable(ConfidenceValue.class);
            this.value = in.readNamedWriteable(ConfidenceValue.class);
            this.keyed = in.readBoolean();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (keyed) {
                builder.startObject(name);
            } else {
                builder.startObject();
                builder.field(CommonFields.KEY.getPreferredName(), name);
            }
            builder.field("calculated", value);
            builder.field("upper", upper);
            builder.field("lower", lower);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeNamedWriteable(upper);
            out.writeNamedWriteable(lower);
            out.writeNamedWriteable(value);
            out.writeBoolean(keyed);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ConfidenceBucket that = (ConfidenceBucket) o;
            return that.keyed == keyed
                && Objects.equals(that.upper, upper)
                && Objects.equals(that.lower, lower)
                && Objects.equals(that.value, value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, upper, lower, value, keyed);
        }
    }

    public static class Bucket extends InternalBucket implements MultiBucketsAggregation.Bucket {
        final long key;
        final long docCount;
        InternalAggregations aggregations;

        public Bucket(long key, long docCount, InternalAggregations aggregations) {
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        public Bucket(StreamInput in) throws IOException {
            key = in.readVLong();
            docCount = in.readVLong();
            aggregations = InternalAggregations.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(key);
            out.writeVLong(getDocCount());
            aggregations.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Aggregation.CommonFields.DOC_COUNT.getPreferredName(), docCount);
            builder.field(Aggregation.CommonFields.KEY.getPreferredName(), key);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public Object getKey() {
            return key;
        }

        public long getRawKey() {
            return key;
        }

        @Override
        public String getKeyAsString() {
            return Long.toString(key);
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        @Override
        public String toString() {
            return "Bucket{" + "key=" + getKeyAsString() + ", docCount=" + docCount + ", aggregations=" + aggregations.asMap() + "}";
        }

    }

    private final List<Bucket> buckets;
    private final List<ConfidenceBucket> confidences;
    private final int confidenceInterval;
    private final double probability;
    private final double pLower;
    private final double pUpper;
    private final boolean keyed;
    private final boolean debug;

    protected InternalConfidenceAggregation(
        String name,
        int confidenceInterval,
        double probability,
        boolean keyed,
        boolean debug,
        Map<String, Object> metadata
    ) {
        this(name, confidenceInterval, probability, keyed, debug, metadata, new ArrayList<>(), new ArrayList<>());
    }

    protected InternalConfidenceAggregation(
        String name,
        int confidenceInterval,
        double probability,
        boolean keyed,
        boolean debug,
        Map<String, Object> metadata,
        List<Bucket> buckets,
        List<ConfidenceBucket> confidences
    ) {
        super(name, metadata);
        this.confidenceInterval = confidenceInterval;
        this.buckets = buckets;
        this.probability = probability;
        this.confidences = confidences;
        double calculatedConfidenceInterval = confidenceInterval / 100.0;
        this.pLower = (1.0 - calculatedConfidenceInterval) / 2.0;
        this.pUpper = (1.0 + calculatedConfidenceInterval) / 2.0;
        this.keyed = keyed;
        this.debug = debug;
    }

    public InternalConfidenceAggregation(StreamInput in) throws IOException {
        super(in);
        this.buckets = in.readList(Bucket::new);
        this.probability = in.readDouble();
        this.confidenceInterval = in.readVInt();
        this.confidences = in.readList(ConfidenceBucket::new);
        this.keyed = in.readBoolean();
        this.debug = in.readBoolean();
        double calculatedConfidenceInterval = confidenceInterval / 100.0;
        this.pLower = (1.0 - calculatedConfidenceInterval) / 2.0;
        this.pUpper = (1.0 + calculatedConfidenceInterval) / 2.0;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeList(buckets);
        out.writeDouble(probability);
        out.writeVInt(confidenceInterval);
        out.writeList(confidences);
        out.writeBoolean(keyed);
        out.writeBoolean(debug);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject("confidences");
        } else {
            builder.startArray("confidences");
        }
        for (ConfidenceBucket confidences : confidences) {
            confidences.toXContent(builder, params);
        }
        if (keyed) {
            builder.endObject();
        } else {
            builder.endArray();
        }
        if (debug) {
            builder.startArray(CommonFields.BUCKETS.getPreferredName());
            for (Bucket bucket : buckets) {
                bucket.toXContent(builder, params);
            }
            builder.endArray();
        }
        return builder;
    }

    @Override
    public InternalConfidenceAggregation create(List<Bucket> buckets) {
        return new InternalConfidenceAggregation(name, confidenceInterval, probability, keyed, debug, super.metadata, buckets, confidences);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.key, prototype.docCount, aggregations);
    }

    public Bucket createBucket(long key, long docCount, InternalAggregations aggregations) {
        return new Bucket(key, docCount, aggregations);
    }

    @Override
    protected Bucket reduceBucket(List<Bucket> buckets, AggregationReduceContext context) {
        assert buckets.size() > 0;
        List<InternalAggregations> aggregations = new ArrayList<>(buckets.size());
        long docCount = 0;
        for (Bucket bucket : buckets) {
            docCount += bucket.docCount;
            aggregations.add((InternalAggregations) bucket.getAggregations());
        }
        InternalAggregations aggs = InternalAggregations.reduce(aggregations, context);
        return createBucket(buckets.get(0).key, docCount, aggs);
    }

    @Override
    public List<Bucket> getBuckets() {
        return buckets;
    }

    @Override
    public String getWriteableName() {
        return ConfidenceAggregationBuilder.NAME;
    }

    private List<Bucket> reduceBuckets(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        final PriorityQueue<IteratorAndCurrent<Bucket>> pq = new PriorityQueue<>(aggregations.size()) {
            @Override
            protected boolean lessThan(IteratorAndCurrent<Bucket> a, IteratorAndCurrent<Bucket> b) {
                return a.current().key < b.current().key;
            }
        };
        for (InternalAggregation aggregation : aggregations) {
            InternalConfidenceAggregation confidence = (InternalConfidenceAggregation) aggregation;
            if (confidence.buckets.isEmpty() == false) {
                pq.add(new IteratorAndCurrent<>(confidence.buckets.iterator()));
            }
        }

        List<Bucket> reducedBuckets = new ArrayList<>();
        if (pq.size() > 0) {
            // list of buckets coming from different shards that have the same key
            List<Bucket> currentBuckets = new ArrayList<>();
            long key = pq.top().current().key;
            do {
                final IteratorAndCurrent<Bucket> top = pq.top();
                if (top.current().key != key) {
                    // The key changes, reduce what we already buffered and reset the buffer for current buckets.
                    // Using Double.compare instead of != to handle NaN correctly.
                    final Bucket reduced = reduceBucket(currentBuckets, reduceContext);
                    reducedBuckets.add(reduced);
                    currentBuckets.clear();
                    key = top.current().key;
                }

                currentBuckets.add(top.current());

                if (top.hasNext()) {
                    top.next();
                    assert top.current().key > key : "shards must return data sorted by key";
                    pq.updateTop();
                } else {
                    pq.pop();
                }
            } while (pq.size() > 0);

            if (currentBuckets.isEmpty() == false) {
                final Bucket reduced = reduceBucket(currentBuckets, reduceContext);
                reducedBuckets.add(reduced);
            }
        }

        return reducedBuckets;
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        List<Bucket> mergedBuckets = reduceBuckets(aggregations, reduceContext);
        if (reduceContext.isFinalReduce() == false || mergedBuckets.isEmpty()) {
            return new InternalConfidenceAggregation(
                name,
                this.confidenceInterval,
                probability,
                keyed,
                debug,
                metadata,
                mergedBuckets,
                List.of()
            );
        }

        Map<String, ConfidenceBuilder> builders = new HashMap<>();
        final Bucket calculatedValueBucket = mergedBuckets.get(0);
        final long docCount = calculatedValueBucket.getDocCount();
        calculatedValueBucket.aggregations.asList()
            .forEach(agg -> builders.put(agg.getName(), ConfidenceBuilder.factory((InternalAggregation) agg, agg.getName(), docCount)));

        for (Bucket b : mergedBuckets.subList(1, mergedBuckets.size())) {
            b.aggregations.asList().forEach(agg -> builders.get(agg.getName()).addAgg((InternalAggregation) agg));
        }
        return new InternalConfidenceAggregation(
            name,
            this.confidenceInterval,
            probability,
            this.keyed,
            this.debug,
            metadata,
            mergedBuckets,
            builders.values().stream().map((cb) -> cb.build(probability, pUpper, pLower, keyed)).collect(Collectors.toList())
        );
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return this;
    }
}
