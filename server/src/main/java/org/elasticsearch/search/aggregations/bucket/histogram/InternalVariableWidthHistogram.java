/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.TransportVersions;
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
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalVariableWidthHistogram extends InternalMultiBucketAggregation<
    InternalVariableWidthHistogram,
    InternalVariableWidthHistogram.Bucket> implements Histogram, HistogramFactory {

    public static class Bucket extends AbstractHistogramBucket implements KeyComparable<Bucket> {

        public static class BucketBounds {
            public double min;
            public double max;

            public BucketBounds(double min, double max) {
                assert min <= max;
                this.min = min;
                this.max = max;
            }

            public BucketBounds(StreamInput in) throws IOException {
                this(in.readDouble(), in.readDouble());
            }

            public void writeTo(StreamOutput out) throws IOException {
                out.writeDouble(min);
                out.writeDouble(max);
            }

            public boolean equals(Object obj) {
                if (this == obj) return true;
                if (obj == null || getClass() != obj.getClass()) return false;
                BucketBounds that = (BucketBounds) obj;
                return min == that.min && max == that.max;
            }

            @Override
            public int hashCode() {
                return Objects.hash(getClass(), min, max);
            }
        }

        private final BucketBounds bounds;
        private final double centroid;

        public Bucket(double centroid, BucketBounds bounds, long docCount, DocValueFormat format, InternalAggregations aggregations) {
            super(docCount, aggregations, format);
            this.centroid = centroid;
            this.bounds = bounds;
        }

        /**
         * Read from a stream.
         */
        public static Bucket readFrom(StreamInput in, DocValueFormat format) throws IOException {
            final double centroid = in.readDouble();
            final long docCount = in.readVLong();
            final BucketBounds bounds = new BucketBounds(in);
            final InternalAggregations aggregations = InternalAggregations.readFrom(in);
            return new Bucket(centroid, bounds, docCount, format, aggregations);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(centroid);
            out.writeVLong(docCount);
            bounds.writeTo(out);
            aggregations.writeTo(out);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != InternalVariableWidthHistogram.Bucket.class) {
                return false;
            }
            InternalVariableWidthHistogram.Bucket that = (InternalVariableWidthHistogram.Bucket) obj;
            return centroid == that.centroid
                && bounds.equals(that.bounds)
                && docCount == that.docCount
                && Objects.equals(aggregations, that.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), centroid, bounds, docCount, aggregations);
        }

        @Override
        public String getKeyAsString() {
            return format.format(centroid).toString();
        }

        /**
         * Buckets are compared using their centroids. But, in the final XContent returned by the aggregation,
         * we want the bucket's key to be its min. Otherwise, it would look like the distances between centroids
         * are buckets, which is incorrect.
         */
        @Override
        public Object getKey() {
            return centroid;
        }

        public double min() {
            return bounds.min;
        }

        public double max() {
            return bounds.max;
        }

        public double centroid() {
            return centroid;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            String keyAsString = format.format((double) getKey()).toString();
            builder.startObject();

            builder.field(CommonFields.MIN.getPreferredName(), min());
            if (format != DocValueFormat.RAW) {
                builder.field(CommonFields.MIN_AS_STRING.getPreferredName(), format.format(min()));
            }

            builder.field(CommonFields.KEY.getPreferredName(), getKey());
            if (format != DocValueFormat.RAW) {
                builder.field(CommonFields.KEY_AS_STRING.getPreferredName(), keyAsString);
            }

            builder.field(CommonFields.MAX.getPreferredName(), max());
            if (format != DocValueFormat.RAW) {
                builder.field(CommonFields.MAX_AS_STRING.getPreferredName(), format.format(max()));
            }

            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int compareKey(InternalVariableWidthHistogram.Bucket other) {
            return Double.compare(centroid, other.centroid); // Use centroid for bucket ordering
        }

        Bucket finalizeSampling(SamplingContext samplingContext) {
            return new Bucket(
                centroid,
                bounds,
                samplingContext.scaleUp(docCount),
                format,
                InternalAggregations.finalizeSampling(aggregations, samplingContext)
            );
        }
    }

    static class EmptyBucketInfo {

        final InternalAggregations subAggregations;

        EmptyBucketInfo(InternalAggregations subAggregations) {
            this.subAggregations = subAggregations;
        }

        EmptyBucketInfo(StreamInput in) throws IOException {
            this(InternalAggregations.readFrom(in));
        }

        public void writeTo(StreamOutput out) throws IOException {
            subAggregations.writeTo(out);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            EmptyBucketInfo that = (EmptyBucketInfo) obj;
            return Objects.equals(subAggregations, that.subAggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), subAggregations);
        }
    }

    private final List<Bucket> buckets;
    private final DocValueFormat format;
    private final int targetNumBuckets;
    final EmptyBucketInfo emptyBucketInfo;

    InternalVariableWidthHistogram(
        String name,
        List<Bucket> buckets,
        EmptyBucketInfo emptyBucketInfo,
        int targetNumBuckets,
        DocValueFormat formatter,
        Map<String, Object> metaData
    ) {
        super(name, metaData);
        this.buckets = buckets;
        this.emptyBucketInfo = emptyBucketInfo;
        this.format = formatter;
        this.targetNumBuckets = targetNumBuckets;
    }

    /**
     * Stream from a stream.
     */
    public InternalVariableWidthHistogram(StreamInput in) throws IOException {
        super(in);
        emptyBucketInfo = new EmptyBucketInfo(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        buckets = in.readCollectionAsList(stream -> Bucket.readFrom(stream, format));
        targetNumBuckets = in.readVInt();
        // we changed the order format in 8.13 for partial reduce, therefore we need to order them to perform merge sort
        if (in.getTransportVersion().between(TransportVersions.V_8_13_0, TransportVersions.V_8_14_0)) {
            // list is mutable by #readCollectionAsList contract
            buckets.sort(Comparator.comparingDouble(b -> b.centroid));
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        emptyBucketInfo.writeTo(out);
        out.writeNamedWriteable(format);
        out.writeCollection(buckets);
        out.writeVInt(targetNumBuckets);
    }

    @Override
    public String getWriteableName() {
        return VariableWidthHistogramAggregationBuilder.NAME;
    }

    @Override
    public List<Bucket> getBuckets() {
        return Collections.unmodifiableList(buckets);
    }

    public int getTargetBuckets() {
        return targetNumBuckets;
    }

    public EmptyBucketInfo getEmptyBucketInfo() {
        return emptyBucketInfo;
    }

    @Override
    public InternalVariableWidthHistogram create(List<Bucket> buckets) {
        return new InternalVariableWidthHistogram(name, buckets, emptyBucketInfo, targetNumBuckets, format, metadata);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.centroid, prototype.bounds, prototype.docCount, prototype.format, aggregations);
    }

    @Override
    public Bucket createBucket(Number key, long docCount, InternalAggregations aggregations) {
        return new Bucket(key.doubleValue(), new Bucket.BucketBounds(key.doubleValue(), key.doubleValue()), docCount, format, aggregations);
    }

    @Override
    public Number getKey(MultiBucketsAggregation.Bucket bucket) {
        return ((Bucket) bucket).centroid;
    }

    private Bucket reduceBucket(List<Bucket> buckets, AggregationReduceContext context) {
        assert buckets.isEmpty() == false;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        double sum = 0;
        try (BucketReducer<Bucket> reducer = new BucketReducer<>(buckets.get(0), context, buckets.size())) {
            for (Bucket bucket : buckets) {
                min = Math.min(min, bucket.bounds.min);
                max = Math.max(max, bucket.bounds.max);
                sum += bucket.docCount * bucket.centroid;
                reducer.accept(bucket);
            }
            final double centroid = sum / reducer.getDocCount();
            final Bucket.BucketBounds bounds = new Bucket.BucketBounds(min, max);
            return new Bucket(centroid, bounds, reducer.getDocCount(), format, reducer.getAggregations());
        }
    }

    public List<Bucket> reduceBuckets(PriorityQueue<IteratorAndCurrent<Bucket>> pq, AggregationReduceContext reduceContext) {
        List<Bucket> reducedBuckets = new ArrayList<>();
        if (pq.size() > 0) {
            double key = pq.top().current().centroid();
            // list of buckets coming from different shards that have the same key
            final List<Bucket> currentBuckets = new ArrayList<>();
            do {
                IteratorAndCurrent<Bucket> top = pq.top();

                if (Double.compare(top.current().centroid(), key) != 0) {
                    // The key changes, reduce what we already buffered and reset the buffer for current buckets.
                    final Bucket reduced = reduceBucket(currentBuckets, reduceContext);
                    reduceContext.consumeBucketsAndMaybeBreak(1);
                    reducedBuckets.add(reduced);
                    currentBuckets.clear();
                    key = top.current().centroid();
                }

                currentBuckets.add(top.current());

                if (top.hasNext()) {
                    Bucket prev = top.current();
                    top.next();
                    assert top.current().compareKey(prev) >= 0 : "shards must return data sorted by centroid";
                    pq.updateTop();
                } else {
                    pq.pop();
                }
            } while (pq.size() > 0);

            if (currentBuckets.isEmpty() == false) {
                final Bucket reduced = reduceBucket(currentBuckets, reduceContext);
                reduceContext.consumeBucketsAndMaybeBreak(1);
                reducedBuckets.add(reduced);
            }
        }

        mergeBucketsIfNeeded(reducedBuckets, targetNumBuckets, reduceContext);
        return reducedBuckets;
    }

    static class BucketRange {
        int startIdx;
        int endIdx;

        /**
         * These are optional utility fields
         * They're useful for determining whether buckets should be merged
         */
        double min;
        double max;
        double centroid;
        long docCount;

        public void mergeWith(BucketRange other) {
            startIdx = Math.min(startIdx, other.startIdx);
            endIdx = Math.max(endIdx, other.endIdx);

            if (docCount + other.docCount > 0) {
                // Avoids div by 0 error. This condition could be false if the optional docCount field was not set
                centroid = ((centroid * docCount) + (other.centroid * other.docCount)) / (docCount + other.docCount);
                docCount += other.docCount;
            }
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
        }
    }

    /**
     * For each range {startIdx, endIdx} in <code>ranges</code>, all the buckets in that index range
     * from <code>buckets</code> are merged, and this merged bucket replaces the entire range.
     */
    private void mergeBucketsWithPlan(List<Bucket> buckets, List<BucketRange> plan, AggregationReduceContext reduceContext) {
        for (int i = plan.size() - 1; i >= 0; i--) {
            BucketRange range = plan.get(i);
            int endIdx = range.endIdx;
            int startIdx = range.startIdx;

            if (startIdx == endIdx) continue;

            List<Bucket> toMerge = new ArrayList<>();
            for (int idx = endIdx; idx > startIdx; idx--) {
                toMerge.add(buckets.get(idx));
                buckets.remove(idx);
            }
            toMerge.add(buckets.get(startIdx)); // Don't remove the startIdx bucket because it will be replaced by the merged bucket

            int toRemove = toMerge.stream().mapToInt(b -> countInnerBucket(b) + 1).sum();
            reduceContext.consumeBucketsAndMaybeBreak(-toRemove + 1);
            Bucket merged_bucket = reduceBucket(toMerge, reduceContext);

            buckets.set(startIdx, merged_bucket);
        }
    }

    /**
     * Makes a merge plan by simulating the merging of the two closest buckets, until the target number of buckets is reached.
     * Distance is determined by centroid comparison.
     * Then, this plan is actually executed and the underlying buckets are merged.
     *
     * Requires: <code>buckets</code> is sorted by centroid.
     */
    private void mergeBucketsIfNeeded(List<Bucket> buckets, int targetNumBuckets, AggregationReduceContext reduceContext) {
        // Make a plan for getting the target number of buckets
        // Each range represents a set of adjacent bucket indices of buckets that will be merged together
        List<BucketRange> ranges = new ArrayList<>();

        // Initialize each range to represent an individual bucket
        for (int i = 0; i < buckets.size(); i++) {
            // Since buckets is sorted by centroid, ranges will be as well
            BucketRange range = new BucketRange();
            range.centroid = buckets.get(i).centroid;
            range.docCount = buckets.get(i).getDocCount();
            range.startIdx = i;
            range.endIdx = i;
            ranges.add(range);
        }

        // Continually merge the two closest ranges until the target is reached
        while (ranges.size() > targetNumBuckets) {

            // Find two closest ranges (i.e. the two closest buckets after the previous merges are completed)
            // We only have to make one pass through the list because it is sorted by centroid
            int closestIdx = 0; // After this loop, (closestIdx, closestIdx + 1) will be the 2 closest buckets
            double smallest_distance = Double.POSITIVE_INFINITY;
            for (int i = 0; i < ranges.size() - 1; i++) {
                double new_distance = ranges.get(i + 1).centroid - ranges.get(i).centroid; // Positive because buckets is sorted
                if (new_distance < smallest_distance) {
                    closestIdx = i;
                    smallest_distance = new_distance;
                }
            }
            // Merge the two closest ranges
            ranges.get(closestIdx).mergeWith(ranges.get(closestIdx + 1));
            ranges.remove(closestIdx + 1);
        }

        // Execute the plan (merge the underlying buckets)
        mergeBucketsWithPlan(buckets, ranges, reduceContext);
    }

    private void mergeBucketsWithSameMin(List<Bucket> buckets, AggregationReduceContext reduceContext) {
        // Create a merge plan
        List<BucketRange> ranges = new ArrayList<>();

        // Initialize each range to represent an individual bucket
        for (int i = 0; i < buckets.size(); i++) {
            BucketRange range = new BucketRange();
            range.min = buckets.get(i).min();
            range.startIdx = i;
            range.endIdx = i;
            ranges.add(range);
        }

        // Merge ranges with same min value
        int i = 0;
        while (i < ranges.size() - 1) {
            BucketRange range = ranges.get(i);
            BucketRange nextRange = ranges.get(i + 1);

            if (range.min == nextRange.min) {
                range.mergeWith(nextRange);
                ranges.remove(i + 1);
            } else {
                i++;
            }
        }

        // Execute the plan (merge the underlying buckets)
        mergeBucketsWithPlan(buckets, ranges, reduceContext);
    }

    /**
     * When two adjacent buckets A, B overlap (A.max &gt; B.min) then their boundary is set to
     * the midpoint: (A.max + B.min) / 2.
     *
     * After this adjustment, A will contain more values than indicated and B will have less.
     */
    private static void adjustBoundsForOverlappingBuckets(List<Bucket> buckets) {
        for (int i = 1; i < buckets.size(); i++) {
            Bucket curBucket = buckets.get(i);
            Bucket prevBucket = buckets.get(i - 1);
            if (curBucket.bounds.min < prevBucket.bounds.max) {
                // We don't want overlapping buckets --> Adjust their bounds
                // TODO: Think of a fairer way to do this. Should prev.max = cur.min?
                curBucket.bounds.min = (prevBucket.bounds.max + curBucket.bounds.min) / 2;
                prevBucket.bounds.max = curBucket.bounds.min;
            }
        }
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        return new AggregatorReducer() {
            private final PriorityQueue<IteratorAndCurrent<Bucket>> pq = new PriorityQueue<>(size) {
                @Override
                protected boolean lessThan(IteratorAndCurrent<Bucket> a, IteratorAndCurrent<Bucket> b) {
                    return Double.compare(a.current().centroid, b.current().centroid) < 0;
                }
            };

            @Override
            public void accept(InternalAggregation aggregation) {
                final InternalVariableWidthHistogram histogram = (InternalVariableWidthHistogram) aggregation;
                if (histogram.buckets.isEmpty() == false) {
                    pq.add(new IteratorAndCurrent<>(histogram.buckets.iterator()));
                }
            }

            @Override
            public InternalAggregation get() {
                final List<Bucket> reducedBuckets = reduceBuckets(pq, reduceContext);
                if (reduceContext.isFinalReduce()) {
                    buckets.sort(Comparator.comparing(Bucket::min));
                    mergeBucketsWithSameMin(reducedBuckets, reduceContext);
                    adjustBoundsForOverlappingBuckets(reducedBuckets);
                }
                return new InternalVariableWidthHistogram(getName(), reducedBuckets, emptyBucketInfo, targetNumBuckets, format, metadata);
            }
        };
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return new InternalVariableWidthHistogram(
            getName(),
            buckets.stream().map(b -> b.finalizeSampling(samplingContext)).toList(),
            emptyBucketInfo,
            targetNumBuckets,
            format,
            getMetadata()
        );
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (Bucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public InternalAggregation createAggregation(List<MultiBucketsAggregation.Bucket> buckets) {
        // convert buckets to the right type
        List<Bucket> buckets2 = new ArrayList<>(buckets.size());
        for (Object b : buckets) {
            buckets2.add((Bucket) b);
        }
        buckets2 = Collections.unmodifiableList(buckets2);
        return new InternalVariableWidthHistogram(name, buckets2, emptyBucketInfo, targetNumBuckets, format, getMetadata());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalVariableWidthHistogram that = (InternalVariableWidthHistogram) obj;
        return Objects.equals(buckets, that.buckets)
            && Objects.equals(format, that.format)
            && Objects.equals(emptyBucketInfo, that.emptyBucketInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), buckets, format, emptyBucketInfo);
    }
}
