package org.elasticsearch.search.aggregations.transformer;


import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;

import java.io.IOException;
import java.util.*;

/**
 * Wraps a List of buckets and provides an iterable interface that automatically generates data on the fly
 * depending on the configured gap policy.  DiscontinuousHistogram returns BucketMetrics objects, which are a collapsed
 * (and back-filled, where appropriate) view of the bucket metrics.  This makes it more convenient to work with
 * inside the transformer
 */
public class DiscontinuousHistogram implements Iterable<DiscontinuousHistogram.BucketMetrics> {
    private List<InternalHistogram.Bucket> buckets;
    private GapPolicy gapPolicy;

    public static enum GapPolicy {
        insert_zeros((byte) 0), interpolate((byte) 1), ignore((byte) 2);

        private byte id;

        private GapPolicy(byte id) {
            this.id = id;
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(id);
        }

        public static GapPolicy readFrom(StreamInput in) throws IOException {
            byte id = in.readByte();
            for (GapPolicy gapPolicy : values()) {
                if (id == gapPolicy.id) {
                    return gapPolicy;
                }
            }
            throw new IllegalStateException("Unknown GapPolicy with id [" + id + "]");
        }
    }
    
    public DiscontinuousHistogram(List<InternalHistogram.Bucket> list) {
        this(list, GapPolicy.ignore);
    }
    
    public DiscontinuousHistogram(List<InternalHistogram.Bucket> list, GapPolicy gapPolicy) {
        this.buckets = list;
        this.gapPolicy = gapPolicy;
    }


    @Override
    public Iterator<BucketMetrics> iterator() {
        switch (gapPolicy) {
            case ignore:
                return new IgnoreIterator(buckets.listIterator());
            case insert_zeros:
                return new InsertZeroIterator(buckets.listIterator());
            case interpolate:
                return new InterpolateIterator(buckets.listIterator());
            default:
                return new IgnoreIterator(buckets.listIterator());
        }
    }


    /**
     * Base abstract class for the discontinuous iterators.  Provides the majority of the functionality, so that
     * the specific gap-policy iterators can just implement onSingleValue() and onMultiValue()
     */
    public abstract class BucketMetricsIterator implements Iterator<BucketMetrics> {

        protected ListIterator<InternalHistogram.Bucket> iterator;
        Map<String, Double> singleValues = new HashMap<>();
        Map<String, Map<String, Double>> multiValues = new HashMap<>();

        public BucketMetricsIterator(ListIterator<InternalHistogram.Bucket> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public BucketMetrics next() {
            InternalHistogram.Bucket bucket = iterator.next();

            singleValues.clear();   // clear rather than re-instantiate to avoid garbage
            multiValues.clear();

            for (Aggregation aggregation : bucket.getAggregations()) {
                if (aggregation instanceof InternalNumericMetricsAggregation.SingleValue) {
                    InternalNumericMetricsAggregation.SingleValue metricAgg = (InternalNumericMetricsAggregation.SingleValue) aggregation;
                    onSingleValueMetric(metricAgg, singleValues, bucket);

                } else if (aggregation instanceof InternalNumericMetricsAggregation.MultiValue) {
                    InternalNumericMetricsAggregation.MultiValue metricAgg = (InternalNumericMetricsAggregation.MultiValue) aggregation;
                    onMultiValueMetric(metricAgg, multiValues, bucket);

                }
            }

            return new BucketMetrics(bucket, singleValues, multiValues);
        }

        @Override
        public void remove() {
            iterator.remove();
        }

        //protected abstract void onDocCount();  // Do we need this?

        /**
         * When a single value metric is encountered, this method is called so that the specific gap-policy can be consulted
         * @param sv                The single-value metric
         * @param singleValueMap    The map of single values.  The over-riding method should add the computed value to this map
         * @param bucket            The bucket that this metric came from, in case the policy needs it
         */
        protected abstract void onSingleValueMetric(InternalNumericMetricsAggregation.SingleValue sv, Map<String, Double> singleValueMap, InternalHistogram.Bucket bucket);

        /**
         * When a multi value metric is encountered, this method is called so that the specific gap-policy can be consulted
         * @param mv                The multi-value metric
         * @param multiValueMap     The map of multi values.  The over-riding method should add the computed value to this map
         * @param bucket            The bucket that this metric came from, in case the policy needs it
         */
        protected abstract void onMultiValueMetric(InternalNumericMetricsAggregation.MultiValue mv, Map<String, Map<String, Double>> multiValueMap, InternalHistogram.Bucket bucket);
    }


    /**
     * A simple iterator that simply ignores gaps
     */
    public class IgnoreIterator extends BucketMetricsIterator {

        public IgnoreIterator(ListIterator<InternalHistogram.Bucket> iterator) {
            super(iterator);
        }

        @Override
        protected void onSingleValueMetric(InternalNumericMetricsAggregation.SingleValue sv, Map<String, Double> singleValueMap, InternalHistogram.Bucket bucket) {
            return;
        }

        @Override
        protected void onMultiValueMetric(InternalNumericMetricsAggregation.MultiValue mv, Map<String, Map<String, Double>> multiValueMap, InternalHistogram.Bucket bucket) {
            return;
        }
    }


    /**
     * This iterator fills gaps with zeros.  This is different from simply skipping the buckets entirely, since the zeros
     * will still be used for the transformer calculations
     */
    public class InsertZeroIterator extends BucketMetricsIterator {

        public InsertZeroIterator(ListIterator<InternalHistogram.Bucket> iterator) {
            super(iterator);
        }

        @Override
        protected void onSingleValueMetric(InternalNumericMetricsAggregation.SingleValue sv, Map<String, Double> singleValueMap, InternalHistogram.Bucket bucket) {
            singleValueMap.put(sv.getName(), bucket.getDocCount() == 0 ? 0 : sv.value());
        }

        @Override
        protected void onMultiValueMetric(InternalNumericMetricsAggregation.MultiValue mv, Map<String, Map<String, Double>> multiValueMap, InternalHistogram.Bucket bucket) {
            Map<String, Double> metricValues = new HashMap<>();
            for (String valueName : mv.valueNames()) {
                metricValues.put(valueName, bucket.getDocCount() == 0 ? 0 : mv.value(valueName));
            }
            multiValueMap.put(mv.getName(), metricValues);
        }
    }


    /**
     * This iterator interpolates across gaps.  When a gap is encountered, it finds the start/end, calculates the per-bucket delta
     * and then interpolates across the gap with that delta.  This iterator will necessarily be slower, since it needs to "scan ahead"
     * when gaps are encountered so that it can determine the per-bucket delta, then backtrack to fill in the values.
     */
    public class InterpolateIterator extends BucketMetricsIterator {

        private int gapCounter = -1;
        private int iteratorPosition = -1;
        private Gap gap;

        public InterpolateIterator(ListIterator<InternalHistogram.Bucket> iterator) {
            super(iterator);
        }

        @Override
        protected void onSingleValueMetric(InternalNumericMetricsAggregation.SingleValue sv, Map<String, Double> singleValueMap, InternalHistogram.Bucket bucket) {

            if (bucket.getDocCount() == 0) {
                // We found a gap!

                if (gapCounter == -1) {
                    // If we do not have an active gap, find it and init
                    gap = Gap.findGap(iterator.previousIndex(), buckets);
                    gapCounter = 1;
                    iteratorPosition = iterator.previousIndex();
                } else if (iteratorPosition < iterator.previousIndex()) {
                    // we have moved forward, but by necessity are still in the gap.  Just increment counter.
                    // Will not be triggered by multiple metrics on same bucket, since iterator position won't change
                    gapCounter += 1;
                    iteratorPosition = iterator.previousIndex();
                }

                // TODO cache these values so we don't have to keep recalculating?
                double perBucketDelta = (gap.end.get(sv.getName()) - gap.start.get(sv.getName())) / ((double)gap.gapSize + 1.0);
                double interpolatedValue = gap.start.get(sv.getName()) + (perBucketDelta * (gapCounter));
                singleValueMap.put(sv.getName(), interpolatedValue);

            } else {
                // No gap, reset gap accounting and use the metric value
                gapCounter = -1;
                iteratorPosition = -1;
                singleValueMap.put(sv.getName(), sv.value());
            }

        }

        @Override
        protected void onMultiValueMetric(InternalNumericMetricsAggregation.MultiValue mv, Map<String, Map<String, Double>> multiValueMap, InternalHistogram.Bucket bucket) {
            //NOCOMMIT TODO implement this!

            return;
        }
    }


    /**
     * A simple container class which holds details about the start/end of a gap
     */
    public class BucketMetrics {
        public Map<String, Double> singleValues;
        public Map<String, Map<String, Double>> multiValues;
        public InternalHistogram.Bucket owningBucket;

        public BucketMetrics(InternalHistogram.Bucket owningBucket, Map<String, Double> singleValues, Map<String, Map<String, Double>> multiValues) {
            this.singleValues = singleValues;
            this.multiValues = multiValues;
            this.owningBucket = owningBucket;
        }
    }
}
