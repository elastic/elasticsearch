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

package org.elasticsearch.search.aggregations.transformer.moving.avg;

import com.google.common.collect.EvictingQueue;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.transformer.InternalSimpleValue;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public class InternalMovingAvg<B extends InternalHistogram.Bucket> extends InternalHistogram<B> implements MovingAvg {

    final static Type TYPE = new Type("moving_average", "movavg");

    private final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalMovingAvg readResult(StreamInput in) throws IOException {
            InternalMovingAvg histogram = new InternalMovingAvg();
            histogram.readFrom(in);
            return histogram;
        }
    };

    public static void registerStream() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private GapPolicy gapPolicy;
    private InternalAggregations aggregations;
    private int windowSize;
    private Weighting weight;


    InternalMovingAvg() {
        super();
    }

    public InternalMovingAvg(String name, boolean keyed, @Nullable ValueFormatter formatter, GapPolicy gapPolicy,
                             int windowSize, MovingAvg.Weighting weight, InternalAggregations subAggregations, Map<String, Object> metaData) {
        super(name, Collections.EMPTY_LIST, null, 1, null, formatter, keyed, metaData);
        this.gapPolicy = gapPolicy;
        this.aggregations = subAggregations;
        this.windowSize = windowSize;
        this.weight = weight;
    }

    public InternalMovingAvg(String name, List<B> buckets, @Nullable ValueFormatter formatter, boolean keyed, GapPolicy gapPolicy,
                             int windowSize, MovingAvg.Weighting weight, Map<String, Object> metaData) {
        super(name, buckets, null, 1, null, formatter, keyed, metaData);
        this.gapPolicy = gapPolicy;
        this.windowSize = windowSize;
        this.weight = weight;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    @SuppressWarnings("unchecked")
    public InternalAggregation reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        List<InternalAggregations> subAggregationsList = new ArrayList<>(aggregations.size());
        for (InternalAggregation aggregation : aggregations) {
            assert aggregation.getName().equals(getName());
            subAggregationsList.add(((InternalMovingAvg<B>) aggregation).aggregations);
        }
        final InternalAggregations aggs = InternalAggregations.reduce(subAggregationsList, reduceContext);
        InternalHistogram histo = (InternalHistogram) aggs.iterator().next();
        Factory factory = histo.getFactory();
        List<InternalHistogram.Bucket> histoBuckets = histo.getBuckets();
        List<InternalHistogram.Bucket> newBuckets = new ArrayList<>();



        Map<String, EvictingQueue<Double>> bucketWindows = new HashMap<>();
        EvictingQueue<Long> docCountWindow = EvictingQueue.create(this.windowSize);

        for (ListIterator<InternalHistogram.Bucket> iter = histoBuckets.listIterator(); iter.hasNext(); ) {
            InternalHistogram.Bucket histoBucket = iter.next();

            if (histoBucket.getDocCount() == 0) {
                if (gapPolicy.equals(GapPolicy.ignore)) {
                    // If ignoring empty buckets, we can just continue looping
                    continue;

                } else if (gapPolicy.equals(GapPolicy.interpolate)) {

                    Gap gap = findGap(iter, histoBuckets);
                    newBuckets.addAll(interpolateBuckets(gap, histoBuckets, docCountWindow, bucketWindows, factory));
                    continue;
                }

                // Other cases handled by processNonInterpolatedBucket
            }

            newBuckets.add(processNonInterpolatedBucket(histoBucket, docCountWindow, bucketWindows, factory));

        }
        return new InternalMovingAvg<>(getName(), newBuckets, formatter(), keyed(), gapPolicy, windowSize, weight, metaData);
    }

    /**
     * Calculate the moving average, according to the weighting method chosen (simple, linear, ewma)
     *
     * @param values Ringbuffer containing the current window of values
     * @param <T>    Type T extending Number
     * @return       Returns a double containing the moving avg for the window
     */
    private <T extends Number> double calculateAvg(EvictingQueue<T> values ) {
        double avg = 0;

        if (weight.equals(Weighting.simple)) {
            for (T v : values) {
                avg += v.doubleValue();
            }
            avg /= values.size();

        } else if (weight.equals(Weighting.linear)) {

            long totalWeight = 1;
            long current = 1;

            for (T v : values) {
                avg += v.doubleValue() * current;
                totalWeight += current;
                current += 1;
            }
            avg /= totalWeight;

        } else if (weight.equals(Weighting.ewma)) {

            double alpha = 2.0 / (double) (values.size() + 1);  //TODO expose alpha or period as a param to the user
            boolean first = true;

            for (T v : values) {
                if (first) {
                    avg = v.doubleValue();
                    first = false;
                } else {
                    avg = (v.doubleValue() * alpha) + (avg * (1 - alpha));
                }
            }
        }

        return avg ;
    }

    /**
     * Process a histogram bucket that does not need to be interpolated.  Will load doc count and metrics,
     * offer to the appropriate ring buffers, then return a newly created moving_avg bucket
     *
     * @param histoBucket       histogram bucket to calc moving avg for
     * @param docCountWindow    ringbuffer for doc counts
     * @param bucketWindows     ringbuffer for doc metrics
     * @param factory           factory for new histogram buckets
     * @return                  newly created moving avg bucket
     */
    private InternalHistogram.Bucket processNonInterpolatedBucket(InternalHistogram.Bucket histoBucket,
                                                                  EvictingQueue<Long> docCountWindow,
                                                                  Map<String, EvictingQueue<Double>> bucketWindows,
                                                                  Factory factory) {

        docCountWindow.offer(histoBucket.getDocCount());
        long newBucketKey = histoBucket.getKeyAsNumber().longValue();

        for (Aggregation aggregation : histoBucket.getAggregations()) {
            if (aggregation instanceof InternalNumericMetricsAggregation.SingleValue) {
                InternalNumericMetricsAggregation.SingleValue metricAgg = (InternalNumericMetricsAggregation.SingleValue) aggregation;
                EvictingQueue<Double> windowValues = bucketWindows.get(metricAgg.getName());

                if (windowValues == null) {
                    windowValues = EvictingQueue.create(this.windowSize);
                }
                double metricValue = transformMetricValue(metricAgg.value());
                windowValues.offer(metricValue);
                bucketWindows.put(metricAgg.getName(), windowValues);
            }
            // NOCOMMIT implement this for multi-value metrics
        }

        return generateBucket(newBucketKey, docCountWindow, bucketWindows, factory);
    }

    /**
     * Finds the end of a gap in a series of buckets, calculates the start/end metrics and returns them
     * as a Gap object.
     *
     * @param iter         iterator over the histogram buckets
     * @param histoBuckets histogram buckets
     */
    private Gap findGap(ListIterator<InternalHistogram.Bucket> iter, List<InternalHistogram.Bucket> histoBuckets) {


        // Record the previous bucket which had real values
        int start = iter.previousIndex() - 1;
        int gapSize = 0;
        InternalHistogram.Bucket histoBucket;

        // Iterate until we find the end of the gap
        do {
            gapSize += 1;
            histoBucket = iter.next();
        } while (histoBucket.getDocCount() == 0 && iter.hasNext());

        // Once the other end is found, record the start/end metrics
        Map<String, Double> startMetrics = new HashMap<>();
        for (Aggregation aggregation : histoBuckets.get(start).getAggregations()) {
            if (aggregation instanceof InternalNumericMetricsAggregation.SingleValue) {
                InternalNumericMetricsAggregation.SingleValue metricAgg = (InternalNumericMetricsAggregation.SingleValue) aggregation;
                startMetrics.put(metricAgg.getName(), metricAgg.value());
            }
            // NOCOMMIT implement this for multi-value metrics
        }

        Map<String, Double> endMetrics = new HashMap<>();
        for (Aggregation aggregation : histoBucket.getAggregations()) {
            if (aggregation instanceof InternalNumericMetricsAggregation.SingleValue) {
                InternalNumericMetricsAggregation.SingleValue metricAgg = (InternalNumericMetricsAggregation.SingleValue) aggregation;
                endMetrics.put(metricAgg.getName(), metricAgg.value());
            }
            // NOCOMMIT implement this for multi-value metrics
        }

        return new Gap(start, gapSize, startMetrics, endMetrics);


    }

    /**
     * Interpolates gaps in a series of buckets.  Method is given a list of buckets and a Gap to interpolate, then iterates
     * across the gap and fills in the interpolated value based on the start/end metrics of the gap.
     *
     * @param gap               Gap that we are filling
     * @param histoBuckets      list of histogram buckets
     * @param docCountWindow    ringbuffer for doc counts
     * @param bucketWindows     ringbuffer for doc metrics
     * @param factory           factory for new histogram buckets
     * @return                  List of interpolated buckets
     */
    private List<InternalHistogram.Bucket> interpolateBuckets(Gap gap, List<InternalHistogram.Bucket> histoBuckets,
                                    EvictingQueue<Long> docCountWindow,
                                    Map<String, EvictingQueue<Double>> bucketWindows,
                                    Factory factory) {

        List<InternalHistogram.Bucket> newBuckets = new ArrayList<>(gap.gapSize);
        InternalHistogram.Bucket histoBucket;

        long newBucketKey = histoBuckets.get(gap.startPosition).getKeyAsNumber().longValue();

        // Now that we have the start/end metrics around the gap, we can backfill data
        // TODO maybe loop per-metric instead of per-bucket, to prevent loading/unloading the ringbuffer repeatedly?
        for (int i = gap.startPosition + 1; i < gap.startPosition + gap.gapSize + 1; i++) {
            histoBucket = histoBuckets.get(i);
            docCountWindow.offer(histoBucket.getDocCount());

            for (Aggregation aggregation : histoBucket.getAggregations()) {
                if (aggregation instanceof InternalNumericMetricsAggregation.SingleValue) {
                    InternalNumericMetricsAggregation.SingleValue metricAgg = (InternalNumericMetricsAggregation.SingleValue) aggregation;
                    EvictingQueue<Double> windowValues = bucketWindows.get(metricAgg.getName());

                    if (windowValues == null) {
                        windowValues = EvictingQueue.create(this.windowSize);
                    }

                    // Calc the per-bucket delta and interpolate.  TODO: this is recalculated often...cache somewhere instead?
                    double perBucketDelta = (gap.end.get(metricAgg.getName()) - gap.start.get(metricAgg.getName())) / ((double)gap.gapSize + 1.0);
                    double interpolatedValue = gap.start.get(metricAgg.getName()) + ( perBucketDelta * (i - gap.startPosition));
                    windowValues.offer(interpolatedValue);
                    bucketWindows.put(metricAgg.getName(), windowValues);
                }
                // NOCOMMIT implement this for multi-value metrics
            }


            newBuckets.add(generateBucket(newBucketKey, docCountWindow, bucketWindows, factory));
        }

        return newBuckets;

    }

    private InternalHistogram.Bucket generateBucket(long newBucketKey, EvictingQueue<Long> docCountWindow,
                                                    Map<String, EvictingQueue<Double>> bucketWindows,
                                                    Factory factory) {

        List<InternalAggregation> metricsAggregations = new ArrayList<>();
        double docCountMovAvg = this.calculateAvg(docCountWindow);

        InternalSimpleValue docCountMovAvgAgg = new InternalSimpleValue("_doc_count", docCountMovAvg, null); // NOCOMMIT change the name of this to something less confusing
        metricsAggregations.add(docCountMovAvgAgg);

        for (Entry<String, EvictingQueue<Double>> entry : bucketWindows.entrySet()) {
            double movAvg = this.calculateAvg(entry.getValue());
            InternalSimpleValue metricAgg = new InternalSimpleValue(entry.getKey(), movAvg, null);
            metricsAggregations.add(metricAgg);
        }
        InternalAggregations metricsAggs = new InternalAggregations(metricsAggregations);
        return factory.createBucket(newBucketKey, 0, metricsAggs, keyed(), formatter());
    }

    private double transformMetricValue(double value) {

        if (!Double.isNaN(value)) {
            return value;
        }

        if (gapPolicy.equals(GapPolicy.insert_zeros)) {
            return 0;
        }

        return 0; // return 0 in default, just in case
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        super.doWriteTo(out);
        if (aggregations == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            aggregations.writeTo(out);
        }
        gapPolicy.writeTo(out);
        weight.writeTo(out);
        out.writeVInt(windowSize);
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        super.doReadFrom(in);
        if (in.readBoolean()) {
            aggregations = InternalAggregations.readAggregations(in);
        } else {
            aggregations = null;
        }
        gapPolicy = GapPolicy.readFrom(in);
        weight = Weighting.readFrom(in);
        windowSize = in.readVInt();
    }

    private class Gap {
        public Map<String, Double> start;
        public Map<String, Double> end;
        public int startPosition;
        public int gapSize;

        public Gap(int startPosition, int gapSize, Map<String, Double> start, Map<String, Double> end) {
            this.start = start;
            this.end = end;
            this.startPosition = startPosition;
            this.gapSize = gapSize;
        }
    }

}
