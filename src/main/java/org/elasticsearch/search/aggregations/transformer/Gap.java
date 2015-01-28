package org.elasticsearch.search.aggregations.transformer;

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;

import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;


public class Gap {

    public Map<String, Double> start;
    public Map<String, Double> end;
    public int startPosition;
    public int endPosition;
    public int gapSize;

    public Gap(int startPosition, int endPosition, int gapSize, Map<String, Double> start, Map<String, Double> end) {
        this.start = start;
        this.end = end;
        this.startPosition = startPosition;
        this.endPosition = endPosition;
        this.gapSize = gapSize;
    }

    /**
     * Finds the end of a gap in a series of buckets, calculates the start/end metrics and returns them
     * as a Gap object.
     *
     * @param start         iterator over the histogram buckets
     * @param histoBuckets histogram buckets
     */
    public static Gap findGap(int start, List<InternalHistogram.Bucket> histoBuckets) {

        int gapSize = 0;
        start -= 1;
        InternalHistogram.Bucket histoBucket;
        ListIterator<InternalHistogram.Bucket> iter = histoBuckets.listIterator(start + 1); // we want to start at the first empty, not the actual start

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

        return new Gap(start, gapSize, start + gapSize, startMetrics, endMetrics);
    }
}


