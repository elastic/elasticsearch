/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.Percentile;
import org.elasticsearch.search.aggregations.metrics.Percentiles;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.joda.time.DateTime;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Processes {@link Aggregation} objects and writes flat JSON documents for each leaf aggregation.
 * In order to ensure that datafeeds can restart without duplicating data, we require that
 * each histogram bucket has a nested max aggregation matching the time_field.
 */
class AggregationToJsonProcessor {

    private static final Logger LOGGER = LogManager.getLogger(AggregationToJsonProcessor.class);

    private final String timeField;
    private final Set<String> fields;
    private final boolean includeDocCount;
    private final LinkedHashMap<String, Object> keyValuePairs;
    private long keyValueWrittenCount;
    private final SortedMap<Long, List<Map<String, Object>>> docsByBucketTimestamp;
    private final long startTime;

    /**
     * Constructs a processor that processes aggregations into JSON
     *
     * @param timeField the time field
     * @param fields the fields to convert into JSON
     * @param includeDocCount whether to include the doc_count
     * @param startTime buckets with a timestamp before this time are discarded
     */
    AggregationToJsonProcessor(String timeField, Set<String> fields, boolean includeDocCount, long startTime)
            throws IOException {
        this.timeField = Objects.requireNonNull(timeField);
        this.fields = Objects.requireNonNull(fields);
        this.includeDocCount = includeDocCount;
        keyValuePairs = new LinkedHashMap<>();
        docsByBucketTimestamp = new TreeMap<>();
        keyValueWrittenCount = 0;
        this.startTime = startTime;
    }

    public void process(Aggregations aggs) throws IOException {
        processAggs(0, aggs.asList());
    }

    /**
     * Processes a list of {@link Aggregation}s and writes a flat JSON document for each of its leaf aggregations.
     * Supported sub-aggregations include:
     *   <ul>
     *       <li>{@link MultiBucketsAggregation}</li>
     *       <li>{@link NumericMetricsAggregation.SingleValue}</li>
     *       <li>{@link Percentiles}</li>
     *   </ul>
     */
    private void processAggs(long docCount, List<Aggregation> aggregations) throws IOException {
        if (aggregations.isEmpty()) {
            // This means we reached a bucket aggregation without sub-aggs. Thus, we can flush the path written so far.
            queueDocToWrite(keyValuePairs, docCount);
            return;
        }

        List<Aggregation> leafAggregations = new ArrayList<>();
        List<MultiBucketsAggregation> bucketAggregations = new ArrayList<>();

        // Sort into leaf and bucket aggregations.
        // The leaf aggregations will be processed first.
        for (Aggregation agg : aggregations) {
            if (agg instanceof MultiBucketsAggregation) {
                bucketAggregations.add((MultiBucketsAggregation)agg);
            } else {
                leafAggregations.add(agg);
            }
        }

        if (bucketAggregations.size() > 1) {
            throw new IllegalArgumentException("Multiple bucket aggregations at the same level are not supported");
        }

        List<String> addedLeafKeys = new ArrayList<>();
        for (Aggregation leafAgg : leafAggregations) {
            if (timeField.equals(leafAgg.getName())) {
                processTimeField(leafAgg);
            } else if (fields.contains(leafAgg.getName())) {
                boolean leafAdded = processLeaf(leafAgg);
                if (leafAdded) {
                    addedLeafKeys.add(leafAgg.getName());
                }
            }
        }

        boolean noMoreBucketsToProcess = bucketAggregations.isEmpty();
        if (noMoreBucketsToProcess == false) {
            MultiBucketsAggregation bucketAgg = bucketAggregations.get(0);
            if (bucketAgg instanceof Histogram) {
                processDateHistogram((Histogram) bucketAgg);
            } else {
                // Ignore bucket aggregations that don't contain a field we
                // are interested in. This avoids problems with pipeline bucket
                // aggregations where we would create extra docs traversing a
                // bucket agg that isn't used but is required for the pipeline agg.
                if (bucketAggContainsRequiredAgg(bucketAgg)) {
                    processBucket(bucketAgg, fields.contains(bucketAgg.getName()));
                } else {
                    noMoreBucketsToProcess = true;
                }
            }
        }

        // If there are no more bucket aggregations to process we've reached the end
        // and it's time to write the doc
        if (noMoreBucketsToProcess) {
            queueDocToWrite(keyValuePairs, docCount);
        }

        addedLeafKeys.forEach(k -> keyValuePairs.remove(k));
    }

    private void processDateHistogram(Histogram agg) throws IOException {
        if (keyValuePairs.containsKey(timeField)) {
            throw new IllegalArgumentException("More than one Date histogram cannot be used in the aggregation. " +
                    "[" + agg.getName() + "] is another instance of a Date histogram");
        }

        // buckets are ordered by time, once we get to a bucket past the
        // start time we no longer need to check the time.
        boolean checkBucketTime = true;
        for (Histogram.Bucket bucket : agg.getBuckets()) {
            if (checkBucketTime) {
                long bucketTime = toHistogramKeyToEpoch(bucket.getKey());
                if (bucketTime < startTime) {
                    // skip buckets outside the required time range
                    LOGGER.debug("Skipping bucket at [{}], startTime is [{}]", bucketTime, startTime);
                    continue;
                } else {
                    checkBucketTime = false;
                }
            }

            List<Aggregation> childAggs = bucket.getAggregations().asList();
            processAggs(bucket.getDocCount(), childAggs);
            keyValuePairs.remove(timeField);
        }
    }

    /*
     * Date Histograms have a {@link DateTime} object as the key,
     * Histograms have either a Double or Long.
     */
    private long toHistogramKeyToEpoch(Object key) {
        if (key instanceof DateTime) {
            return ((DateTime)key).getMillis();
        } else if (key instanceof Double) {
            return ((Double)key).longValue();
        } else if (key instanceof Long){
            return (Long)key;
        } else {
            throw new IllegalStateException("Histogram key [" + key + "] cannot be converted to a timestamp");
        }
    }

    private void processTimeField(Aggregation agg) {
        if (agg instanceof Max == false) {
            throw new IllegalArgumentException(Messages.getMessage(Messages.DATAFEED_MISSING_MAX_AGGREGATION_FOR_TIME_FIELD, timeField));
        }

        long timestamp = (long) ((Max) agg).value();
        keyValuePairs.put(timeField, timestamp);
    }

    boolean bucketAggContainsRequiredAgg(MultiBucketsAggregation aggregation) {
        if (fields.contains(aggregation.getName())) {
            return true;
        }

        if (aggregation.getBuckets().isEmpty()) {
            return false;
        }

        boolean foundRequiredAgg = false;
        List<Aggregation> aggs = asList(aggregation.getBuckets().get(0).getAggregations());
        for (Aggregation agg : aggs) {
            if (fields.contains(agg.getName())) {
                foundRequiredAgg = true;
                break;
            }

            if (agg instanceof MultiBucketsAggregation) {
                foundRequiredAgg = bucketAggContainsRequiredAgg((MultiBucketsAggregation) agg);
                if (foundRequiredAgg) {
                    break;
                }
            }
        }

        return foundRequiredAgg;
    }

    private void processBucket(MultiBucketsAggregation bucketAgg, boolean addField) throws IOException {
        for (MultiBucketsAggregation.Bucket bucket : bucketAgg.getBuckets()) {
            if (addField) {
                keyValuePairs.put(bucketAgg.getName(), bucket.getKey());
            }
            processAggs(bucket.getDocCount(), asList(bucket.getAggregations()));
            if (addField) {
                keyValuePairs.remove(bucketAgg.getName());
            }
        }
    }

    /**
     * Adds a leaf key-value. It returns the name of the key added or {@code null} when nothing was added.
     * Non-finite metric values are not added.
     */
    private boolean processLeaf(Aggregation agg) throws IOException {
        if (agg instanceof NumericMetricsAggregation.SingleValue) {
            return processSingleValue((NumericMetricsAggregation.SingleValue) agg);
        } else if (agg instanceof Percentiles) {
            return processPercentiles((Percentiles) agg);
        } else {
            throw new IllegalArgumentException("Unsupported aggregation type [" + agg.getName() + "]");
        }
    }

    private boolean processSingleValue(NumericMetricsAggregation.SingleValue singleValue) throws IOException {
        return addMetricIfFinite(singleValue.getName(), singleValue.value());
    }

    private boolean addMetricIfFinite(String key, double value) {
        if (Double.isFinite(value)) {
            keyValuePairs.put(key, value);
            return true;
        }
        return false;
    }

    private boolean processPercentiles(Percentiles percentiles) throws IOException {
        Iterator<Percentile> percentileIterator = percentiles.iterator();
        boolean aggregationAdded = addMetricIfFinite(percentiles.getName(), percentileIterator.next().getValue());
        if (percentileIterator.hasNext()) {
            throw new IllegalArgumentException("Multi-percentile aggregation [" + percentiles.getName() + "] is not supported");
        }
        return aggregationAdded;
    }

    private void queueDocToWrite(Map<String, Object> doc, long docCount) {
        if (docCount > 0) {
            Map<String, Object> copy = new LinkedHashMap<>(doc);
            if (includeDocCount) {
                copy.put(DatafeedConfig.DOC_COUNT, docCount);
            }

            Long timeStamp = (Long) copy.get(timeField);
            if (timeStamp == null) {
                throw new IllegalArgumentException(
                        Messages.getMessage(Messages.DATAFEED_MISSING_MAX_AGGREGATION_FOR_TIME_FIELD, timeField));
            }

            docsByBucketTimestamp.computeIfAbsent(timeStamp, (t) -> new ArrayList<>()).add(copy);
        }
    }

    /**
     * Write the aggregated documents one bucket at a time until {@code batchSize}
     * key-value pairs have been written. Buckets are written in their entirety and
     * the check on {@code batchSize} run after the bucket has been written so more
     * than {@code batchSize} key-value pairs could be written.
     * The function should be called repeatedly until it returns false, at that point
     * there are no more documents to write.
     *
     * @param batchSize The number of key-value pairs to write.
     * @return True if there are any more documents to write after the call.
     * False if there are no documents to write.
     * @throws IOException If an error occurs serialising the JSON
     */
    boolean writeDocs(int batchSize, OutputStream outputStream) throws IOException {

        if (docsByBucketTimestamp.isEmpty()) {
            return false;
        }

        try (XContentBuilder jsonBuilder = new XContentBuilder(JsonXContent.jsonXContent, outputStream)) {
            long previousWrittenCount = keyValueWrittenCount;
            Iterator<Map.Entry<Long, List<Map<String, Object>>>> iterator = docsByBucketTimestamp.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, List<Map<String, Object>>> entry = iterator.next();
                for (Map<String, Object> map : entry.getValue()) {
                    writeJsonObject(jsonBuilder, map);
                }
                iterator.remove();

                if (keyValueWrittenCount - previousWrittenCount >= batchSize) {
                    break;
                }
            }
        }

        return docsByBucketTimestamp.isEmpty() == false;
    }

    private void writeJsonObject(XContentBuilder jsonBuilder, Map<String, Object> record) throws IOException {
        jsonBuilder.startObject();
        for (Map.Entry<String, Object> keyValue : record.entrySet()) {
            jsonBuilder.field(keyValue.getKey(), keyValue.getValue());
            keyValueWrittenCount++;
        }
        jsonBuilder.endObject();
    }

    /**
     * The key-value pairs that have been written so far
     */
    public long getKeyValueCount() {
        return keyValueWrittenCount;
    }

    private static List<Aggregation> asList(@Nullable Aggregations aggs) {
        return aggs == null ? Collections.emptyList() : aggs.asList();
    }
}
