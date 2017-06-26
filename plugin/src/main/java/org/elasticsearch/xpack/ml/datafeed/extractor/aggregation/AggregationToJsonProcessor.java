/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;

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

/**
 * Processes {@link Aggregation} objects and writes flat JSON documents for each leaf aggregation.
 * In order to ensure that datafeeds can restart without duplicating data, we require that
 * each histogram bucket has a nested max aggregation matching the time_field.
 */
class AggregationToJsonProcessor implements Releasable {

    private final String timeField;
    private final Set<String> fields;
    private final boolean includeDocCount;
    private final XContentBuilder jsonBuilder;
    private final Map<String, Object> keyValuePairs;
    private long keyValueWrittenCount;

    /**
     * Constructs a processor that processes aggregations into JSON
     *
     * @param timeField the time field
     * @param fields the fields to convert into JSON
     * @param includeDocCount whether to include the doc_count
     * @param outputStream the stream to write the output
     * @throws IOException if an error occurs during the processing
     */
    AggregationToJsonProcessor(String timeField, Set<String> fields, boolean includeDocCount, OutputStream outputStream)
            throws IOException {
        this.timeField = Objects.requireNonNull(timeField);
        this.fields = Objects.requireNonNull(fields);
        this.includeDocCount = includeDocCount;
        jsonBuilder = new XContentBuilder(JsonXContent.jsonXContent, outputStream);
        keyValuePairs = new LinkedHashMap<>();
        keyValueWrittenCount = 0;
    }

    /**
     * Processes a {@link Histogram.Bucket} and writes a flat JSON document for each of its leaf aggregations.
     * Supported sub-aggregations include:
     *   <ul>
     *       <li>{@link MultiBucketsAggregation}</li>
     *       <li>{@link NumericMetricsAggregation.SingleValue}</li>
     *       <li>{@link Percentiles}</li>
     *   </ul>
     */
    public void process(Histogram.Bucket bucket) throws IOException {
        if (bucket.getDocCount() == 0) {
            return;
        }

        Aggregations aggs = bucket.getAggregations();
        Aggregation timeAgg = aggs == null ? null : aggs.get(timeField);
        if (timeAgg instanceof Max == false) {
            throw new IllegalArgumentException("Missing max aggregation for time_field [" + timeField + "]");
        }

        // We want to handle the max time aggregation only at the bucket level.
        // So, we add the value here and then remove the aggregation before
        // processing the rest of the sub aggs.
        long timestamp = (long) ((Max) timeAgg).value();
        keyValuePairs.put(timeField, timestamp);
        List<Aggregation> subAggs = new ArrayList<>(aggs.asList());
        subAggs.remove(timeAgg);
        processNestedAggs(bucket.getDocCount(), subAggs);
    }

    private void processNestedAggs(long docCount, List<Aggregation> aggs) throws IOException {
        if (aggs.isEmpty()) {
            // This means we reached a bucket aggregation without sub-aggs. Thus, we can flush the path written so far.
            writeJsonObject(docCount);
            return;
        }

        boolean processedBucketAgg = false;
        List<String> addedLeafKeys = new ArrayList<>();
        for (Aggregation agg : aggs) {
            if (fields.contains(agg.getName())) {
                if (agg instanceof MultiBucketsAggregation) {
                    if (processedBucketAgg) {
                        throw new IllegalArgumentException("Multiple bucket aggregations at the same level are not supported");
                    }
                    if (addedLeafKeys.isEmpty() == false) {
                        throw new IllegalArgumentException("Mixing bucket and leaf aggregations at the same level is not supported");
                    }
                    processedBucketAgg = true;
                    processBucket((MultiBucketsAggregation) agg);
                } else {
                    if (processedBucketAgg) {
                        throw new IllegalArgumentException("Mixing bucket and leaf aggregations at the same level is not supported");
                    }
                    String addedKey = processLeaf(agg);
                    if (addedKey != null) {
                        addedLeafKeys.add(addedKey);
                    }
                }
            }
        }

        if (addedLeafKeys.isEmpty() == false) {
            writeJsonObject(docCount);
            addedLeafKeys.forEach(k -> keyValuePairs.remove(k));
        }
    }

    private void processBucket(MultiBucketsAggregation bucketAgg) throws IOException {
        for (MultiBucketsAggregation.Bucket bucket : bucketAgg.getBuckets()) {
            keyValuePairs.put(bucketAgg.getName(), bucket.getKey());
            processNestedAggs(bucket.getDocCount(), asList(bucket.getAggregations()));
            keyValuePairs.remove(bucketAgg.getName());
        }
    }

    /**
     * Adds a leaf key-value. It returns the name of the key added or {@code null} when nothing was added.
     * Non-finite metric values are not added.
     */
    @Nullable
    private String processLeaf(Aggregation agg) throws IOException {
        if (agg instanceof NumericMetricsAggregation.SingleValue) {
            return processSingleValue((NumericMetricsAggregation.SingleValue) agg);
        } else if (agg instanceof Percentiles) {
            return processPercentiles((Percentiles) agg);
        } else {
            throw new IllegalArgumentException("Unsupported aggregation type [" + agg.getName() + "]");
        }
    }

    private String processSingleValue(NumericMetricsAggregation.SingleValue singleValue) throws IOException {
        return addMetricIfFinite(singleValue.getName(), singleValue.value());
    }

    @Nullable
    private String addMetricIfFinite(String key, double value) {
        if (Double.isFinite(value)) {
            keyValuePairs.put(key, value);
            return key;
        }
        return null;
    }

    private String processPercentiles(Percentiles percentiles) throws IOException {
        Iterator<Percentile> percentileIterator = percentiles.iterator();
        String addedKey = addMetricIfFinite(percentiles.getName(), percentileIterator.next().getValue());
        if (percentileIterator.hasNext()) {
            throw new IllegalArgumentException("Multi-percentile aggregation [" + percentiles.getName() + "] is not supported");
        }
        return addedKey;
    }

    private void writeJsonObject(long docCount) throws IOException {
        if (docCount > 0) {
            jsonBuilder.startObject();
            for (Map.Entry<String, Object> keyValue : keyValuePairs.entrySet()) {
                jsonBuilder.field(keyValue.getKey(), keyValue.getValue());
                keyValueWrittenCount++;
            }
            if (includeDocCount) {
                jsonBuilder.field(DatafeedConfig.DOC_COUNT, docCount);
                keyValueWrittenCount++;
            }
            jsonBuilder.endObject();
        }
    }

    @Override
    public void close() {
        jsonBuilder.close();
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
