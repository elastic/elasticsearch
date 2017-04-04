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
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
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

/**
 * Processes {@link Aggregation} objects and writes flat JSON documents for each leaf aggregation.
 * In order to ensure that datafeeds can restart without duplicating data, we require that
 * each histogram bucket has a nested max aggregation matching the time_field.
 */
class AggregationToJsonProcessor implements Releasable {

    private final String timeField;
    private final boolean includeDocCount;
    private final XContentBuilder jsonBuilder;
    private final Map<String, Object> keyValuePairs;
    private long keyValueWrittenCount;

    AggregationToJsonProcessor(String timeField, boolean includeDocCount, OutputStream outputStream)
            throws IOException {
        this.timeField = Objects.requireNonNull(timeField);
        this.includeDocCount = includeDocCount;
        jsonBuilder = new XContentBuilder(JsonXContent.jsonXContent, outputStream);
        keyValuePairs = new LinkedHashMap<>();
        keyValueWrittenCount = 0;
    }

    /**
     * Processes a {@link Histogram.Bucket} and writes a flat JSON document for each of its leaf aggregations.
     * Supported sub-aggregations include:
     *   <ul>
     *       <li>{@link Terms}</li>
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
            writeJsonObject(docCount);
            return;
        }
        if (aggs.get(0) instanceof Terms) {
            if (aggs.size() > 1) {
                throw new IllegalArgumentException("Multiple non-leaf nested aggregations are not supported");
            }
            processTerms((Terms) aggs.get(0));
        } else {
            List<String> addedKeys = new ArrayList<>();
            for (Aggregation nestedAgg : aggs) {
                if (nestedAgg instanceof NumericMetricsAggregation.SingleValue) {
                    addedKeys.add(processSingleValue((NumericMetricsAggregation.SingleValue) nestedAgg));
                } else if (nestedAgg instanceof Percentiles) {
                    addedKeys.add(processPercentiles((Percentiles) nestedAgg));
                } else {
                    throw new IllegalArgumentException("Unsupported aggregation type [" + nestedAgg.getName() + "]");
                }
            }
            writeJsonObject(docCount);
            addedKeys.forEach(k -> keyValuePairs.remove(k));
        }
    }

    private void processTerms(Terms termsAgg) throws IOException {
        for (Terms.Bucket bucket : termsAgg.getBuckets()) {
            keyValuePairs.put(termsAgg.getName(), bucket.getKey());
            processNestedAggs(bucket.getDocCount(), asList(bucket.getAggregations()));
            keyValuePairs.remove(termsAgg.getName());
        }
    }

    private String processSingleValue(NumericMetricsAggregation.SingleValue singleValue) throws IOException {
        keyValuePairs.put(singleValue.getName(), singleValue.value());
        return singleValue.getName();
    }

    private String processPercentiles(Percentiles percentiles) throws IOException {
        Iterator<Percentile> percentileIterator = percentiles.iterator();
        keyValuePairs.put(percentiles.getName(), percentileIterator.next().getValue());
        if (percentileIterator.hasNext()) {
            throw new IllegalArgumentException("Multi-percentile aggregation [" + percentiles.getName() + "] is not supported");
        }
        return percentiles.getName();
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
