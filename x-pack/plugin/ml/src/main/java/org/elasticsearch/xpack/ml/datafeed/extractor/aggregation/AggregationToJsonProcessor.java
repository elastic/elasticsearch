/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.GeoCentroid;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.Percentile;
import org.elasticsearch.search.aggregations.metrics.Percentiles;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.io.IOException;
import java.io.OutputStream;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

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
    private final String compositeAggDateValueSourceName;

    /**
     * Constructs a processor that processes aggregations into JSON
     *
     * @param timeField the time field
     * @param fields the fields to convert into JSON
     * @param includeDocCount whether to include the doc_count
     * @param startTime buckets with a timestamp before this time are discarded
     * @param compositeAggDateValueSourceName the value source for the date_histogram source in the composite agg, if it exists
     */
    AggregationToJsonProcessor(
        String timeField,
        Set<String> fields,
        boolean includeDocCount,
        long startTime,
        @Nullable String compositeAggDateValueSourceName
    ) {
        this.timeField = Objects.requireNonNull(timeField);
        this.fields = Objects.requireNonNull(fields);
        this.includeDocCount = includeDocCount;
        keyValuePairs = new LinkedHashMap<>();
        docsByBucketTimestamp = new TreeMap<>();
        keyValueWrittenCount = 0;
        this.startTime = startTime;
        this.compositeAggDateValueSourceName = compositeAggDateValueSourceName;
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
        List<SingleBucketAggregation> singleBucketAggregations = new ArrayList<>();

        // Sort into leaf and bucket aggregations.
        // The leaf aggregations will be processed first.
        for (Aggregation agg : aggregations) {
            if (agg instanceof MultiBucketsAggregation) {
                bucketAggregations.add((MultiBucketsAggregation) agg);
            } else if (agg instanceof SingleBucketAggregation singleBucketAggregation) {
                // Skip a level down for single bucket aggs, if they have a sub-agg that is not
                // a bucketed agg we should treat it like a leaf in this bucket
                for (Aggregation subAgg : singleBucketAggregation.getAggregations()) {
                    if (subAgg instanceof MultiBucketsAggregation || subAgg instanceof SingleBucketAggregation) {
                        singleBucketAggregations.add(singleBucketAggregation);
                    } else {
                        leafAggregations.add(subAgg);
                    }
                }
            } else {
                leafAggregations.add(agg);
            }
        }

        // If on the current level (indicated via bucketAggregations) or one of the next levels (singleBucketAggregations)
        // we have more than 1 `MultiBucketsAggregation`, we should error out.
        // We need to make the check in this way as each of the items in `singleBucketAggregations` is treated as a separate branch
        // in the recursive handling of this method.
        int bucketAggLevelCount = Math.max(
            bucketAggregations.size(),
            (int) singleBucketAggregations.stream()
                .flatMap(s -> asList(s.getAggregations()).stream())
                .filter(MultiBucketsAggregation.class::isInstance)
                .count()
        );

        if (bucketAggLevelCount > 1) {
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
            } else if (bucketAgg instanceof CompositeAggregation) {
                // This indicates that our composite agg contains our date histogram bucketing via one of its sources
                processCompositeAgg((CompositeAggregation) bucketAgg);
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
        noMoreBucketsToProcess = singleBucketAggregations.isEmpty() && noMoreBucketsToProcess;
        // we support more than one `SingleBucketAggregation` at each level
        // However, we only want to recurse with multi/single bucket aggs.
        // Non-bucketed sub-aggregations were handle as leaf aggregations at this level
        for (SingleBucketAggregation singleBucketAggregation : singleBucketAggregations) {
            processAggs(
                singleBucketAggregation.getDocCount(),
                asList(singleBucketAggregation.getAggregations()).stream()
                    .filter(
                        aggregation -> (aggregation instanceof MultiBucketsAggregation || aggregation instanceof SingleBucketAggregation)
                    )
                    .collect(Collectors.toList())
            );
        }

        // If there are no more bucket aggregations to process we've reached the end
        // and it's time to write the doc
        if (noMoreBucketsToProcess) {
            queueDocToWrite(keyValuePairs, docCount);
        }

        addedLeafKeys.forEach(keyValuePairs::remove);
    }

    private void processDateHistogram(Histogram agg) throws IOException {
        if (keyValuePairs.containsKey(timeField)) {
            throw new IllegalArgumentException(
                "More than one composite or date_histogram cannot be used in the aggregation. "
                    + "["
                    + agg.getName()
                    + "] is another instance of a composite or date_histogram aggregation"
            );
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

    private void processCompositeAgg(CompositeAggregation agg) throws IOException {
        if (keyValuePairs.containsKey(timeField)) {
            throw new IllegalArgumentException(
                "More than one composite or date_histogram cannot be used in the aggregation. "
                    + "["
                    + agg.getName()
                    + "] is another instance of a composite or date_histogram aggregation"
            );
        }
        // Shouldn't ever happen
        if (compositeAggDateValueSourceName == null) {
            throw new IllegalArgumentException(
                "attempted to process composite agg [" + agg.getName() + "] but does not contain date_histogram value source"
            );
        }

        // Composite aggs have multiple items in the bucket. It is possible that within the current
        // date_histogram interval, there are still unprocessed terms, so we shouldn't skip forward past those buckets
        // Instead, we skip according to the `max(timeField)` agg.
        boolean checkBucketTime = true;
        for (CompositeAggregation.Bucket bucket : agg.getBuckets()) {
            if (checkBucketTime) {

                long bucketTime = toHistogramKeyToEpoch(bucket.getKey().get(compositeAggDateValueSourceName));
                if (bucketTime < startTime) {
                    LOGGER.debug(() -> format("Skipping bucket at [%s], startTime is [%s]", bucketTime, startTime));
                    continue;
                } else {
                    checkBucketTime = false;
                }
            }

            Collection<String> addedFields = processCompositeAggBucketKeys(bucket.getKey());
            List<Aggregation> childAggs = bucket.getAggregations().asList();
            processAggs(bucket.getDocCount(), childAggs);
            keyValuePairs.remove(timeField);
            for (String fieldName : addedFields) {
                keyValuePairs.remove(fieldName);
            }
        }
    }

    /**
     * It is possible that the key values in composite agg bucket contain field values we care about
     * Make sure if they do, they get processed
     * @param bucketKeys the composite agg bucket keys
     * @return The field names we added to the key value pairs
     */
    private Collection<String> processCompositeAggBucketKeys(Map<String, Object> bucketKeys) {
        List<String> addedFieldValues = new ArrayList<>();
        for (Map.Entry<String, Object> bucketKey : bucketKeys.entrySet()) {
            if (bucketKey.getKey().equals(compositeAggDateValueSourceName) == false && fields.contains(bucketKey.getKey())) {
                // TODO any validations or processing???
                keyValuePairs.put(bucketKey.getKey(), bucketKey.getValue());
                addedFieldValues.add(bucketKey.getKey());
            }
        }
        return addedFieldValues;
    }

    /*
     * Date Histograms have a {@link ZonedDateTime} object as the key,
     * Histograms have either a Double or Long.
     */
    private static long toHistogramKeyToEpoch(Object key) {
        if (key instanceof ZonedDateTime) {
            return ((ZonedDateTime) key).toInstant().toEpochMilli();
        } else if (key instanceof Double) {
            return ((Double) key).longValue();
        } else if (key instanceof Long) {
            return (Long) key;
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
        if (aggregation instanceof CompositeAggregation
            && Sets.haveNonEmptyIntersection(((CompositeAggregation) aggregation).afterKey().keySet(), fields)) {
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
            List<String> addedFields = new ArrayList<>();
            if (addField) {
                addedFields.add(bucketAgg.getName());
                keyValuePairs.put(bucketAgg.getName(), bucket.getKey());
            }
            if (bucket instanceof CompositeAggregation.Bucket) {
                addedFields.addAll(processCompositeAggBucketKeys(((CompositeAggregation.Bucket) bucket).getKey()));
            }
            processAggs(bucket.getDocCount(), asList(bucket.getAggregations()));
            for (String fieldName : addedFields) {
                keyValuePairs.remove(fieldName);
            }
        }
    }

    /**
     * Adds a leaf key-value. It returns {@code true} if the key added or {@code false} when nothing was added.
     * Non-finite metric values are not added.
     */
    private boolean processLeaf(Aggregation agg) {
        if (agg instanceof NumericMetricsAggregation.SingleValue) {
            return processSingleValue((NumericMetricsAggregation.SingleValue) agg);
        } else if (agg instanceof Percentiles) {
            return processPercentiles((Percentiles) agg);
        } else if (agg instanceof GeoCentroid) {
            return processGeoCentroid((GeoCentroid) agg);
        } else {
            throw new IllegalArgumentException("Unsupported aggregation type [" + agg.getName() + "]");
        }
    }

    private boolean processSingleValue(NumericMetricsAggregation.SingleValue singleValue) {
        return addMetricIfFinite(singleValue.getName(), singleValue.value());
    }

    private boolean addMetricIfFinite(String key, double value) {
        if (Double.isFinite(value)) {
            keyValuePairs.put(key, value);
            return true;
        }
        return false;
    }

    private boolean processGeoCentroid(GeoCentroid agg) {
        if (agg.count() > 0) {
            keyValuePairs.put(agg.getName(), agg.centroid().getY() + "," + agg.centroid().getX());
            return true;
        }
        return false;
    }

    private boolean processPercentiles(Percentiles percentiles) {
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
                    Messages.getMessage(Messages.DATAFEED_MISSING_MAX_AGGREGATION_FOR_TIME_FIELD, timeField)
                );
            }

            docsByBucketTimestamp.computeIfAbsent(timeStamp, (t) -> new ArrayList<>()).add(copy);
        }
    }

    /**
     * This writes ALL the documents stored within the processor object unless indicated otherwise by the `shouldCancel` predicate
     *
     * This returns `true` if it is safe to cancel the overall process as the current `date_histogram` bucket has finished.
     *
     * For a simple `date_histogram` this is guaranteed. But, for a `composite` agg, it is possible that the current page is in the
     * middle of a bucket. If you are writing with `composite` aggs, don't cancel the processing until this method returns true.
     *
     * @param shouldCancel determines if a given timestamp indicates that the processing stream should be cancelled
     * @param outputStream where to write the aggregated data
     * @return true if it is acceptable for the caller to close the process and cancel the stream
     * @throws IOException if there is a parsing exception
     */
    boolean writeAllDocsCancellable(Predicate<Long> shouldCancel, OutputStream outputStream) throws IOException {
        if (docsByBucketTimestamp.isEmpty()) {
            return true;
        }

        try (XContentBuilder jsonBuilder = new XContentBuilder(JsonXContent.jsonXContent, outputStream)) {
            Iterator<Map.Entry<Long, List<Map<String, Object>>>> iterator = docsByBucketTimestamp.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, List<Map<String, Object>>> entry = iterator.next();
                if (shouldCancel.test(entry.getKey())) {
                    return true;
                }
                for (Map<String, Object> map : entry.getValue()) {
                    writeJsonObject(jsonBuilder, map);
                }
                iterator.remove();
            }
        }

        return false;
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
