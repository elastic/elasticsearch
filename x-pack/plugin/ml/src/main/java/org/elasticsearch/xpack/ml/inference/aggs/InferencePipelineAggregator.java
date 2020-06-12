/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.aggs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.InvalidAggregationPathException;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.ml.inference.loadingservice.Model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


public class InferencePipelineAggregator extends PipelineAggregator {

    private static final Logger logger = LogManager.getLogger(InferencePipelineAggregator.class);

    private Map<String, String> bucketPathMap;
    private InferenceConfigUpdate configUpdate;
    private final BucketHelpers.GapPolicy gapPolicy;
    private Model model;

    public InferencePipelineAggregator(String name, Map<String, String> bucketPathMap, Map<String, Object> metaData,
                                       BucketHelpers.GapPolicy gapPolicy,
                                       InferenceConfigUpdate configUpdate,
                                       Model model) {
        super(name, bucketPathMap.values().toArray(new String[] {}), metaData);
        this.bucketPathMap = bucketPathMap;
        this.gapPolicy = gapPolicy;
        this.configUpdate = configUpdate;
        this.model = model;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, InternalAggregation.ReduceContext reduceContext) {
        logger.warn("starting agg");

        InternalMultiBucketAggregation<InternalMultiBucketAggregation, InternalMultiBucketAggregation.InternalBucket> originalAgg =
            (InternalMultiBucketAggregation<InternalMultiBucketAggregation, InternalMultiBucketAggregation.InternalBucket>) aggregation;
        List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = originalAgg.getBuckets();

        List<InternalMultiBucketAggregation.InternalBucket> newBuckets = new ArrayList<>();
        for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
            Map<String, Object> inputFields = new HashMap<>();
            boolean skipBucket = false;
            for (Map.Entry<String, String> entry : bucketPathMap.entrySet()) {
                String aggName = entry.getKey();
                String bucketPath = entry.getValue();
                logger.warn("bucket path {} ", bucketPath);
                Object value = resolveBucketValue(originalAgg, bucket, bucketPath, gapPolicy);

                if (BucketHelpers.GapPolicy.SKIP == gapPolicy && value == null) {
                    logger.info("skipping");
                    skipBucket = true;
                    break;
                }

                logger.warn("got field {} : {}", aggName, value);
                inputFields.put(aggName, value);
            }
            if (skipBucket) {
                newBuckets.add(bucket);
                continue;
            }

            InferenceResults inference;
            try {
                 inference = model.infer(inputFields, configUpdate);
            } catch (Exception e) {
                inference = new WarningInferenceResults(e.getMessage());
            }

            final List<InternalAggregation> aggs = StreamSupport.stream(bucket.getAggregations().spliterator(), false).map(
                (p) -> (InternalAggregation) p).collect(Collectors.toList());


            InternalInferenceAggregation infResult = new InternalInferenceAggregation(name(), metadata(), inference);
            aggs.add(infResult);
            InternalMultiBucketAggregation.InternalBucket newBucket = originalAgg.createBucket(new InternalAggregations(aggs),
                bucket);
            newBuckets.add(newBucket);

        }
        return originalAgg.create(newBuckets);
    }

    public static Object resolveBucketValue(MultiBucketsAggregation agg,
                                            InternalMultiBucketAggregation.InternalBucket bucket,
                                            String aggPath,
                                            BucketHelpers.GapPolicy gapPolicy) {
        List<String> aggPathsList = AggregationPath.parse(aggPath).getPathElementsAsStringList();
        return resolveBucketValue(agg, bucket, aggPathsList, gapPolicy);
    }

    public static Object resolveBucketValue(MultiBucketsAggregation agg,
                                            InternalMultiBucketAggregation.InternalBucket bucket,
                                            List<String> aggPathAsList, BucketHelpers.GapPolicy gapPolicy) {
        try {
            Object propertyValue = bucket.getProperty(agg.getName(), aggPathAsList);

            if (propertyValue == null) {
                throw new AggregationExecutionException(AbstractPipelineAggregationBuilder.BUCKETS_PATH_FIELD.getPreferredName()
                    + " must reference either a number value or a single value numeric metric aggregation");
            } else {
                return propertyValue;
            }
        } catch (InvalidAggregationPathException e) {
            logger.error("parse error", e);
            return null;
        }
    }

    private static AggregationExecutionException formatResolutionError(MultiBucketsAggregation agg,
                                                                       List<String> aggPathAsList, Object propertyValue) {
        String currentAggName;
        Object currentAgg;
        if (aggPathAsList.isEmpty()) {
            currentAggName = agg.getName();
            currentAgg = agg;
        } else {
            currentAggName = aggPathAsList.get(0);
            currentAgg = propertyValue;
        }
        if (currentAgg instanceof InternalNumericMetricsAggregation.MultiValue) {
            return new AggregationExecutionException(AbstractPipelineAggregationBuilder.BUCKETS_PATH_FIELD.getPreferredName()
                + " must reference either a number value or a single value numeric metric aggregation, but [" + currentAggName
                + "] contains multiple values. Please specify which to use.");
        } else {
            return new AggregationExecutionException(AbstractPipelineAggregationBuilder.BUCKETS_PATH_FIELD.getPreferredName()
                + " must reference either a number value or a single value numeric metric aggregation, got: ["
                + propertyValue.getClass().getSimpleName() + "] at aggregation [" + currentAggName + "]");
        }
    }
}
