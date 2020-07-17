/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.aggs;

import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class InferencePipelineAggregator extends PipelineAggregator {

    private final Map<String, String> bucketPathMap;
    private final InferenceConfigUpdate configUpdate;
    private final LocalModel model;

    public InferencePipelineAggregator(String name, Map<String,
                                       String> bucketPathMap,
                                       Map<String, Object> metaData,
                                       InferenceConfigUpdate configUpdate,
                                       LocalModel model) {
        super(name, bucketPathMap.values().toArray(new String[] {}), metaData);
        this.bucketPathMap = bucketPathMap;
        this.configUpdate = configUpdate;
        this.model = model;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, InternalAggregation.ReduceContext reduceContext) {

        try (model) {
            InternalMultiBucketAggregation<InternalMultiBucketAggregation, InternalMultiBucketAggregation.InternalBucket> originalAgg =
                (InternalMultiBucketAggregation<InternalMultiBucketAggregation, InternalMultiBucketAggregation.InternalBucket>) aggregation;
            List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = originalAgg.getBuckets();

            List<InternalMultiBucketAggregation.InternalBucket> newBuckets = new ArrayList<>();
            for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
                Map<String, Object> inputFields = new HashMap<>();

                if (bucket.getDocCount() == 0) {
                    // ignore this empty bucket unless the doc count is used
                    if (bucketPathMap.containsKey("_count") == false) {
                        newBuckets.add(bucket);
                        continue;
                    }
                }

                for (Map.Entry<String, String> entry : bucketPathMap.entrySet()) {
                    String aggName = entry.getKey();
                    String bucketPath = entry.getValue();
                    Object propertyValue = resolveBucketValue(originalAgg, bucket, bucketPath);

                    if (propertyValue instanceof Number) {
                        double doubleVal = ((Number) propertyValue).doubleValue();
                        // NaN or infinite values indicate a missing value or a
                        // valid result of an invalid calculation. Either way only
                        // a valid number will do
                        if (Double.isFinite(doubleVal)) {
                            inputFields.put(aggName, doubleVal);
                        }
                    } else if (propertyValue instanceof InternalNumericMetricsAggregation.SingleValue) {
                        double doubleVal = ((InternalNumericMetricsAggregation.SingleValue) propertyValue).value();
                        if (Double.isFinite(doubleVal)) {
                            inputFields.put(aggName, doubleVal);
                        }
                    } else if (propertyValue instanceof StringTerms.Bucket) {
                        StringTerms.Bucket b = (StringTerms.Bucket) propertyValue;
                        inputFields.put(aggName, b.getKeyAsString());
                    } else if (propertyValue instanceof String) {
                        inputFields.put(aggName, propertyValue);
                    } else if (propertyValue != null) {
                        // Doubles, String terms or null are valid, any other type is an error
                        throw invalidAggTypeError(bucketPath, propertyValue);
                    }
                }


                InferenceResults inference;
                try {
                    inference = model.infer(inputFields, configUpdate);
                } catch (Exception e) {
                    inference = new WarningInferenceResults(e.getMessage());
                }

                final List<InternalAggregation> aggs = bucket.getAggregations().asList().stream().map(
                    (p) -> (InternalAggregation) p).collect(Collectors.toList());

                InternalInferenceAggregation aggResult = new InternalInferenceAggregation(name(), metadata(), inference);
                aggs.add(aggResult);
                InternalMultiBucketAggregation.InternalBucket newBucket = originalAgg.createBucket(InternalAggregations.from(aggs), bucket);
                newBuckets.add(newBucket);
            }

            // the model is released at the end of this block.
            assert model.getReferenceCount() > 0;

            return originalAgg.create(newBuckets);
        }
    }

    public static Object resolveBucketValue(MultiBucketsAggregation agg,
                                            InternalMultiBucketAggregation.InternalBucket bucket,
                                            String aggPath) {

        List<String> aggPathsList = AggregationPath.parse(aggPath).getPathElementsAsStringList();
        return bucket.getProperty(agg.getName(), aggPathsList);
    }

    private static AggregationExecutionException invalidAggTypeError(String aggPath, Object propertyValue) {

        String msg = AbstractPipelineAggregationBuilder.BUCKETS_PATH_FIELD.getPreferredName() +
            " must reference either a number value, a single value numeric metric aggregation or a string: got [" +
            propertyValue + "] of type [" + propertyValue.getClass().getSimpleName() + "] " +
            "] at aggregation [" + aggPath + "]";
        return new AggregationExecutionException(msg);
    }
}
