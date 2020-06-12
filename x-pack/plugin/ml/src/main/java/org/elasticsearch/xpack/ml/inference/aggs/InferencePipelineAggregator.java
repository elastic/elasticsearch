/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.aggs;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
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

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

public class InferencePipelineAggregator extends PipelineAggregator {

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
                Double value = resolveBucketValue(originalAgg, bucket, bucketPath, gapPolicy);
                if (BucketHelpers.GapPolicy.SKIP == gapPolicy && (value == null || Double.isNaN(value))) {
                    skipBucket = true;
                    break;
                }
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


            InternalSimpleValue simpleValue = new InternalSimpleValue(name(), 10.1, DocValueFormat.RAW, metadata());
            aggs.add(simpleValue);
            InternalMultiBucketAggregation.InternalBucket newBucket = originalAgg.createBucket(new InternalAggregations(aggs),
                bucket);
            newBuckets.add(newBucket);

        }
        return originalAgg.create(newBuckets);
    }
}
