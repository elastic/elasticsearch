/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.script.BucketAggregationSelectorScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

public class BucketSelectorPipelineAggregator extends PipelineAggregator {
    private GapPolicy gapPolicy;
    private Script script;
    private Map<String, String> bucketsPathsMap;

    BucketSelectorPipelineAggregator(String name, Map<String, String> bucketsPathsMap, Script script, GapPolicy gapPolicy,
            Map<String, Object> metadata) {
        super(name, bucketsPathsMap.values().toArray(new String[0]), metadata);
        this.bucketsPathsMap = bucketsPathsMap;
        this.script = script;
        this.gapPolicy = gapPolicy;
    }

    /**
     * Read from a stream.
     */
    @SuppressWarnings("unchecked")
    public BucketSelectorPipelineAggregator(StreamInput in) throws IOException {
        super(in);
        script = new Script(in);
        gapPolicy = GapPolicy.readFrom(in);
        bucketsPathsMap = (Map<String, String>) in.readGenericValue();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        script.writeTo(out);
        gapPolicy.writeTo(out);
        out.writeGenericValue(bucketsPathsMap);
    }

    @Override
    public String getWriteableName() {
        return BucketSelectorPipelineAggregationBuilder.NAME;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
        @SuppressWarnings({"rawtypes", "unchecked"})
        InternalMultiBucketAggregation<InternalMultiBucketAggregation, InternalMultiBucketAggregation.InternalBucket> originalAgg =
                (InternalMultiBucketAggregation<InternalMultiBucketAggregation, InternalMultiBucketAggregation.InternalBucket>) aggregation;
        List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = originalAgg.getBuckets();

        BucketAggregationSelectorScript.Factory factory =
            reduceContext.scriptService().compile(script, BucketAggregationSelectorScript.CONTEXT);
        List<InternalMultiBucketAggregation.InternalBucket> newBuckets = new ArrayList<>();
        for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
            Map<String, Object> vars = new HashMap<>();
            if (script.getParams() != null) {
                vars.putAll(script.getParams());
            }
            for (Map.Entry<String, String> entry : bucketsPathsMap.entrySet()) {
                String varName = entry.getKey();
                String bucketsPath = entry.getValue();
                Double value = resolveBucketValue(originalAgg, bucket, bucketsPath, gapPolicy);
                vars.put(varName, value);
            }
            // TODO: can we use one instance of the script for all buckets? it should be stateless?
            BucketAggregationSelectorScript executableScript = factory.newInstance(vars);
            if (executableScript.execute()) {
                newBuckets.add(bucket);
            }
        }
        return originalAgg.create(newBuckets);
    }
}
