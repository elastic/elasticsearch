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
import org.elasticsearch.script.BucketAggregationScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

public class BucketScriptPipelineAggregator extends PipelineAggregator {
    private final DocValueFormat formatter;
    private final GapPolicy gapPolicy;
    private final Script script;
    private final Map<String, String> bucketsPathsMap;

    BucketScriptPipelineAggregator(
        String name,
        Map<String, String> bucketsPathsMap,
        Script script,
        DocValueFormat formatter,
        GapPolicy gapPolicy,
        Map<String, Object> metadata
    ) {
        super(name, bucketsPathsMap.values().toArray(new String[0]), metadata);
        this.bucketsPathsMap = bucketsPathsMap;
        this.script = script;
        this.formatter = formatter;
        this.gapPolicy = gapPolicy;
    }

    /**
     * Read from a stream.
     */
    @SuppressWarnings("unchecked")
    public BucketScriptPipelineAggregator(StreamInput in) throws IOException {
        super(in);
        script = new Script(in);
        formatter = in.readNamedWriteable(DocValueFormat.class);
        gapPolicy = GapPolicy.readFrom(in);
        bucketsPathsMap = (Map<String, String>) in.readGenericValue();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        script.writeTo(out);
        out.writeNamedWriteable(formatter);
        gapPolicy.writeTo(out);
        out.writeGenericValue(bucketsPathsMap);
    }

    @Override
    public String getWriteableName() {
        return BucketScriptPipelineAggregationBuilder.NAME;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
        @SuppressWarnings({ "rawtypes", "unchecked" })
        InternalMultiBucketAggregation<InternalMultiBucketAggregation, InternalMultiBucketAggregation.InternalBucket> originalAgg =
            (InternalMultiBucketAggregation<InternalMultiBucketAggregation, InternalMultiBucketAggregation.InternalBucket>) aggregation;
        List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = originalAgg.getBuckets();

        BucketAggregationScript.Factory factory = reduceContext.scriptService().compile(script, BucketAggregationScript.CONTEXT);
        List<InternalMultiBucketAggregation.InternalBucket> newBuckets = new ArrayList<>();
        for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
            Map<String, Object> vars = new HashMap<>();
            if (script.getParams() != null) {
                vars.putAll(script.getParams());
            }
            boolean skipBucket = false;
            for (Map.Entry<String, String> entry : bucketsPathsMap.entrySet()) {
                String varName = entry.getKey();
                String bucketsPath = entry.getValue();
                Double value = resolveBucketValue(originalAgg, bucket, bucketsPath, gapPolicy);
                if (gapPolicy.isSkippable && (value == null || Double.isNaN(value))) {
                    skipBucket = true;
                    break;
                }
                vars.put(varName, value);
            }
            if (skipBucket) {
                newBuckets.add(bucket);
            } else {
                Number returned = factory.newInstance(vars).execute();
                if (returned == null) {
                    newBuckets.add(bucket);
                } else {
                    final List<InternalAggregation> aggs = StreamSupport.stream(bucket.getAggregations().spliterator(), false)
                        .map((p) -> (InternalAggregation) p)
                        .collect(Collectors.toList());

                    InternalSimpleValue simpleValue = new InternalSimpleValue(name(), returned.doubleValue(), formatter, metadata());
                    aggs.add(simpleValue);
                    InternalMultiBucketAggregation.InternalBucket newBucket = originalAgg.createBucket(
                        InternalAggregations.from(aggs),
                        bucket
                    );
                    newBuckets.add(newBucket);
                }
            }
        }
        return originalAgg.create(newBuckets);
    }
}
