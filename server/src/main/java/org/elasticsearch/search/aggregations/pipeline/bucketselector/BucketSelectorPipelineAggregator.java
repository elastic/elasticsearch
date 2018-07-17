/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.pipeline.bucketselector;


import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

public class BucketSelectorPipelineAggregator extends PipelineAggregator {
    private final GapPolicy gapPolicy;

    private final Script script;

    private final Predicate<Object> keepBucket;

    private final Map<String, String> bucketsPathsMap;

    public BucketSelectorPipelineAggregator(String name, Map<String, String> bucketsPathsMap, Script script, GapPolicy gapPolicy,
            Map<String, Object> metadata) {
        super(name, bucketsPathsMap.values().toArray(new String[bucketsPathsMap.size()]), metadata);
        this.bucketsPathsMap = bucketsPathsMap;
        this.script = script;
        keepBucket = buildKeepBucket(script);
        this.gapPolicy = gapPolicy;
    }

    /**
     * Read from a stream.
     */
    @SuppressWarnings("unchecked")
    public BucketSelectorPipelineAggregator(StreamInput in) throws IOException {
        super(in);
        script = new Script(in);
        keepBucket = buildKeepBucket(script);
        gapPolicy = GapPolicy.readFrom(in);
        bucketsPathsMap = (Map<String, String>) in.readGenericValue();
    }

    private static Predicate<Object> buildKeepBucket(Script script) {
        if ("expression".equals(script.getLang())) {
            return val -> (double) val == 1.0;
        } else {
            return val -> (boolean) val;
        }
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
        InternalMultiBucketAggregation<InternalMultiBucketAggregation, InternalMultiBucketAggregation.InternalBucket> originalAgg =
                (InternalMultiBucketAggregation<InternalMultiBucketAggregation, InternalMultiBucketAggregation.InternalBucket>) aggregation;
        List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = originalAgg.getBuckets();

        ExecutableScript.Factory factory = reduceContext.scriptService().compile(script, ExecutableScript.AGGS_CONTEXT);
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
            ExecutableScript executableScript = factory.newInstance(vars);
            Object scriptReturnValue = executableScript.run();
            if (keepBucket.test(scriptReturnValue)) {
                newBuckets.add(bucket);
            }
        }
        return originalAgg.create(newBuckets);
    }
}
