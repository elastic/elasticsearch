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

package org.elasticsearch.search.aggregations.metrics.scripted;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalMetricsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InternalScriptedMetric extends InternalMetricsAggregation implements ScriptedMetric {

    public final static Type TYPE = new Type("scripted_metric");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalScriptedMetric readResult(StreamInput in) throws IOException {
            InternalScriptedMetric result = new InternalScriptedMetric();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private Script reduceScript;
    private Object aggregation;

    private InternalScriptedMetric() {
    }

    private InternalScriptedMetric(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
    }

    public InternalScriptedMetric(String name, Object aggregation, Script reduceScript, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        this(name, pipelineAggregators, metaData);
        this.aggregation = aggregation;
        this.reduceScript = reduceScript;
    }

    @Override
    public Object aggregation() {
        return aggregation;
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        List<Object> aggregationObjects = new ArrayList<>();
        for (InternalAggregation aggregation : aggregations) {
            InternalScriptedMetric mapReduceAggregation = (InternalScriptedMetric) aggregation;
            aggregationObjects.add(mapReduceAggregation.aggregation());
        }
        InternalScriptedMetric firstAggregation = ((InternalScriptedMetric) aggregations.get(0));
        Object aggregation;
        if (firstAggregation.reduceScript != null) {
            Map<String, Object> vars = new HashMap<>();
            vars.put("_aggs", aggregationObjects);
            if (firstAggregation.reduceScript.getParams() != null) {
                vars.putAll(firstAggregation.reduceScript.getParams());
            }
            CompiledScript compiledScript = reduceContext.scriptService().compile(firstAggregation.reduceScript,
                    ScriptContext.Standard.AGGS, Collections.emptyMap(), reduceContext.clusterState());
            ExecutableScript script = reduceContext.scriptService().executable(compiledScript, vars);
            aggregation = script.run();
        } else {
            aggregation = aggregationObjects;
        }
        return new InternalScriptedMetric(firstAggregation.getName(), aggregation, firstAggregation.reduceScript, pipelineAggregators(), getMetaData());

    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1 && "value".equals(path.get(0))) {
            return aggregation;
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            reduceScript = new Script(in);
        }
        aggregation = in.readGenericValue();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        boolean hasScript = reduceScript != null;
        out.writeBoolean(hasScript);
        if (hasScript) {
            reduceScript.writeTo(out);
        }
        out.writeGenericValue(aggregation);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder.field("value", aggregation);
    }

}
