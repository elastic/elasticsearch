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
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalScriptedMetric extends InternalAggregation implements ScriptedMetric {
    final Script reduceScript;
    private final List<Object> aggregation;

    public InternalScriptedMetric(String name, Object aggregation, Script reduceScript, List<PipelineAggregator> pipelineAggregators,
                                  Map<String, Object> metaData) {
        this(name, Collections.singletonList(aggregation), reduceScript, pipelineAggregators, metaData);
    }

    private InternalScriptedMetric(String name, List<Object> aggregation, Script reduceScript, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.aggregation = aggregation;
        this.reduceScript = reduceScript;
    }

    /**
     * Read from a stream.
     */
    public InternalScriptedMetric(StreamInput in) throws IOException {
        super(in);
        reduceScript = in.readOptionalWriteable(Script::new);
        aggregation = Collections.singletonList(in.readGenericValue());
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(reduceScript);
        out.writeGenericValue(aggregation());
    }

    @Override
    public String getWriteableName() {
        return ScriptedMetricAggregationBuilder.NAME;
    }

    @Override
    public Object aggregation() {
        if (aggregation.size() != 1) {
            throw new IllegalStateException("aggregation was not reduced");
        }
        return aggregation.get(0);
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        List<Object> aggregationObjects = new ArrayList<>();
        for (InternalAggregation aggregation : aggregations) {
            InternalScriptedMetric mapReduceAggregation = (InternalScriptedMetric) aggregation;
            aggregationObjects.addAll(mapReduceAggregation.aggregation);
        }
        InternalScriptedMetric firstAggregation = ((InternalScriptedMetric) aggregations.get(0));
        List<Object> aggregation;
        if (firstAggregation.reduceScript != null && reduceContext.isFinalReduce()) {
            Map<String, Object> vars = new HashMap<>();
            vars.put("_aggs", aggregationObjects);
            if (firstAggregation.reduceScript.getParams() != null) {
                vars.putAll(firstAggregation.reduceScript.getParams());
            }
            ExecutableScript.Factory factory = reduceContext.scriptService().compile(
                firstAggregation.reduceScript, ExecutableScript.AGGS_CONTEXT);
            ExecutableScript script = factory.newInstance(vars);
            aggregation = Collections.singletonList(script.run());
        } else if (reduceContext.isFinalReduce())  {
            aggregation = Collections.singletonList(aggregationObjects);
        } else {
            // if we are not an final reduce we have to maintain all the aggs from all the incoming one
            // until we hit the final reduce phase.
            aggregation = aggregationObjects;
        }
        return new InternalScriptedMetric(firstAggregation.getName(), aggregation, firstAggregation.reduceScript, pipelineAggregators(),
                getMetaData());
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1 && "value".equals(path.get(0))) {
            return aggregation();
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder.field(CommonFields.VALUE.getPreferredName(), aggregation());
    }

    @Override
    protected boolean doEquals(Object obj) {
        InternalScriptedMetric other = (InternalScriptedMetric) obj;
        return Objects.equals(reduceScript, other.reduceScript) &&
                Objects.equals(aggregation, other.aggregation);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(reduceScript, aggregation);
    }

}
