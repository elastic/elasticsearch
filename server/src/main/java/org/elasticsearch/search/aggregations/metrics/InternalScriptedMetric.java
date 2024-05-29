/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptedMetricAggContexts;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalScriptedMetric extends InternalAggregation implements ScriptedMetric {
    final Script reduceScript;
    private final List<Object> aggregations;

    InternalScriptedMetric(String name, List<Object> aggregations, Script reduceScript, Map<String, Object> metadata) {
        super(name, metadata);
        this.aggregations = aggregations;
        this.reduceScript = reduceScript;
    }

    /**
     * Read from a stream.
     */
    public InternalScriptedMetric(StreamInput in) throws IOException {
        super(in);
        reduceScript = in.readOptionalWriteable(Script::new);
        aggregations = in.readCollectionAsList(StreamInput::readGenericValue);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(reduceScript);
        out.writeCollection(aggregations, StreamOutput::writeGenericValue);
    }

    @Override
    public String getWriteableName() {
        return ScriptedMetricAggregationBuilder.NAME;
    }

    @Override
    public Object aggregation() {
        if (aggregations.size() != 1) {
            throw new IllegalStateException("aggregation was not reduced");
        }
        return aggregations.get(0);
    }

    List<Object> aggregationsList() {
        return aggregations;
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        return new AggregatorReducer() {

            final List<Object> aggregationObjects = new ArrayList<>();

            @Override
            public void accept(InternalAggregation aggregation) {
                aggregationObjects.addAll(((InternalScriptedMetric) aggregation).aggregationsList());
            }

            @Override
            public InternalAggregation get() {
                List<Object> aggregation;
                if (reduceScript != null && reduceContext.isFinalReduce()) {
                    Map<String, Object> params = new HashMap<>();
                    if (reduceScript.getParams() != null) {
                        params.putAll(reduceScript.getParams());
                    }

                    ScriptedMetricAggContexts.ReduceScript.Factory factory = reduceContext.scriptService()
                        .compile(reduceScript, ScriptedMetricAggContexts.ReduceScript.CONTEXT);
                    ScriptedMetricAggContexts.ReduceScript script = factory.newInstance(params, aggregationObjects);

                    Object scriptResult = script.execute();
                    CollectionUtils.ensureNoSelfReferences(scriptResult, "reduce script");

                    aggregation = Collections.singletonList(scriptResult);
                } else if (reduceContext.isFinalReduce()) {
                    aggregation = Collections.singletonList(aggregationObjects);
                } else {
                    // if we are not an final reduce we have to maintain all the aggs from all the incoming one
                    // until we hit the final reduce phase.
                    aggregation = aggregationObjects;
                }
                return new InternalScriptedMetric(getName(), aggregation, reduceScript, getMetadata());
            }
        };
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return this;
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return true;
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
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalScriptedMetric other = (InternalScriptedMetric) obj;
        return Objects.equals(reduceScript, other.reduceScript) && Objects.equals(aggregations, other.aggregations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), reduceScript, aggregations);
    }

    @Override
    public String toString() {
        return "InternalScriptedMetric{" + reduceScript + "}";
    }
}
