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

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptedMetricAggContexts;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.singletonList;

public class InternalScriptedMetric extends InternalAggregation implements ScriptedMetric {
    final Script reduceScript;
    private final List<Object> aggregations;

    InternalScriptedMetric(String name, Object aggregation, Script reduceScript, Map<String, Object> metadata) {
        this(name, Collections.singletonList(aggregation), reduceScript, metadata);
    }


    private InternalScriptedMetric(String name, List<Object> aggregations, Script reduceScript, Map<String, Object> metadata) {
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
        if (in.getVersion().before(Version.V_7_8_0)) {
            aggregations = singletonList(in.readGenericValue());
        } else {
            aggregations = in.readList(StreamInput::readGenericValue);
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(reduceScript);
        if (out.getVersion().before(Version.V_7_8_0)) {
            if (aggregations.size() > 0) {
                /*
                 * I *believe* that this situation can only happen in cross
                 * cluster search right now. Thus the message. But computers
                 * are hard. 
                 */
                throw new IllegalArgumentException("scripted_metric doesn't support cross cluster search until 7.8.0");
            }
            out.writeGenericValue(aggregations.get(0));
        } else {
            out.writeCollection(aggregations, StreamOutput::writeGenericValue);
        }
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

    List<Object> getAggregation() {
        return aggregations;
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        List<Object> aggregationObjects = new ArrayList<>();
        for (InternalAggregation aggregation : aggregations) {
            InternalScriptedMetric mapReduceAggregation = (InternalScriptedMetric) aggregation;
            aggregationObjects.addAll(mapReduceAggregation.aggregations);
        }
        InternalScriptedMetric firstAggregation = ((InternalScriptedMetric) aggregations.get(0));
        List<Object> aggregation;
        if (firstAggregation.reduceScript != null && reduceContext.isFinalReduce()) {
            Map<String, Object> params = new HashMap<>();
            if (firstAggregation.reduceScript.getParams() != null) {
                params.putAll(firstAggregation.reduceScript.getParams());
            }

            ScriptedMetricAggContexts.ReduceScript.Factory factory = reduceContext.scriptService().compile(
                firstAggregation.reduceScript, ScriptedMetricAggContexts.ReduceScript.CONTEXT);
            ScriptedMetricAggContexts.ReduceScript script = factory.newInstance(params, aggregationObjects);

            Object scriptResult = script.execute();
            CollectionUtils.ensureNoSelfReferences(scriptResult, "reduce script");

            aggregation = Collections.singletonList(scriptResult);
        } else if (reduceContext.isFinalReduce())  {
            aggregation = Collections.singletonList(aggregationObjects);
        } else {
            // if we are not an final reduce we have to maintain all the aggs from all the incoming one
            // until we hit the final reduce phase.
            aggregation = aggregationObjects;
        }
        return new InternalScriptedMetric(firstAggregation.getName(), aggregation, firstAggregation.reduceScript, getMetadata());
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
        return Objects.equals(reduceScript, other.reduceScript) &&
                Objects.equals(aggregations, other.aggregations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), reduceScript, aggregations);
    }

}
