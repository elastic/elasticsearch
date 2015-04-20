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

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalMetricsAggregation;

import java.io.IOException;
import java.util.ArrayList;
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

    private String scriptLang;
    private ScriptType scriptType;
    private String reduceScript;
    private Map<String, Object> reduceParams;
    private Object aggregation;

    private InternalScriptedMetric() {
    }

    private InternalScriptedMetric(String name, Map<String, Object> metaData) {
        super(name, metaData);
    }

    public InternalScriptedMetric(String name, Object aggregation, String scriptLang, ScriptType scriptType, String reduceScript,
            Map<String, Object> reduceParams, Map<String, Object> metaData) {
        this(name, metaData);
        this.aggregation = aggregation;
        this.scriptType = scriptType;
        this.reduceScript = reduceScript;
        this.reduceParams = reduceParams;
        this.scriptLang = scriptLang;
    }

    @Override
    public Object aggregation() {
        return aggregation;
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        List<Object> aggregationObjects = new ArrayList<>();
        for (InternalAggregation aggregation : aggregations) {
            InternalScriptedMetric mapReduceAggregation = (InternalScriptedMetric) aggregation;
            aggregationObjects.add(mapReduceAggregation.aggregation());
        }
        InternalScriptedMetric firstAggregation = ((InternalScriptedMetric) aggregations.get(0));
        Object aggregation;
        if (firstAggregation.reduceScript != null) {
            Map<String, Object> params;
            if (firstAggregation.reduceParams != null) {
                params = new HashMap<>(firstAggregation.reduceParams);
            } else {
                params = new HashMap<>();
            }
            params.put("_aggs", aggregationObjects);
            ExecutableScript script = reduceContext.scriptService().executable(new Script(firstAggregation.scriptLang, firstAggregation.reduceScript,
                    firstAggregation.scriptType, params), ScriptContext.Standard.AGGS);
            aggregation = script.run();
        } else {
            aggregation = aggregationObjects;
        }
        return new InternalScriptedMetric(firstAggregation.getName(), aggregation, firstAggregation.scriptLang, firstAggregation.scriptType,
                firstAggregation.reduceScript, firstAggregation.reduceParams, getMetaData());

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
            throw new ElasticsearchIllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        scriptLang = in.readOptionalString();
        scriptType = ScriptType.readFrom(in);
        reduceScript = in.readOptionalString();
        reduceParams = in.readMap();
        aggregation = in.readGenericValue();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalString(scriptLang);
        ScriptType.writeTo(scriptType, out);
        out.writeOptionalString(reduceScript);
        out.writeMap(reduceParams);
        out.writeGenericValue(aggregation);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder.field("value", aggregation);
    }

}
