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

    private InternalScriptedMetric(String name) {
        super(name);
    }

    public InternalScriptedMetric(String name, Object aggregation, String scriptLang, ScriptType scriptType, String reduceScript,
            Map<String, Object> reduceParams) {
        this(name);
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
    public InternalAggregation reduce(ReduceContext reduceContext) {
        List<Object> aggregationObjects = new ArrayList<>();
        for (InternalAggregation aggregation : reduceContext.aggregations()) {
            InternalScriptedMetric mapReduceAggregation = (InternalScriptedMetric) aggregation;
            aggregationObjects.add(mapReduceAggregation.aggregation());
        }
        InternalScriptedMetric firstAggregation = ((InternalScriptedMetric) reduceContext.aggregations().get(0));
        Object aggregation;
        if (firstAggregation.reduceScript != null) {
            Map<String, Object> params;
            if (firstAggregation.reduceParams != null) {
                params = new HashMap<String, Object>(firstAggregation.reduceParams);
            } else {
                params = new HashMap<String, Object>();
            }
            params.put("_aggs", aggregationObjects);
            ExecutableScript script = reduceContext.scriptService().executable(firstAggregation.scriptLang, firstAggregation.reduceScript,
                    firstAggregation.scriptType, params);
            aggregation = script.run();
        } else {
            aggregation = aggregationObjects;
        }
        return new InternalScriptedMetric(firstAggregation.getName(), aggregation, firstAggregation.scriptLang, firstAggregation.scriptType,
                firstAggregation.reduceScript, firstAggregation.reduceParams);

    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        scriptLang = in.readOptionalString();
        scriptType = ScriptType.readFrom(in);
        reduceScript = in.readOptionalString();
        reduceParams = in.readMap();
        aggregation = in.readGenericValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
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
