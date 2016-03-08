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
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregatorBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class ScriptedMetricAggregatorBuilder extends AggregatorBuilder<ScriptedMetricAggregatorBuilder> {

    static final ScriptedMetricAggregatorBuilder PROTOTYPE = new ScriptedMetricAggregatorBuilder("");

    private Script initScript;
    private Script mapScript;
    private Script combineScript;
    private Script reduceScript;
    private Map<String, Object> params;

    public ScriptedMetricAggregatorBuilder(String name) {
        super(name, InternalScriptedMetric.TYPE);
    }

    /**
     * Set the <tt>init</tt> script.
     */
    public ScriptedMetricAggregatorBuilder initScript(Script initScript) {
        if (initScript == null) {
            throw new IllegalArgumentException("[initScript] must not be null: [" + name + "]");
        }
        this.initScript = initScript;
        return this;
    }

    /**
     * Get the <tt>init</tt> script.
     */
    public Script initScript() {
        return initScript;
    }

    /**
     * Set the <tt>map</tt> script.
     */
    public ScriptedMetricAggregatorBuilder mapScript(Script mapScript) {
        if (mapScript == null) {
            throw new IllegalArgumentException("[mapScript] must not be null: [" + name + "]");
        }
        this.mapScript = mapScript;
        return this;
    }

    /**
     * Get the <tt>map</tt> script.
     */
    public Script mapScript() {
        return mapScript;
    }

    /**
     * Set the <tt>combine</tt> script.
     */
    public ScriptedMetricAggregatorBuilder combineScript(Script combineScript) {
        if (combineScript == null) {
            throw new IllegalArgumentException("[combineScript] must not be null: [" + name + "]");
        }
        this.combineScript = combineScript;
        return this;
    }

    /**
     * Get the <tt>combine</tt> script.
     */
    public Script combineScript() {
        return combineScript;
    }

    /**
     * Set the <tt>reduce</tt> script.
     */
    public ScriptedMetricAggregatorBuilder reduceScript(Script reduceScript) {
        if (reduceScript == null) {
            throw new IllegalArgumentException("[reduceScript] must not be null: [" + name + "]");
        }
        this.reduceScript = reduceScript;
        return this;
    }

    /**
     * Get the <tt>reduce</tt> script.
     */
    public Script reduceScript() {
        return reduceScript;
    }

    /**
     * Set parameters that will be available in the <tt>init</tt>,
     * <tt>map</tt> and <tt>combine</tt> phases.
     */
    public ScriptedMetricAggregatorBuilder params(Map<String, Object> params) {
        if (params == null) {
            throw new IllegalArgumentException("[params] must not be null: [" + name + "]");
        }
        this.params = params;
        return this;
    }

    /**
     * Get parameters that will be available in the <tt>init</tt>,
     * <tt>map</tt> and <tt>combine</tt> phases.
     */
    public Map<String, Object> params() {
        return params;
    }

    @Override
    protected ScriptedMetricAggregatorFactory doBuild(AggregationContext context, AggregatorFactory<?> parent,
            Builder subfactoriesBuilder) throws IOException {
        return new ScriptedMetricAggregatorFactory(name, type, initScript, mapScript, combineScript, reduceScript, params, context,
                parent, subfactoriesBuilder, metaData);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params builderParams) throws IOException {
        builder.startObject();
        if (initScript != null) {
            builder.field(ScriptedMetricParser.INIT_SCRIPT_FIELD.getPreferredName(), initScript);
        }

        if (mapScript != null) {
            builder.field(ScriptedMetricParser.MAP_SCRIPT_FIELD.getPreferredName(), mapScript);
        }

        if (combineScript != null) {
            builder.field(ScriptedMetricParser.COMBINE_SCRIPT_FIELD.getPreferredName(), combineScript);
        }

        if (reduceScript != null) {
            builder.field(ScriptedMetricParser.REDUCE_SCRIPT_FIELD.getPreferredName(), reduceScript);
        }
        if (params != null) {
            builder.field(ScriptedMetricParser.PARAMS_FIELD.getPreferredName());
            builder.map(params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    protected ScriptedMetricAggregatorBuilder doReadFrom(String name, StreamInput in) throws IOException {
        ScriptedMetricAggregatorBuilder factory = new ScriptedMetricAggregatorBuilder(name);
        factory.initScript = in.readOptionalStreamable(Script.SUPPLIER);
        factory.mapScript = in.readOptionalStreamable(Script.SUPPLIER);
        factory.combineScript = in.readOptionalStreamable(Script.SUPPLIER);
        factory.reduceScript = in.readOptionalStreamable(Script.SUPPLIER);
        if (in.readBoolean()) {
            factory.params = in.readMap();
        }
        return factory;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalStreamable(initScript);
        out.writeOptionalStreamable(mapScript);
        out.writeOptionalStreamable(combineScript);
        out.writeOptionalStreamable(reduceScript);
        boolean hasParams = params != null;
        out.writeBoolean(hasParams);
        if (hasParams) {
            out.writeMap(params);
        }
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(initScript, mapScript, combineScript, reduceScript, params);
    }

    @Override
    protected boolean doEquals(Object obj) {
        ScriptedMetricAggregatorBuilder other = (ScriptedMetricAggregatorBuilder) obj;
        return Objects.equals(initScript, other.initScript)
                && Objects.equals(mapScript, other.mapScript)
                && Objects.equals(combineScript, other.combineScript)
                && Objects.equals(reduceScript, other.reduceScript)
                && Objects.equals(params, other.params);
    }

}