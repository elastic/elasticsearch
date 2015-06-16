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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptParameterParser;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * Builder for the {@link ScriptedMetric} aggregation.
 */
public class ScriptedMetricBuilder extends MetricsAggregationBuilder {

    private Script initScript = null;
    private Script mapScript = null;
    private Script combineScript = null;
    private Script reduceScript = null;
    private Map<String, Object> params = null;

    /**
     * Sole constructor.
     */
    public ScriptedMetricBuilder(String name) {
        super(name, InternalScriptedMetric.TYPE.name());
    }

    /**
     * Set the <tt>init</tt> script.
     */
    public ScriptedMetricBuilder initScript(Script initScript) {
        this.initScript = initScript;
        return this;
    }

    /**
     * Set the <tt>map</tt> script.
     */
    public ScriptedMetricBuilder mapScript(Script mapScript) {
        this.mapScript = mapScript;
        return this;
    }

    /**
     * Set the <tt>combine</tt> script.
     */
    public ScriptedMetricBuilder combineScript(Script combineScript) {
        this.combineScript = combineScript;
        return this;
    }

    /**
     * Set the <tt>reduce</tt> script.
     */
    public ScriptedMetricBuilder reduceScript(Script reduceScript) {
        this.reduceScript = reduceScript;
        return this;
    }

    /**
     * Set parameters that will be available in the <tt>init</tt>, <tt>map</tt>
     * and <tt>combine</tt> phases.
     */
    public ScriptedMetricBuilder params(Map<String, Object> params) {
        this.params = params;
        return this;
    }

    @Override
    protected void internalXContent(XContentBuilder builder, Params builderParams) throws IOException {

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
    }

}
