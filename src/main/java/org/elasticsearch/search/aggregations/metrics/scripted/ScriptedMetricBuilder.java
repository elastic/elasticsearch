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
import org.elasticsearch.script.ScriptParameterParser;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * Builder for the {@link ScriptedMetric} aggregation.
 */
public class ScriptedMetricBuilder extends MetricsAggregationBuilder {

    private Map<String, Object> params = null;
    private Map<String, Object> reduceParams = null;
    private String initScript = null;
    private String mapScript = null;
    private String combineScript = null;
    private String reduceScript = null;
    private String initScriptFile = null;
    private String mapScriptFile = null;
    private String combineScriptFile = null;
    private String reduceScriptFile = null;
    private String initScriptId = null;
    private String mapScriptId = null;
    private String combineScriptId = null;
    private String reduceScriptId = null;
    private String lang = null;

    /**
     * Sole constructor.
     */
    public ScriptedMetricBuilder(String name) {
        super(name, InternalScriptedMetric.TYPE.name());
    }

    /**
     * Set parameters that will be available in the <tt>init</tt>, <tt>map</tt>
     * and <tt>combine</tt> phases.
     */
    public ScriptedMetricBuilder params(Map<String, Object> params) {
        this.params = params;
        return this;
    }

    /**
     * Set parameters that will be available in the <tt>reduce</tt> phase.
     */
    public ScriptedMetricBuilder reduceParams(Map<String, Object> reduceParams) {
        this.reduceParams = reduceParams;
        return this;
    }

    /**
     * Set the <tt>init</tt> script.
     */
    public ScriptedMetricBuilder initScript(String initScript) {
        this.initScript = initScript;
        return this;
    }

    /**
     * Set the <tt>map</tt> script.
     */
    public ScriptedMetricBuilder mapScript(String mapScript) {
        this.mapScript = mapScript;
        return this;
    }

    /**
     * Set the <tt>combine</tt> script.
     */
    public ScriptedMetricBuilder combineScript(String combineScript) {
        this.combineScript = combineScript;
        return this;
    }

    /**
     * Set the <tt>reduce</tt> script.
     */
    public ScriptedMetricBuilder reduceScript(String reduceScript) {
        this.reduceScript = reduceScript;
        return this;
    }

    /**
     * Set the <tt>init</tt> script file.
     */
    public ScriptedMetricBuilder initScriptFile(String initScriptFile) {
        this.initScriptFile = initScriptFile;
        return this;
    }

    /**
     * Set the <tt>map</tt> script file.
     */
    public ScriptedMetricBuilder mapScriptFile(String mapScriptFile) {
        this.mapScriptFile = mapScriptFile;
        return this;
    }

    /**
     * Set the <tt>combine</tt> script file.
     */
    public ScriptedMetricBuilder combineScriptFile(String combineScriptFile) {
        this.combineScriptFile = combineScriptFile;
        return this;
    }

    /**
     * Set the <tt>reduce</tt> script file.
     */
    public ScriptedMetricBuilder reduceScriptFile(String reduceScriptFile) {
        this.reduceScriptFile = reduceScriptFile;
        return this;
    }

    /**
     * Set the indexed <tt>init</tt> script id.
     */
    public ScriptedMetricBuilder initScriptId(String initScriptId) {
        this.initScriptId = initScriptId;
        return this;
    }

    /**
     * Set the indexed <tt>map</tt> script id.
     */
    public ScriptedMetricBuilder mapScriptId(String mapScriptId) {
        this.mapScriptId = mapScriptId;
        return this;
    }

    /**
     * Set the indexed <tt>combine</tt> script id.
     */
    public ScriptedMetricBuilder combineScriptId(String combineScriptId) {
        this.combineScriptId = combineScriptId;
        return this;
    }

    /**
     * Set the indexed <tt>reduce</tt> script id.
     */
    public ScriptedMetricBuilder reduceScriptId(String reduceScriptId) {
        this.reduceScriptId = reduceScriptId;
        return this;
    }

    /**
     * Set the script language.
     */
    public ScriptedMetricBuilder lang(String lang) {
        this.lang = lang;
        return this;
    }

    @Override
    protected void internalXContent(XContentBuilder builder, Params builderParams) throws IOException {
        if (params != null) {
            builder.field(ScriptedMetricParser.PARAMS_FIELD.getPreferredName());
            builder.map(params);
        }
        
        if (reduceParams != null) {
            builder.field(ScriptedMetricParser.REDUCE_PARAMS_FIELD.getPreferredName());
            builder.map(reduceParams);
        }
        
        if (initScript != null) {
            builder.field(ScriptedMetricParser.INIT_SCRIPT, initScript);
        }
        
        if (mapScript != null) {
            builder.field(ScriptedMetricParser.MAP_SCRIPT, mapScript);
        }
        
        if (combineScript != null) {
            builder.field(ScriptedMetricParser.COMBINE_SCRIPT, combineScript);
        }
        
        if (reduceScript != null) {
            builder.field(ScriptedMetricParser.REDUCE_SCRIPT, reduceScript);
        }
        
        if (initScriptFile != null) {
            builder.field(ScriptedMetricParser.INIT_SCRIPT + ScriptParameterParser.FILE_SUFFIX, initScriptFile);
        }
        
        if (mapScriptFile != null) {
            builder.field(ScriptedMetricParser.MAP_SCRIPT + ScriptParameterParser.FILE_SUFFIX, mapScriptFile);
        }
        
        if (combineScriptFile != null) {
            builder.field(ScriptedMetricParser.COMBINE_SCRIPT + ScriptParameterParser.FILE_SUFFIX, combineScriptFile);
        }
        
        if (reduceScriptFile != null) {
            builder.field(ScriptedMetricParser.REDUCE_SCRIPT + ScriptParameterParser.FILE_SUFFIX, reduceScriptFile);
        }
        
        if (initScriptId != null) {
            builder.field(ScriptedMetricParser.INIT_SCRIPT + ScriptParameterParser.INDEXED_SUFFIX, initScriptId);
        }
        
        if (mapScriptId != null) {
            builder.field(ScriptedMetricParser.MAP_SCRIPT + ScriptParameterParser.INDEXED_SUFFIX, mapScriptId);
        }
        
        if (combineScriptId != null) {
            builder.field(ScriptedMetricParser.COMBINE_SCRIPT + ScriptParameterParser.INDEXED_SUFFIX, combineScriptId);
        }
        
        if (reduceScriptId != null) {
            builder.field(ScriptedMetricParser.REDUCE_SCRIPT + ScriptParameterParser.INDEXED_SUFFIX, reduceScriptId);
        }
        
        if (lang != null) {
            builder.field(ScriptedMetricParser.LANG_FIELD.getPreferredName(), lang);
        }
    }

}
