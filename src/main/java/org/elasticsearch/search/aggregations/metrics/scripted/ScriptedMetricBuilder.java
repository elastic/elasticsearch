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
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;

import java.io.IOException;
import java.util.Map;

public class ScriptedMetricBuilder extends MetricsAggregationBuilder {

    private Map<String, Object> params = null;
    private Map<String, Object> reduceParams = null;
    private ScriptType scriptType = null;
    private String initScript = null;
    private String mapScript = null;
    private String combineScript = null;
    private String reduceScript = null;
    private String lang = null;

    public ScriptedMetricBuilder(String name) {
        super(name, InternalScriptedMetric.TYPE.name());
    }

    public ScriptedMetricBuilder params(Map<String, Object> params) {
        this.params = params;
        return this;
    }

    public ScriptedMetricBuilder reduceParams(Map<String, Object> reduceParams) {
        this.reduceParams = reduceParams;
        return this;
    }

    public ScriptedMetricBuilder initScript(String initScript) {
        this.initScript = initScript;
        return this;
    }

    public ScriptedMetricBuilder mapScript(String mapScript) {
        this.mapScript = mapScript;
        return this;
    }

    public ScriptedMetricBuilder combineScript(String combineScript) {
        this.combineScript = combineScript;
        return this;
    }

    public ScriptedMetricBuilder reduceScript(String reduceScript) {
        this.reduceScript = reduceScript;
        return this;
    }

    public ScriptedMetricBuilder lang(String lang) {
        this.lang = lang;
        return this;
    }

    public ScriptedMetricBuilder scriptType(ScriptType scriptType) {
        this.scriptType = scriptType;
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
        
        if (lang != null) {
            builder.field(ScriptedMetricParser.LANG_FIELD.getPreferredName(), lang);
        }
        
        if (scriptType != null) {
            builder.field(ScriptedMetricParser.SCRIPT_TYPE_FIELD.getPreferredName(), scriptType.name());
        }
    }

}
