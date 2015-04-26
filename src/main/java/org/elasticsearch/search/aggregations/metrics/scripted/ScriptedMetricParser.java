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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ScriptParameterParser;
import org.elasticsearch.script.ScriptParameterParser.ScriptParameterValue;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ScriptedMetricParser implements Aggregator.Parser {
    
    public static final String INIT_SCRIPT = "init_script";
    public static final String MAP_SCRIPT = "map_script";
    public static final String COMBINE_SCRIPT = "combine_script";
    public static final String REDUCE_SCRIPT = "reduce_script";
    public static final ParseField PARAMS_FIELD = new ParseField("params");
    public static final ParseField REDUCE_PARAMS_FIELD = new ParseField("reduce_params");
    public static final ParseField LANG_FIELD = new ParseField("lang");

    @Override
    public String type() {
        return InternalScriptedMetric.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {
        String scriptLang;
        Map<String, Object> params = null;
        Map<String, Object> reduceParams = null;
        XContentParser.Token token;
        String currentFieldName = null;
        Set<String> scriptParameters = new HashSet<>();
        scriptParameters.add(INIT_SCRIPT);
        scriptParameters.add(MAP_SCRIPT);
        scriptParameters.add(COMBINE_SCRIPT);
        scriptParameters.add(REDUCE_SCRIPT);
        ScriptParameterParser scriptParameterParser = new ScriptParameterParser(scriptParameters);

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (PARAMS_FIELD.match(currentFieldName)) {
                    params = parser.map();
                } else if (REDUCE_PARAMS_FIELD.match(currentFieldName)) {
                  reduceParams = parser.map();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token.isValue()) {
                if (!scriptParameterParser.token(currentFieldName, token, parser)) {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].");
            }
        }
        
        ScriptParameterValue initScriptValue = scriptParameterParser.getScriptParameterValue(INIT_SCRIPT);
        String initScript = null;
        ScriptType initScriptType = null;
        if (initScriptValue != null) {
            initScript = initScriptValue.script();
            initScriptType = initScriptValue.scriptType();
        }
        ScriptParameterValue mapScriptValue = scriptParameterParser.getScriptParameterValue(MAP_SCRIPT);
        String mapScript = null;
        ScriptType mapScriptType = null;
        if (mapScriptValue != null) {
            mapScript = mapScriptValue.script();
            mapScriptType = mapScriptValue.scriptType();
        }
        ScriptParameterValue combineScriptValue = scriptParameterParser.getScriptParameterValue(COMBINE_SCRIPT);
        String combineScript = null;
        ScriptType combineScriptType = null;
        if (combineScriptValue != null) {
            combineScript = combineScriptValue.script();
            combineScriptType = combineScriptValue.scriptType();
        }
        ScriptParameterValue reduceScriptValue = scriptParameterParser.getScriptParameterValue(REDUCE_SCRIPT);
        String reduceScript = null;
        ScriptType reduceScriptType = null;
        if (reduceScriptValue != null) {
            reduceScript = reduceScriptValue.script();
            reduceScriptType = reduceScriptValue.scriptType();
        }
        scriptLang = scriptParameterParser.lang();
        
        if (mapScript == null) {
            throw new SearchParseException(context, "map_script field is required in [" + aggregationName + "].");
        }
        return new ScriptedMetricAggregator.Factory(aggregationName, scriptLang, initScriptType, initScript, mapScriptType, mapScript,
                combineScriptType, combineScript, reduceScriptType, reduceScript, params, reduceParams);
    }

}
