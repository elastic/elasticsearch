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
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

public class ScriptedMetricParser implements Aggregator.Parser {

    public static final ParseField PARAMS_FIELD = new ParseField("params");
    public static final ParseField REDUCE_PARAMS_FIELD = new ParseField("reduce_params");
    public static final ParseField INIT_SCRIPT_FIELD = new ParseField("init_script");
    public static final ParseField MAP_SCRIPT_FIELD = new ParseField("map_script");
    public static final ParseField COMBINE_SCRIPT_FIELD = new ParseField("combine_script");
    public static final ParseField REDUCE_SCRIPT_FIELD = new ParseField("reduce_script");
    public static final ParseField INIT_SCRIPT_FILE_FIELD = new ParseField("init_script_file");
    public static final ParseField MAP_SCRIPT_FILE_FIELD = new ParseField("map_script_file");
    public static final ParseField COMBINE_SCRIPT_FILE_FIELD = new ParseField("combine_script_file");
    public static final ParseField REDUCE_SCRIPT_FILE_FIELD = new ParseField("reduce_script_file");
    public static final ParseField INIT_SCRIPT_ID_FIELD = new ParseField("init_script_id");
    public static final ParseField MAP_SCRIPT_ID_FIELD = new ParseField("map_script_id");
    public static final ParseField COMBINE_SCRIPT_ID_FIELD = new ParseField("combine_script_id");
    public static final ParseField REDUCE_SCRIPT_ID_FIELD = new ParseField("reduce_script_id");
    public static final ParseField LANG_FIELD = new ParseField("lang");

    @Override
    public String type() {
        return InternalScriptedMetric.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {
        String initScript = null;
        String mapScript = null;
        String combineScript = null;
        String reduceScript = null;
        String scriptLang = null;
        ScriptType initScriptType = ScriptType.INLINE;
        ScriptType mapScriptType = ScriptType.INLINE;
        ScriptType combineScriptType = ScriptType.INLINE;
        ScriptType reduceScriptType = ScriptType.INLINE;
        Map<String, Object> params = null;
        Map<String, Object> reduceParams = null;
        XContentParser.Token token;
        String currentFieldName = null;

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
                if (INIT_SCRIPT_FIELD.match(currentFieldName)) {
                    if (initScript != null) {
                        throw new SearchParseException(context, "Only one of [init_script, init_script_file, init_script_id] is allowed in [" + aggregationName + "].");
                    }
                    initScript = parser.text();
                    initScriptType = ScriptType.INLINE;
                } else if (MAP_SCRIPT_FIELD.match(currentFieldName)) {
                    if (mapScript != null) {
                        throw new SearchParseException(context, "Only one of [map_script, map_script_file, map_script_id] is allowed in [" + aggregationName + "].");
                    }
                    mapScript = parser.text();
                    mapScriptType = ScriptType.INLINE;
                } else if (COMBINE_SCRIPT_FIELD.match(currentFieldName)) {
                    if (combineScript != null) {
                        throw new SearchParseException(context, "Only one of [combine_script, combine_script_file, combine_script_id] is allowed in [" + aggregationName + "].");
                    }
                    combineScript = parser.text();
                    combineScriptType = ScriptType.INLINE;
                } else if (REDUCE_SCRIPT_FIELD.match(currentFieldName)) {
                    if (reduceScript != null) {
                        throw new SearchParseException(context, "Only one of [reduce_script, reduce_script_file, reduce_script_id] is allowed in [" + aggregationName + "].");
                    }
                    reduceScript = parser.text();
                    reduceScriptType = ScriptType.INLINE;
                } else if (INIT_SCRIPT_FILE_FIELD.match(currentFieldName)) {
                    if (initScript != null) {
                        throw new SearchParseException(context, "Only one of [init_script, init_script_file, init_script_id] is allowed in [" + aggregationName + "].");
                    }
                    initScript = parser.text();
                    initScriptType = ScriptType.FILE;
                } else if (MAP_SCRIPT_FILE_FIELD.match(currentFieldName)) {
                    if (mapScript != null) {
                        throw new SearchParseException(context, "Only one of [map_script, map_script_file, map_script_id] is allowed in [" + aggregationName + "].");
                    }
                    mapScript = parser.text();
                    mapScriptType = ScriptType.FILE;
                } else if (COMBINE_SCRIPT_FILE_FIELD.match(currentFieldName)) {
                    if (combineScript != null) {
                        throw new SearchParseException(context, "Only one of [combine_script, combine_script_file, combine_script_id] is allowed in [" + aggregationName + "].");
                    }
                    combineScript = parser.text();
                    combineScriptType = ScriptType.FILE;
                } else if (REDUCE_SCRIPT_FILE_FIELD.match(currentFieldName)) {
                    if (reduceScript != null) {
                        throw new SearchParseException(context, "Only one of [reduce_script, reduce_script_file, reduce_script_id] is allowed in [" + aggregationName + "].");
                    }
                    reduceScript = parser.text();
                    reduceScriptType = ScriptType.FILE;
                } else if (INIT_SCRIPT_ID_FIELD.match(currentFieldName)) {
                    if (initScript != null) {
                        throw new SearchParseException(context, "Only one of [init_script, init_script_file, init_script_id] is allowed in [" + aggregationName + "].");
                    }
                    initScript = parser.text();
                    initScriptType = ScriptType.INDEXED;
                } else if (MAP_SCRIPT_ID_FIELD.match(currentFieldName)) {
                    if (mapScript != null) {
                        throw new SearchParseException(context, "Only one of [map_script, map_script_file, map_script_id] is allowed in [" + aggregationName + "].");
                    }
                    mapScript = parser.text();
                    mapScriptType = ScriptType.INDEXED;
                } else if (COMBINE_SCRIPT_ID_FIELD.match(currentFieldName)) {
                    if (combineScript != null) {
                        throw new SearchParseException(context, "Only one of [combine_script, combine_script_file, combine_script_id] is allowed in [" + aggregationName + "].");
                    }
                    combineScript = parser.text();
                    combineScriptType = ScriptType.INDEXED;
                } else if (REDUCE_SCRIPT_ID_FIELD.match(currentFieldName)) {
                    if (reduceScript != null) {
                        throw new SearchParseException(context, "Only one of [reduce_script, reduce_script_file, reduce_script_id] is allowed in [" + aggregationName + "].");
                    }
                    reduceScript = parser.text();
                    reduceScriptType = ScriptType.INDEXED;
                } else if (LANG_FIELD.match(currentFieldName)) {
                    scriptLang = parser.text();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].");
            }
        }
        if (mapScript == null) {
            throw new SearchParseException(context, "map_script field is required in [" + aggregationName + "].");
        }
        return new ScriptedMetricAggregator.Factory(aggregationName, scriptLang, initScriptType, initScript, mapScriptType, mapScript,
                combineScriptType, combineScript, reduceScriptType, reduceScript, params, reduceParams);
    }

}
