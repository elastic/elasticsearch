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
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptParameterParser;
import org.elasticsearch.script.ScriptParameterParser.ScriptParameterValue;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ScriptedMetricAggregationBuilder extends AbstractAggregationBuilder<ScriptedMetricAggregationBuilder> {

    public static final String NAME = InternalScriptedMetric.TYPE.name();
    public static final ParseField AGGREGATION_NAME_FIELD = new ParseField(NAME);

    private static final ParseField INIT_SCRIPT_FIELD = new ParseField("init_script");
    private static final ParseField MAP_SCRIPT_FIELD = new ParseField("map_script");
    private static final ParseField COMBINE_SCRIPT_FIELD = new ParseField("combine_script");
    private static final ParseField REDUCE_SCRIPT_FIELD = new ParseField("reduce_script");
    private static final ParseField PARAMS_FIELD = new ParseField("params");
    private static final ParseField REDUCE_PARAMS_FIELD = new ParseField("reduce_params");

    private Script initScript;
    private Script mapScript;
    private Script combineScript;
    private Script reduceScript;
    private Map<String, Object> params;

    public ScriptedMetricAggregationBuilder(String name) {
        super(name, InternalScriptedMetric.TYPE);
    }

    /**
     * Read from a stream.
     */
    public ScriptedMetricAggregationBuilder(StreamInput in) throws IOException {
        super(in, InternalScriptedMetric.TYPE);
        initScript = in.readOptionalWriteable(Script::new);
        mapScript = in.readOptionalWriteable(Script::new);
        combineScript = in.readOptionalWriteable(Script::new);
        reduceScript = in.readOptionalWriteable(Script::new);
        if (in.readBoolean()) {
            params = in.readMap();
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(initScript);
        out.writeOptionalWriteable(mapScript);
        out.writeOptionalWriteable(combineScript);
        out.writeOptionalWriteable(reduceScript);
        boolean hasParams = params != null;
        out.writeBoolean(hasParams);
        if (hasParams) {
            out.writeMap(params);
        }
    }

    /**
     * Set the <tt>init</tt> script.
     */
    public ScriptedMetricAggregationBuilder initScript(Script initScript) {
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
    public ScriptedMetricAggregationBuilder mapScript(Script mapScript) {
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
    public ScriptedMetricAggregationBuilder combineScript(Script combineScript) {
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
    public ScriptedMetricAggregationBuilder reduceScript(Script reduceScript) {
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
    public ScriptedMetricAggregationBuilder params(Map<String, Object> params) {
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
            builder.field(INIT_SCRIPT_FIELD.getPreferredName(), initScript);
        }

        if (mapScript != null) {
            builder.field(MAP_SCRIPT_FIELD.getPreferredName(), mapScript);
        }

        if (combineScript != null) {
            builder.field(COMBINE_SCRIPT_FIELD.getPreferredName(), combineScript);
        }

        if (reduceScript != null) {
            builder.field(REDUCE_SCRIPT_FIELD.getPreferredName(), reduceScript);
        }
        if (params != null) {
            builder.field(PARAMS_FIELD.getPreferredName());
            builder.map(params);
        }
        builder.endObject();
        return builder;
    }

    public static ScriptedMetricAggregationBuilder parse(String aggregationName, QueryParseContext context) throws IOException {
        Script initScript = null;
        Script mapScript = null;
        Script combineScript = null;
        Script reduceScript = null;
        Map<String, Object> params = null;
        Map<String, Object> reduceParams = null;
        XContentParser.Token token;
        String currentFieldName = null;
        Set<String> scriptParameters = new HashSet<>();
        scriptParameters.add(INIT_SCRIPT_FIELD.getPreferredName());
        scriptParameters.add(MAP_SCRIPT_FIELD.getPreferredName());
        scriptParameters.add(COMBINE_SCRIPT_FIELD.getPreferredName());
        scriptParameters.add(REDUCE_SCRIPT_FIELD.getPreferredName());
        ScriptParameterParser scriptParameterParser = new ScriptParameterParser(scriptParameters);

        XContentParser parser = context.parser();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (context.getParseFieldMatcher().match(currentFieldName, INIT_SCRIPT_FIELD)) {
                    initScript = Script.parse(parser, context.getParseFieldMatcher());
                } else if (context.getParseFieldMatcher().match(currentFieldName, MAP_SCRIPT_FIELD)) {
                    mapScript = Script.parse(parser, context.getParseFieldMatcher());
                } else if (context.getParseFieldMatcher().match(currentFieldName, COMBINE_SCRIPT_FIELD)) {
                    combineScript = Script.parse(parser, context.getParseFieldMatcher());
                } else if (context.getParseFieldMatcher().match(currentFieldName, REDUCE_SCRIPT_FIELD)) {
                    reduceScript = Script.parse(parser, context.getParseFieldMatcher());
                } else if (context.getParseFieldMatcher().match(currentFieldName, PARAMS_FIELD)) {
                    params = parser.map();
                } else if (context.getParseFieldMatcher().match(currentFieldName, REDUCE_PARAMS_FIELD)) {
                  reduceParams = parser.map();
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token.isValue()) {
                if (!scriptParameterParser.token(currentFieldName, token, parser, context.getParseFieldMatcher())) {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Unexpected token " + token + " in [" + aggregationName + "].");
            }
        }

        if (initScript == null) { // Didn't find anything using the new API so try using the old one instead
            ScriptParameterValue scriptValue = scriptParameterParser.getScriptParameterValue(INIT_SCRIPT_FIELD.getPreferredName());
            if (scriptValue != null) {
                initScript = new Script(scriptValue.script(), scriptValue.scriptType(), scriptParameterParser.lang(), params);
            }
        } else if (initScript.getParams() != null) {
            throw new ParsingException(parser.getTokenLocation(), "init_script params are not supported. Parameters for the "
                    + "init_script must be specified in the params field on the scripted_metric aggregator not inside the init_script "
                    + "object");
        }

        if (mapScript == null) { // Didn't find anything using the new API so try using the old one instead
            ScriptParameterValue scriptValue = scriptParameterParser.getScriptParameterValue(MAP_SCRIPT_FIELD.getPreferredName());
            if (scriptValue != null) {
                mapScript = new Script(scriptValue.script(), scriptValue.scriptType(), scriptParameterParser.lang(), params);
            }
        } else if (mapScript.getParams() != null) {
            throw new ParsingException(parser.getTokenLocation(), "map_script params are not supported. Parameters for the map_script "
                    + "must be specified in the params field on the scripted_metric aggregator not inside the map_script object");
        }

        if (combineScript == null) { // Didn't find anything using the new API so try using the old one instead
            ScriptParameterValue scriptValue = scriptParameterParser.getScriptParameterValue(COMBINE_SCRIPT_FIELD.getPreferredName());
            if (scriptValue != null) {
                combineScript = new Script(scriptValue.script(), scriptValue.scriptType(), scriptParameterParser.lang(), params);
            }
        } else if (combineScript.getParams() != null) {
            throw new ParsingException(parser.getTokenLocation(),
                    "combine_script params are not supported. Parameters for the "
                            + "combine_script must be specified in the params field on the scripted_metric aggregator not inside the "
                            + "combine_script object");
        }

        if (reduceScript == null) { // Didn't find anything using the new API so try using the old one instead
            ScriptParameterValue scriptValue = scriptParameterParser.getScriptParameterValue(REDUCE_SCRIPT_FIELD.getPreferredName());
            if (scriptValue != null) {
                reduceScript = new Script(scriptValue.script(), scriptValue.scriptType(), scriptParameterParser.lang(), reduceParams);
            }
        }

        if (mapScript == null) {
            throw new ParsingException(parser.getTokenLocation(), "map_script field is required in [" + aggregationName + "].");
        }

        ScriptedMetricAggregationBuilder factory = new ScriptedMetricAggregationBuilder(aggregationName);
        if (initScript != null) {
            factory.initScript(initScript);
        }
        if (mapScript != null) {
            factory.mapScript(mapScript);
        }
        if (combineScript != null) {
            factory.combineScript(combineScript);
        }
        if (reduceScript != null) {
            factory.reduceScript(reduceScript);
        }
        if (params != null) {
            factory.params(params);
        }
        return factory;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(initScript, mapScript, combineScript, reduceScript, params);
    }

    @Override
    protected boolean doEquals(Object obj) {
        ScriptedMetricAggregationBuilder other = (ScriptedMetricAggregationBuilder) obj;
        return Objects.equals(initScript, other.initScript)
                && Objects.equals(mapScript, other.mapScript)
                && Objects.equals(combineScript, other.combineScript)
                && Objects.equals(reduceScript, other.reduceScript)
                && Objects.equals(params, other.params);
    }

}
