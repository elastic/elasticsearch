/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptedMetricAggContexts;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class ScriptedMetricAggregationBuilder extends AbstractAggregationBuilder<ScriptedMetricAggregationBuilder> {
    public static final String NAME = "scripted_metric";

    private static final ParseField INIT_SCRIPT_FIELD = new ParseField("init_script");
    private static final ParseField MAP_SCRIPT_FIELD = new ParseField("map_script");
    private static final ParseField COMBINE_SCRIPT_FIELD = new ParseField("combine_script");
    private static final ParseField REDUCE_SCRIPT_FIELD = new ParseField("reduce_script");
    private static final ParseField PARAMS_FIELD = new ParseField("params");

    private static void validateScript(String scriptName, String aggName, Script script, List<String> allowedScripts) {
        if (script == null) {
            throw new IllegalArgumentException("[" + scriptName + "] must not be null: [" + aggName + "]");
        }
        if (allowedScripts.isEmpty() == false) {
            if (allowedScripts.contains(script.getIdOrCode()) == false) {
                throw new IllegalArgumentException("[" + scriptName + "] contains not allowed script: [" + aggName + "]");
            }
        }
    }

    private static void validateParams(Map<String, Object> params, String aggName) {
        if (params == null) {
            throw new IllegalArgumentException("[params] must not be null: [" + aggName + "]");
        }
    }

    public static ConstructingObjectParser<ScriptedMetricAggregationBuilder, String> createParser(List<String> allowedScripts) {
        ConstructingObjectParser<ScriptedMetricAggregationBuilder, String> parser = new ConstructingObjectParser<>(
            NAME,
            false,
            (args, name) -> {
                Script script = (Script) args[0];
                validateScript(MAP_SCRIPT_FIELD.getPreferredName(), name, script, allowedScripts);
                ScriptedMetricAggregationBuilder builder = new ScriptedMetricAggregationBuilder(name);
                builder.mapScript(script);
                return builder;
            }
        );

        Script.declareScript(parser, (builder, script) -> {
            validateScript(INIT_SCRIPT_FIELD.getPreferredName(), builder.name, script, allowedScripts);
            builder.initScript(script);
        }, INIT_SCRIPT_FIELD);
        // TODO: Why do we use constructorArg() here, rather than being consistent with the other parsed fields?
        Script.declareScript(parser, constructorArg(), MAP_SCRIPT_FIELD);
        Script.declareScript(parser, (builder, script) -> {
            validateScript(COMBINE_SCRIPT_FIELD.getPreferredName(), builder.name, script, allowedScripts);
            builder.combineScript(script);
        }, COMBINE_SCRIPT_FIELD);
        Script.declareScript(parser, (builder, script) -> {
            validateScript(REDUCE_SCRIPT_FIELD.getPreferredName(), builder.name, script, allowedScripts);
            builder.reduceScript(script);
        }, REDUCE_SCRIPT_FIELD);
        parser.declareObject(ScriptedMetricAggregationBuilder::params, (p, name) -> {
            validateParams(p.map(), name);
            return p.map();
        }, PARAMS_FIELD);

        return parser;
    }

    private Script initScript;
    private Script mapScript;
    private Script combineScript;
    private Script reduceScript;
    private Map<String, Object> params;

    public ScriptedMetricAggregationBuilder(String name) {
        super(name);
    }

    protected ScriptedMetricAggregationBuilder(
        ScriptedMetricAggregationBuilder clone,
        Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.initScript = clone.initScript;
        this.mapScript = clone.mapScript;
        this.combineScript = clone.combineScript;
        this.reduceScript = clone.reduceScript;
        this.params = clone.params;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new ScriptedMetricAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public ScriptedMetricAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        initScript = in.readOptionalWriteable(Script::new);
        mapScript = in.readOptionalWriteable(Script::new);
        combineScript = in.readOptionalWriteable(Script::new);
        reduceScript = in.readOptionalWriteable(Script::new);
        if (in.readBoolean()) {
            params = in.readGenericMap();
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
            out.writeGenericMap(params);
        }
    }

    @Override
    public boolean supportsSampling() {
        return true;
    }

    /**
     * Set the {@code init} script.
     */
    public ScriptedMetricAggregationBuilder initScript(Script initScript) {
        this.initScript = initScript;
        return this;
    }

    /**
     * Set the {@code map} script.
     */
    public ScriptedMetricAggregationBuilder mapScript(Script mapScript) {
        this.mapScript = mapScript;
        return this;
    }

    /**
     * Set the {@code combine} script.
     */
    public ScriptedMetricAggregationBuilder combineScript(Script combineScript) {
        this.combineScript = combineScript;
        return this;
    }

    /**
     * Set the {@code reduce} script.
     */
    public ScriptedMetricAggregationBuilder reduceScript(Script reduceScript) {
        this.reduceScript = reduceScript;
        return this;
    }

    /**
     * Set parameters that will be available in the {@code init},
     * {@code map} and {@code combine} phases.
     */
    public ScriptedMetricAggregationBuilder params(Map<String, Object> params) {
        this.params = params;
        return this;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.NONE;
    }

    @Override
    protected ScriptedMetricAggregatorFactory doBuild(AggregationContext context, AggregatorFactory parent, Builder subfactoriesBuilder)
        throws IOException {

        if (combineScript == null) {
            throw new IllegalArgumentException("[combineScript] must not be null: [" + name + "]");
        }

        if (reduceScript == null) {
            throw new IllegalArgumentException("[reduceScript] must not be null: [" + name + "]");
        }

        // Extract params from scripts and pass them along to ScriptedMetricAggregatorFactory, since it won't have
        // access to them for the scripts it's given precompiled.

        ScriptedMetricAggContexts.InitScript.Factory compiledInitScript;
        Map<String, Object> initScriptParams;
        if (initScript != null) {
            compiledInitScript = context.compile(initScript, ScriptedMetricAggContexts.InitScript.CONTEXT);
            initScriptParams = initScript.getParams();
        } else {
            compiledInitScript = null;
            initScriptParams = Collections.emptyMap();
        }

        ScriptedMetricAggContexts.MapScript.Factory compiledMapScript = context.compile(
            mapScript,
            ScriptedMetricAggContexts.MapScript.CONTEXT
        );
        Map<String, Object> mapScriptParams = mapScript.getParams();

        ScriptedMetricAggContexts.CombineScript.Factory compiledCombineScript = context.compile(
            combineScript,
            ScriptedMetricAggContexts.CombineScript.CONTEXT
        );
        Map<String, Object> combineScriptParams = combineScript.getParams();

        return new ScriptedMetricAggregatorFactory(
            name,
            compiledMapScript,
            mapScriptParams,
            compiledInitScript,
            initScriptParams,
            compiledCombineScript,
            combineScriptParams,
            reduceScript,
            params,
            context,
            parent,
            subfactoriesBuilder,
            metadata
        );
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

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), initScript, mapScript, combineScript, reduceScript, params);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        ScriptedMetricAggregationBuilder other = (ScriptedMetricAggregationBuilder) obj;
        return Objects.equals(initScript, other.initScript)
            && Objects.equals(mapScript, other.mapScript)
            && Objects.equals(combineScript, other.combineScript)
            && Objects.equals(reduceScript, other.reduceScript)
            && Objects.equals(params, other.params);
    }
}
