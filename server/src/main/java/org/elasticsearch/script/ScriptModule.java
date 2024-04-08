/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.IntervalFilterScript;
import org.elasticsearch.plugins.ScriptPlugin;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Manages building {@link ScriptService}.
 */
public class ScriptModule {

    public static final Set<ScriptContext<?>> RUNTIME_FIELDS_CONTEXTS = Set.of(
        BooleanFieldScript.CONTEXT,
        DateFieldScript.CONTEXT,
        DoubleFieldScript.CONTEXT,
        LongFieldScript.CONTEXT,
        StringFieldScript.CONTEXT,
        GeoPointFieldScript.CONTEXT,
        GeometryFieldScript.CONTEXT,
        IpFieldScript.CONTEXT,
        CompositeFieldScript.CONTEXT
    );

    public static final Map<String, ScriptContext<?>> CORE_CONTEXTS;
    static {
        CORE_CONTEXTS = Stream.concat(
            Stream.of(
                FieldScript.CONTEXT,
                AggregationScript.CONTEXT,
                ScoreScript.CONTEXT,
                NumberSortScript.CONTEXT,
                StringSortScript.CONTEXT,
                BytesRefSortScript.CONTEXT,
                TermsSetQueryScript.CONTEXT,
                UpdateScript.CONTEXT,
                ReindexScript.CONTEXT,
                UpdateByQueryScript.CONTEXT,
                BucketAggregationScript.CONTEXT,
                BucketAggregationSelectorScript.CONTEXT,
                SignificantTermsHeuristicScoreScript.CONTEXT,
                IngestScript.CONTEXT,
                IngestConditionalScript.CONTEXT,
                FilterScript.CONTEXT,
                SimilarityScript.CONTEXT,
                SimilarityWeightScript.CONTEXT,
                TemplateScript.CONTEXT,
                TemplateScript.INGEST_CONTEXT,
                ScriptedMetricAggContexts.InitScript.CONTEXT,
                ScriptedMetricAggContexts.MapScript.CONTEXT,
                ScriptedMetricAggContexts.CombineScript.CONTEXT,
                ScriptedMetricAggContexts.ReduceScript.CONTEXT,
                IntervalFilterScript.CONTEXT,
                DoubleValuesScript.CONTEXT
            ),
            RUNTIME_FIELDS_CONTEXTS.stream()
        ).collect(Collectors.toMap(c -> c.name, Function.identity()));
    }

    public final Map<String, ScriptEngine> engines;
    public final Map<String, ScriptContext<?>> contexts;

    public ScriptModule(Settings settings, List<ScriptPlugin> scriptPlugins) {
        Map<String, ScriptEngine> engines = new HashMap<>();
        Map<String, ScriptContext<?>> contexts = new HashMap<>(CORE_CONTEXTS);
        for (ScriptPlugin plugin : scriptPlugins) {
            for (ScriptContext<?> context : plugin.getContexts()) {
                ScriptContext<?> oldContext = contexts.put(context.name, context);
                if (oldContext != null) {
                    throw new IllegalArgumentException("Context name [" + context.name + "] defined twice");
                }
            }
        }
        for (ScriptPlugin plugin : scriptPlugins) {
            ScriptEngine engine = plugin.getScriptEngine(settings, contexts.values());
            if (engine != null) {
                ScriptEngine existing = engines.put(engine.getType(), engine);
                if (existing != null) {
                    throw new IllegalArgumentException(
                        "scripting language ["
                            + engine.getType()
                            + "] defined for engine ["
                            + existing.getClass().getName()
                            + "] and ["
                            + engine.getClass().getName()
                    );
                }
            }
        }
        this.engines = Collections.unmodifiableMap(engines);
        this.contexts = Collections.unmodifiableMap(contexts);
    }

    /**
     * Allow the script service to register any settings update handlers on the cluster settings
     */
    public static void registerClusterSettingsListeners(ScriptService scriptService, ClusterSettings clusterSettings) {
        scriptService.registerClusterSettingsListeners(clusterSettings);
    }
}
