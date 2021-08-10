/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;


import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.painless.action.PainlessContextAction;
import org.elasticsearch.painless.action.PainlessExecuteAction;
import org.elasticsearch.painless.spi.PainlessExtension;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.painless.spi.annotation.WhitelistAnnotationParser;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.script.FieldScript;
import org.elasticsearch.script.FilterScript;
import org.elasticsearch.script.IngestScript;
import org.elasticsearch.script.NumberSortScript;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptedMetricAggContexts;
import org.elasticsearch.script.StringSortScript;
import org.elasticsearch.search.aggregations.pipeline.MovingFunctionScript;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Registers Painless as a plugin.
 */
public final class PainlessPlugin extends Plugin implements ScriptPlugin, ExtensiblePlugin, ActionPlugin {

    private static final Map<ScriptContext<?>, List<Whitelist>> whitelists;
    private static final String[] BASE_WHITELIST_FILES = new String[] {
        "org.elasticsearch.txt",
        "org.elasticsearch.net.txt",
        "org.elasticsearch.script.fields.txt",
        "java.lang.txt",
        "java.math.txt",
        "java.text.txt",
        "java.time.txt",
        "java.time.chrono.txt",
        "java.time.format.txt",
        "java.time.temporal.txt",
        "java.time.zone.txt",
        "java.util.txt",
        "java.util.function.txt",
        "java.util.regex.txt",
        "java.util.stream.txt"
    };
    public static final List<Whitelist> BASE_WHITELISTS =
            Collections.singletonList(WhitelistLoader.loadFromResourceFiles(
                    PainlessPlugin.class, WhitelistAnnotationParser.BASE_ANNOTATION_PARSERS, BASE_WHITELIST_FILES));

    /*
     * Contexts from Core that need custom whitelists can add them to the map below.
     * Whitelist resources should be added as appropriately named, separate files
     * under Painless' resources
     */
    static {
        Map<ScriptContext<?>, List<Whitelist>> map = new HashMap<>();

        // Moving Function Pipeline Agg
        List<Whitelist> movFn = new ArrayList<>();
        Whitelist movFnWhitelist = WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.aggs.movfn.txt");
        movFn.add(movFnWhitelist);
        map.put(MovingFunctionScript.CONTEXT, movFn);

        // Functions used for scoring docs
        List<Whitelist> scoreFn = new ArrayList<>();
        Whitelist scoreFnWhitelist = WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.score.txt");
        Whitelist scoreFieldWhitelist =
            WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.script.fields.score.txt");
        scoreFn.add(scoreFnWhitelist);
        scoreFn.add(scoreFieldWhitelist);
        map.put(ScoreScript.CONTEXT, scoreFn);

        // Functions available to ingest pipelines
        List<Whitelist> ingest = new ArrayList<>();
        Whitelist ingestWhitelist = WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.ingest.txt");
        ingest.add(ingestWhitelist);
        map.put(IngestScript.CONTEXT, ingest);

        // Functions available to runtime fields

        for (ScriptContext<?> scriptContext : ScriptModule.RUNTIME_FIELDS_CONTEXTS) {
            map.put(scriptContext, getRuntimeFieldWhitelist(scriptContext.name));
        }

        List<Whitelist> numSort = new ArrayList<>();
        Whitelist numSortField =
            WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.script.fields.numbersort.txt");
        numSort.add(numSortField);
        map.put(NumberSortScript.CONTEXT, numSort);

        List<Whitelist> strSort = new ArrayList<>();
        Whitelist strSortField =
            WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.script.fields.stringsort.txt");
        strSort.add(strSortField);
        map.put(StringSortScript.CONTEXT, strSort);

        List<Whitelist> filter = new ArrayList<>();
        Whitelist filterWhitelist =
            WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.script.fields.filter.txt");
        filter.add(filterWhitelist);
        map.put(FilterScript.CONTEXT, filter);

        List<Whitelist> aggregation = new ArrayList<>();
        Whitelist aggregationWhitelist =
                WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.script.fields.aggregation.txt");
        aggregation.add(aggregationWhitelist);
        map.put(AggregationScript.CONTEXT, aggregation);

        List<Whitelist> aggmap = new ArrayList<>();
        Whitelist aggmapWhitelist =
                WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.script.fields.aggmap.txt");
        aggmap.add(aggmapWhitelist);
        map.put(ScriptedMetricAggContexts.MapScript.CONTEXT, aggmap);

        List<Whitelist> field = new ArrayList<>();
        Whitelist fieldWhitelist =
                WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.script.fields.field.txt");
        field.add(fieldWhitelist);
        map.put(FieldScript.CONTEXT, field);

        // Execute context gets everything
        List<Whitelist> test = new ArrayList<>();
        test.add(movFnWhitelist);
        test.add(scoreFnWhitelist);
        test.add(ingestWhitelist);
        test.add(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.json.txt"));
        map.put(PainlessExecuteAction.PainlessTestScript.CONTEXT, test);

        whitelists = map;
    }

    private static List<Whitelist> getRuntimeFieldWhitelist(String contextName) {
        List<Whitelist> scriptField = new ArrayList<>();
        Whitelist whitelist = WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class,
            "org.elasticsearch.script." + contextName + ".txt");
        scriptField.add(whitelist);
        return scriptField;
    }

    private final SetOnce<PainlessScriptEngine> painlessScriptEngine = new SetOnce<>();

    @Override
    public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
        Map<ScriptContext<?>, List<Whitelist>> contextsWithWhitelists = new HashMap<>();
        for (ScriptContext<?> context : contexts) {
            // we might have a context that only uses the base whitelists, so would not have been filled in by reloadSPI
            List<Whitelist> contextWhitelists = whitelists.get(context);
            if (contextWhitelists == null) {
                contextWhitelists = new ArrayList<>(BASE_WHITELISTS);
            } else {
                contextWhitelists.addAll(BASE_WHITELISTS);
            }
            contextsWithWhitelists.put(context, contextWhitelists);
        }
        painlessScriptEngine.set(new PainlessScriptEngine(settings, contextsWithWhitelists));
        return painlessScriptEngine.get();
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry,
                                               IndexNameExpressionResolver expressionResolver,
                                               Supplier<RepositoriesService> repositoriesServiceSupplier) {
        // this is a hack to bind the painless script engine in guice (all components are added to guice), so that
        // the painless context api. this is a temporary measure until transport actions do no require guice
        return Collections.singletonList(painlessScriptEngine.get());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(CompilerSettings.REGEX_ENABLED, CompilerSettings.REGEX_LIMIT_FACTOR);
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        loader.loadExtensions(PainlessExtension.class).stream()
            .flatMap(extension -> extension.getContextWhitelists().entrySet().stream())
            .forEach(entry -> {
                List<Whitelist> existing = whitelists.computeIfAbsent(entry.getKey(),
                    c -> new ArrayList<>(BASE_WHITELISTS));
                existing.addAll(entry.getValue());
            });
    }

    @Override
    public List<ScriptContext<?>> getContexts() {
        return Collections.singletonList(PainlessExecuteAction.PainlessTestScript.CONTEXT);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = new ArrayList<>();
        actions.add(new ActionHandler<>(PainlessExecuteAction.INSTANCE, PainlessExecuteAction.TransportAction.class));
        actions.add(new ActionHandler<>(PainlessContextAction.INSTANCE, PainlessContextAction.TransportAction.class));
        return actions;
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        List<RestHandler> handlers = new ArrayList<>();
        handlers.add(new PainlessExecuteAction.RestAction());
        handlers.add(new PainlessContextAction.RestAction());
        return handlers;
    }
}
