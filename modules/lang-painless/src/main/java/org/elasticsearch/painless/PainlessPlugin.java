/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.painless.action.PainlessContextAction;
import org.elasticsearch.painless.action.PainlessExecuteAction;
import org.elasticsearch.painless.spi.PainlessExtension;
import org.elasticsearch.painless.spi.PainlessTestScript;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.painless.spi.annotation.WhitelistAnnotationParser;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Registers Painless as a plugin.
 */
public final class PainlessPlugin extends Plugin implements ScriptPlugin, ExtensiblePlugin, ActionPlugin {

    private volatile Map<ScriptContext<?>, List<Whitelist>> whitelists;

    private final SetOnce<PainlessScriptEngine> painlessScriptEngine = new SetOnce<>();

    public static List<Whitelist> baseWhiteList() {
        return List.of(
            WhitelistLoader.loadFromResourceFiles(
                PainlessPlugin.class,
                WhitelistAnnotationParser.BASE_ANNOTATION_PARSERS,
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
                "java.util.stream.txt",
                "java.nio.txt"
            )
        );
    }

    @Override
    public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
        final var wl = whitelists;
        whitelists = null;
        assert wl != null;
        Map<ScriptContext<?>, List<Whitelist>> contextsWithWhitelists = new HashMap<>();
        final List<Whitelist> baseWhiteList = baseWhiteList();
        for (ScriptContext<?> context : contexts) {
            // we might have a context that only uses the base whitelists, so would not have been filled in by reloadSPI
            List<Whitelist> contextWhitelists = wl.get(context);
            final List<Whitelist> mergedWhitelists;
            if (contextWhitelists != null && contextWhitelists.isEmpty() == false) {
                mergedWhitelists = CollectionUtils.concatLists(baseWhiteList, contextWhitelists);
            } else {
                mergedWhitelists = baseWhiteList;
            }
            contextsWithWhitelists.put(context, mergedWhitelists);
        }
        painlessScriptEngine.set(new PainlessScriptEngine(settings, contextsWithWhitelists));
        return painlessScriptEngine.get();
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
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
        final Map<ScriptContext<?>, List<Whitelist>> whitelistsBuilder = new HashMap<>();
        /*
         * Contexts from Core that need custom whitelists can add them to the map below.
         * Whitelist resources should be added as appropriately named, separate files
         * under Painless' resources
         */
        for (ScriptContext<?> context : ScriptModule.CORE_CONTEXTS.values()) {
            List<Whitelist> contextWhitelists = new ArrayList<>();
            if (PainlessPlugin.class.getResource("org.elasticsearch.script." + context.name.replace('-', '_') + ".txt") != null) {
                contextWhitelists.add(
                    WhitelistLoader.loadFromResourceFiles(
                        PainlessPlugin.class,
                        "org.elasticsearch.script." + context.name.replace('-', '_') + ".txt"
                    )
                );
            }

            whitelistsBuilder.put(context, contextWhitelists);
        }

        List<Whitelist> testWhitelists = new ArrayList<>();
        for (ScriptContext<?> context : ScriptModule.CORE_CONTEXTS.values()) {
            if (ScriptModule.RUNTIME_FIELDS_CONTEXTS.contains(context) == false) {
                testWhitelists.addAll(whitelistsBuilder.get(context));
            }
        }
        testWhitelists.add(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.json.txt"));
        whitelistsBuilder.put(PainlessTestScript.CONTEXT, testWhitelists);
        loader.loadExtensions(PainlessExtension.class)
            .stream()
            .flatMap(extension -> extension.getContextWhitelists().entrySet().stream())
            .forEach(entry -> {
                List<Whitelist> existing = whitelistsBuilder.computeIfAbsent(entry.getKey(), c -> new ArrayList<>());
                existing.addAll(entry.getValue());
            });

        this.whitelists = whitelistsBuilder;
    }

    @Override
    public List<ScriptContext<?>> getContexts() {
        return Collections.singletonList(PainlessTestScript.CONTEXT);
    }

    @Override
    public List<ActionHandler> getActions() {
        List<ActionHandler> actions = new ArrayList<>();
        actions.add(new ActionHandler(PainlessExecuteAction.INSTANCE, PainlessExecuteAction.TransportAction.class));
        actions.add(new ActionHandler(PainlessContextAction.INSTANCE, PainlessContextAction.TransportAction.class));
        return actions;
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        NamedWriteableRegistry namedWriteableRegistry,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        List<RestHandler> handlers = new ArrayList<>();
        handlers.add(new PainlessExecuteAction.RestAction());
        handlers.add(new PainlessContextAction.RestAction());
        return handlers;
    }
}
