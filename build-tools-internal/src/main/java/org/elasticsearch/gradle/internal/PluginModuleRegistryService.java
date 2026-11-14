/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import org.gradle.api.GradleException;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Shared build service for plugin metadata used by REST-test dependency wiring.
 * <p>
 * Producers register their plugin metadata during configuration ({@code afterEvaluate}), so consumers can resolve
 * module/plugin relationships without cross-project reads (required for isolated projects).
 * <p>
 * The mutable maps in this service are configuration-time-only state. On a configuration-cache hit the service
 * would be empty, which is currently safe because the esplugin/rest-test build graph is not configuration-cache
 * compatible yet.
 */
public abstract class PluginModuleRegistryService implements BuildService<BuildServiceParameters.None> {

    public static final String NAME = "elasticsearchPluginModules";

    private final ConcurrentMap<String, ModuleSnapshot> modulesByPath = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Set<String>> modulePathsByName = new ConcurrentHashMap<>();

    /**
     * Registers plugin metadata for one esplugin project.
     */
    public void registerModule(String projectPath, String pluginName, List<String> extendedPlugins, boolean isModule) {
        Objects.requireNonNull(projectPath, "projectPath");
        Objects.requireNonNull(pluginName, "pluginName");
        Objects.requireNonNull(extendedPlugins, "extendedPlugins");

        ModuleSnapshot replacement = new ModuleSnapshot(pluginName, List.copyOf(extendedPlugins), isModule);
        ModuleSnapshot previous = modulesByPath.put(projectPath, replacement);

        if (previous != null && previous.isModule()) {
            removeNameMapping(previous.pluginName(), projectPath);
        }
        if (isModule) {
            modulePathsByName.computeIfAbsent(pluginName, ignored -> ConcurrentHashMap.newKeySet()).add(projectPath);
        }
    }

    /**
     * Resolves the unique module project path for a plugin name.
     */
    public Optional<String> modulePath(String pluginName) {
        Objects.requireNonNull(pluginName, "pluginName");

        Set<String> paths = modulePathsByName.get(pluginName);
        if (paths == null || paths.isEmpty()) {
            return Optional.empty();
        }

        List<String> matchingPaths = new ArrayList<>(paths);
        matchingPaths.sort(String::compareTo);
        if (matchingPaths.size() > 1) {
            throw new GradleException(
                "Ambiguous module plugin name ["
                    + pluginName
                    + "] is registered by projects "
                    + matchingPaths
                    + ". Module plugin names must be unique."
            );
        }

        return Optional.of(matchingPaths.get(0));
    }

    /**
     * Returns extended-plugin declarations for the registered project path (module or non-module).
     */
    public List<String> extendedPlugins(String projectPath) {
        ModuleSnapshot moduleSnapshot = modulesByPath.get(projectPath);
        if (moduleSnapshot == null) {
            return List.of();
        }
        return moduleSnapshot.extendedPlugins();
    }

    /**
     * Returns whether any plugin metadata was registered for the project path (module or non-module plugin).
     */
    public boolean isRegistered(String projectPath) {
        return modulesByPath.containsKey(projectPath);
    }

    private void removeNameMapping(String pluginName, String projectPath) {
        modulePathsByName.computeIfPresent(pluginName, (ignored, paths) -> {
            paths.remove(projectPath);
            return paths.isEmpty() ? null : paths;
        });
    }

    private record ModuleSnapshot(String pluginName, List<String> extendedPlugins, boolean isModule) {}
}
