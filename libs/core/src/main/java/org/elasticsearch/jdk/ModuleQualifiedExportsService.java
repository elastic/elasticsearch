/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.jdk;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.lang.module.ModuleDescriptor.Exports;
import java.lang.module.ModuleDescriptor.Opens;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An object that provides a callback for qualified exports and opens.
 *
 * Because Elasticsearch sometimes constructs module layers dynamically
 * (eg for plugins), qualified exports are silently dropped if a target
 * module does not yet exist. Modules that have qualified exports to
 * other dynamically created modules should implement this service so
 * that when a qualified export module is loaded, the exporting or
 * opening module can be informed and export or open dynamically to
 * the newly loaded module.
 */
public abstract class ModuleQualifiedExportsService {

    private static final Logger logger = LogManager.getLogger(ModuleQualifiedExportsService.class);

    // holds instances of ModuleQualfiedExportsService that exist in the boot layer
    private static class Holder {
        private static final Map<String, List<ModuleQualifiedExportsService>> exportsServices;

        static {
            Map<String, List<ModuleQualifiedExportsService>> qualifiedExports = new HashMap<>();
            var loader = ServiceLoader.load(ModuleQualifiedExportsService.class, ModuleQualifiedExportsService.class.getClassLoader());
            for (var exportsService : loader) {
                addExportsService(qualifiedExports, exportsService, exportsService.getClass().getModule().getName());
            }
            exportsServices = Map.copyOf(qualifiedExports);
        }
    }

    /**
     * A utility method to add an export service to the given map of exports services.
     *
     * The map is inverted, keyed by the target module name to which an exports/opens applies.
     *
     * @param qualifiedExports A map of modules to which qualfied exports need to be applied
     * @param exportsService The exports service to add to the map
     * @param moduleName The name of the module that is doing the exporting
     */
    public static void addExportsService(
        Map<String, List<ModuleQualifiedExportsService>> qualifiedExports,
        ModuleQualifiedExportsService exportsService,
        String moduleName
    ) {
        for (String targetName : exportsService.getTargets()) {
            logger.debug("Registered qualified export from module " + moduleName + " to " + targetName);
            qualifiedExports.computeIfAbsent(targetName, k -> new ArrayList<>()).add(exportsService);
        }
    }

    /**
     * Adds qualified exports and opens declared in other upstream modules to the target module.
     * This is required since qualified statements targeting yet-to-be-created modules, i.e. plugins,
     * are silently dropped when the boot layer is created.
     */
    public static void exposeQualifiedExportsAndOpens(Module target, Map<String, List<ModuleQualifiedExportsService>> qualifiedExports) {
        qualifiedExports.getOrDefault(target.getName(), List.of()).forEach(exportService -> exportService.addExportsAndOpens(target));
    }

    /**
     * Returns a mapping of ModuleQualifiedExportsServices that exist in the boot layer.
     */
    public static Map<String, List<ModuleQualifiedExportsService>> getBootServices() {
        return Holder.exportsServices;
    }

    protected final Module module;
    private final Map<String, List<String>> qualifiedExports;
    private final Map<String, List<String>> qualifiedOpens;
    private final Set<String> targets;

    protected ModuleQualifiedExportsService() {
        this(null);
    }

    protected ModuleQualifiedExportsService(Module module) {
        this.module = module == null ? getClass().getModule() : module;
        this.qualifiedExports = invert(this.module.getDescriptor().exports(), Exports::isQualified, Exports::source, Exports::targets);
        this.qualifiedOpens = invert(this.module.getDescriptor().opens(), Opens::isQualified, Opens::source, Opens::targets);
        this.targets = Stream.concat(qualifiedExports.keySet().stream(), qualifiedOpens.keySet().stream())
            .collect(Collectors.toUnmodifiableSet());
    }

    private static <T> Map<String, List<String>> invert(
        Collection<T> sourcesToTargets,
        Predicate<T> qualifiedPredicate,
        Function<T, String> sourceGetter,
        Function<T, Collection<String>> targetsGetter
    ) {
        Map<String, List<String>> targetsToSources = new HashMap<>();
        for (T sourceToTargets : sourcesToTargets) {
            if (qualifiedPredicate.test(sourceToTargets) == false) {
                continue; // not a qualified statement
            }
            String source = sourceGetter.apply(sourceToTargets);
            Collection<String> targets = targetsGetter.apply(sourceToTargets);
            for (String target : targets) {
                targetsToSources.computeIfAbsent(target, k -> new ArrayList<>()).add(source);
            }
        }
        targetsToSources.replaceAll((k, v) -> List.copyOf(v));
        return Map.copyOf(targetsToSources);
    }

    /**
     * Returns all qualified targets of the owning module.
     */
    public Set<String> getTargets() {
        return targets;
    }

    /**
     * Add exports and opens for a target module.
     * @param target A module whose name exists in {@link #getTargets()}
     */
    public void addExportsAndOpens(Module target) {
        String targetName = target.getName();
        if (targets.contains(targetName) == false) {
            throw new IllegalArgumentException(
                "Module " + module.getName() + " does not contain qualified exports or opens for module " + targetName
            );
        }
        List<String> exports = qualifiedExports.getOrDefault(targetName, List.of());
        for (String export : exports) {
            addExports(export, target);
        }
        List<String> opens = qualifiedOpens.getOrDefault(targetName, List.of());
        for (String open : opens) {
            addOpens(open, target);
        }
    }

    /**
     * Add a qualified export. This should always be implemented as follows:
     * <code>
     *     module.addExports(pkg, target);
     * </code>
     */
    protected abstract void addExports(String pkg, Module target);

    /**
     * Add a qualified open. This should always be implemented as follows:
     * <code>
     *     module.addOpens(pkg, target);
     * </code>
     */
    protected abstract void addOpens(String pkg, Module target);
}
