/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.jdk;

import java.lang.module.ModuleDescriptor.Exports;
import java.lang.module.ModuleDescriptor.Opens;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An object that provides a callback for qualified exports and opens.
 *
 * Because Elasticsearch constructs plugin module layers dynamically, qualified
 * exports are silently dropped in the boot layer. Modules that have qualified
 * exports should implement this service so that when a qualified export
 * module is loaded, the exporting or opening module can be informed and
 * export or open dynamically to the newly loaded module.
 */
public abstract class ModuleQualifiedExportsService {

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

    private <T> Map<String, List<String>> invert(
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
