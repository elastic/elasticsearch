/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.util;

import com.github.jengelman.gradle.plugins.shadow.ShadowBasePlugin;

import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.ResolvableDependencies;
import org.gradle.api.artifacts.component.ComponentIdentifier;
import org.gradle.api.artifacts.component.ProjectComponentIdentifier;
import org.gradle.api.artifacts.result.ResolvedComponentResult;
import org.gradle.api.artifacts.result.ResolvedDependencyResult;
import org.gradle.api.file.FileCollection;
import org.gradle.api.specs.AndSpec;
import org.gradle.api.specs.Spec;
import org.jetbrains.annotations.NotNull;

import java.util.Set;
import java.util.stream.Collectors;

public class DependenciesUtils {

    public static FileCollection createFileCollectionFromNonTransitiveArtifactsView(
        Configuration configuration,
        Spec<ComponentIdentifier> componentFilter
    ) {
        ResolvableDependencies incoming = configuration.getIncoming();
        return incoming.artifactView(viewConfiguration -> {
            Set<ComponentIdentifier> firstLevelDependencyComponents = incoming.getResolutionResult()
                .getRootComponent()
                .map(
                    rootComponent -> rootComponent.getDependencies()
                        .stream()
                        .filter(dependency -> dependency instanceof ResolvedDependencyResult)
                        .map(dependency -> (ResolvedDependencyResult) dependency)
                        .filter(dependency -> dependency.getSelected() instanceof ResolvedComponentResult)
                        .map(dependency -> dependency.getSelected().getId())
                        .collect(Collectors.toSet())
                )
                .get();
            viewConfiguration.componentFilter(
                new AndSpec<>(identifier -> firstLevelDependencyComponents.contains(identifier), componentFilter)
            );
        }).getFiles();
    }

    public static @NotNull FileCollection plainProjectedDependenciesFilteredView(Configuration configuration) {
        ResolvableDependencies incoming = configuration.getIncoming();
        return incoming.artifactView(v -> {
            // resolve componentIdentifier for all shadowed project dependencies
            Set<ComponentIdentifier> shadowedDependencies = incoming.getResolutionResult()
                .getRootComponent()
                .map(
                    root -> root.getDependencies()
                        .stream()
                        .filter(dep -> dep instanceof ResolvedDependencyResult)
                        .map(dep -> (ResolvedDependencyResult) dep)
                        .filter(dep -> dep.getSelected() instanceof ResolvedComponentResult)
                        .filter(dep -> dep.getResolvedVariant().getDisplayName() == ShadowBasePlugin.COMPONENT_NAME)
                        .map(dep -> dep.getSelected().getId())
                        .collect(Collectors.toSet())
                )
                .get();
            System.out.println("shadowedDependencies " + shadowedDependencies.size() + " " + shadowedDependencies);
            // filter out project dependencies if they are not a shadowed dependency
            v.componentFilter(i -> (i instanceof ProjectComponentIdentifier == false || shadowedDependencies.contains(i)));
        }).getFiles();
    }
}
