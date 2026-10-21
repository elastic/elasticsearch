/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.nativelibs;

import org.elasticsearch.gradle.internal.ProjectSubscribeBuildService;
import org.elasticsearch.gradle.internal.ProjectSubscribeServicePlugin;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.provider.Provider;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Gathers the native libraries built in this build.
 *
 * <p>Applied to the project that merges the per-platform trees into the single layout in
 * {@code es.nativelibs.path}. {@value #PROJECTS_CONFIGURATION} is a dependency scope that
 * a collector extends with the native libraries it declares itself (the ones it downloads by
 * coordinate).
 */
public class NativeLibraryCollectionPlugin implements Plugin<Project> {

    public static final String PROJECTS_CONFIGURATION = "nativeLibraryProjects";

    @Override
    public void apply(Project project) {
        Provider<ProjectSubscribeBuildService> service = project.getPlugins().apply(ProjectSubscribeServicePlugin.class).getService();
        DependencyHandler dependencies = project.getDependencies();

        // Read late: the topic is only complete once every project has been configured.
        Provider<List<Dependency>> producers = service.flatMap(s -> s.getProjectsByTopic(NativeLibraries.TOPIC))
            .map(paths -> dependenciesOn(dependencies, paths));

        project.getConfigurations()
            .dependencyScope(PROJECTS_CONFIGURATION, configuration -> configuration.getDependencies().addAllLater(producers));
    }

    private static List<Dependency> dependenciesOn(DependencyHandler dependencies, Collection<String> projectPaths) {
        return projectPaths.stream()
            .map(path -> dependencies.project(Map.of("path", path, "configuration", NativeLibraryBuildPlugin.ELEMENTS_CONFIGURATION)))
            .toList();
    }
}
