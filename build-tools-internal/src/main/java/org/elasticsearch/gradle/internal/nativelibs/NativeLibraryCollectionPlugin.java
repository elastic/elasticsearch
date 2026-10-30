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
import org.gradle.api.NamedDomainObjectProvider;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.DependencyScopeConfiguration;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.provider.Provider;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Collects the native libraries of every project in this build that applies {@link NativeLibraryBuildPlugin}.
 *
 * <p>Apply this plugin to projects that need all the native libraries, not only those coming from
 * dependencies: for example the distribution projects, which need to include libraries loaded
 * only by modules and plugins.
 * The {@value #ALL_LIBRARIES_CONFIGURATION} configuration holds the directory containing the
 * native libraries for each project that applies {@link NativeLibraryBuildPlugin}.
 * <p>
 * To give a task only the native libraries of the projects it depends on, use
 * {@link NativeLibraryPathExtension} instead.
 */
public class NativeLibraryCollectionPlugin implements Plugin<Project> {

    public static final String PROJECTS_CONFIGURATION = "nativeLibraryProjects";

    /**
     * Resolvable configuration holding the native libraries directory for each project
     * that applies {@link NativeLibraryBuildPlugin}
     */
    public static final String ALL_LIBRARIES_CONFIGURATION = "allNativeLibraries";

    @Override
    public void apply(Project project) {
        Provider<ProjectSubscribeBuildService> service = project.getPlugins().apply(ProjectSubscribeServicePlugin.class).getService();
        DependencyHandler dependencies = project.getDependencies();

        // Read late: the topic is only complete once every project has been configured.
        Provider<List<Dependency>> producers = service.flatMap(s -> s.getProjectsByTopic(NativeLibraries.TOPIC))
            .map(paths -> dependenciesOn(dependencies, paths));

        NamedDomainObjectProvider<DependencyScopeConfiguration> projects = project.getConfigurations()
            .dependencyScope(PROJECTS_CONFIGURATION, configuration -> configuration.getDependencies().addAllLater(producers));
        project.getConfigurations().resolvable(ALL_LIBRARIES_CONFIGURATION, configuration -> {
            configuration.extendsFrom(projects.get());
            configuration.getAttributes().attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE);
        });
    }

    private static List<Dependency> dependenciesOn(DependencyHandler dependencies, Collection<String> projectPaths) {
        return projectPaths.stream()
            .map(path -> dependencies.project(Map.of("path", path, "configuration", NativeLibraryBuildPlugin.ELEMENTS_CONFIGURATION)))
            .toList();
    }
}
