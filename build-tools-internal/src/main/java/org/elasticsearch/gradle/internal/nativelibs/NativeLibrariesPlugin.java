/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.nativelibs;

import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;

import java.util.Map;

/**
 * Resolves the native libraries a project consumes, each from either its published artifact or
 * from a fresh source-code build, and exposes them through {@value #LIBRARIES_CONFIGURATION}.
 *
 * <p>Libraries are declared as {@link NativeLibrarySpec}, and the choice of their origin (source-code
 * vs published artifact) is made here.
 */
public class NativeLibrariesPlugin implements Plugin<Project> {

    /** Extension listing the libraries this project consumes. */
    public static final String EXTENSION = "nativeLibraries";

    /** Dependency-scope configuration holding whichever source was selected per library. */
    public static final String SOURCES_CONFIGURATION = "nativeLibrarySources";

    /** Resolvable configuration to consume; yields per-platform directories. */
    public static final String LIBRARIES_CONFIGURATION = "resolvedNativeLibraries";

    @Override
    public void apply(Project project) {
        NamedDomainObjectContainer<NativeLibrarySpec> libraries = project.getObjects().domainObjectContainer(NativeLibrarySpec.class);
        project.getExtensions().add(EXTENSION, libraries);

        ProviderFactory providers = project.getProviders();
        DependencyHandler dependencyHandler = project.getDependencies();

        // Gradle 9 rejects a configuration that both declares dependencies and is resolved, so two
        // configurations are needed: libraries are declared in the first, consumers resolve the second.
        Configuration sources = project.getConfigurations().dependencyScope(SOURCES_CONFIGURATION).get();

        project.getConfigurations().resolvable(LIBRARIES_CONFIGURATION, configuration -> {
            configuration.extendsFrom(sources);
            configuration.getAttributes().attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE);
        });

        libraries.all(
            library -> dependencyHandler.addProvider(SOURCES_CONFIGURATION, dependencyFor(providers, dependencyHandler, library))
        );
    }

    private static Provider<Dependency> dependencyFor(
        ProviderFactory providers,
        DependencyHandler dependencies,
        NativeLibrarySpec library
    ) {
        return library.getModeEnvironmentVariable()
            .flatMap(providers::environmentVariable)
            .orElse(BuildNativeLibraryTask.PUBLISHED_MODE)
            .map(
                mode -> BuildNativeLibraryTask.PUBLISHED_MODE.equals(mode)
                    ? dependencies.create(library.getPublishedModule().get())
                    : dependencies.project(
                        Map.of("path", library.getBuiltBy().get(), "configuration", NativeLibraryBuildPlugin.ELEMENTS_CONFIGURATION)
                    )
            );
    }
}
