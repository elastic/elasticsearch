/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import org.elasticsearch.gradle.transform.FilteringJarTransform;
import org.gradle.api.Named;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.attributes.Bundling;
import org.gradle.api.attributes.Category;
import org.gradle.api.attributes.Usage;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.tasks.AbstractCopyTask;

import java.util.List;

import javax.inject.Inject;

/**
 * Creates distribution-layout configurations and exposes typed copy-spec builders via {@code esDistributionLayout}.
 */
public class DistributionLayoutPlugin implements Plugin<Project> {

    private final FileSystemOperations fileSystemOperations;

    @Inject
    public DistributionLayoutPlugin(FileSystemOperations fileSystemOperations) {
        this.fileSystemOperations = fileSystemOperations;
    }

    @Override
    public void apply(Project project) {
        ObjectFactory objects = project.getObjects();
        createLibraryConfigurations(project, objects);
        registerEntitlementBridgeTransform(project);

        DistributionLayoutExtension extension = project.getExtensions()
            .create(
                "esDistributionLayout",
                DistributionLayoutExtension.class,
                project,
                fileSystemOperations,
                objects,
                project.getProviders()
            );

        project.getTasks().withType(AbstractCopyTask.class).configureEach(copyTask -> {
            Task task = copyTask;
            // Shared project-wide registry for step 4: every copy task tracks all realized expansion maps.
            // This can invalidate unrelated distribution tasks but is strictly more correct than missing inputs.
            // TODO(step 7): precise per-task token expansion inputs.
            task.getInputs().property("esDistributionLayout.tokenExpansions", extension.getRequestedTokenExpansions());
        });
    }

    private static void registerEntitlementBridgeTransform(Project project) {
        FilteringJarTransform.registerTransform(project.getDependencies(), spec -> {
            spec.exclude("module-info.class");
            spec.exclude("META-INF/versions/**");
        });
    }

    private static void createLibraryConfigurations(Project project, ObjectFactory objects) {
        List<String> baseConfigurations = List.of(
            DistributionLibraries.LIBS,
            DistributionLibraries.LIBS_VERSION_CHECKER,
            DistributionLibraries.LIBS_CLI_LAUNCHER,
            DistributionLibraries.LIBS_SERVER_CLI,
            DistributionLibraries.LIBS_SERVER_LAUNCHER,
            DistributionLibraries.LIBS_SERVER_LAUNCHER_NATIVE_X64,
            DistributionLibraries.LIBS_SERVER_LAUNCHER_NATIVE_AARCH64,
            DistributionLibraries.LIBS_WINDOWS_SERVICE_CLI,
            DistributionLibraries.LIBS_PLUGIN_CLI,
            DistributionLibraries.LIBS_KEYSTORE_CLI,
            DistributionLibraries.LIBS_SECURITY_CLI,
            DistributionLibraries.LIBS_GEOIP_CLI,
            DistributionLibraries.LIBS_ANSI_CONSOLE,
            DistributionLibraries.LIBS_NATIVE,
            DistributionLibraries.LIBS_ENTITLEMENT_AGENT
        );

        for (String name : baseConfigurations) {
            project.getConfigurations().create(name, configuration -> configureRuntimeLibraryConfiguration(configuration, objects));
        }

        project.getConfigurations().create(DistributionLibraries.LIBS_ENTITLEMENT_BRIDGE, configuration -> {
            configureRuntimeLibraryConfiguration(configuration, objects);
            configuration.getAttributes()
                .attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, FilteringJarTransform.FILTERED_JAR_TYPE);
        });
    }

    private static void configureRuntimeLibraryConfiguration(Configuration configuration, ObjectFactory objects) {
        configuration.setCanBeConsumed(false);
        configuration.setCanBeResolved(true);
        configuration.attributes(attributes -> {
            attributes.attribute(Category.CATEGORY_ATTRIBUTE, named(objects, Category.class, Category.LIBRARY));
            attributes.attribute(Usage.USAGE_ATTRIBUTE, named(objects, Usage.class, Usage.JAVA_RUNTIME));
            attributes.attribute(Bundling.BUNDLING_ATTRIBUTE, named(objects, Bundling.class, Bundling.EXTERNAL));
        });
    }

    private static <T extends Named> T named(ObjectFactory objects, Class<T> type, String name) {
        return objects.named(type, name);
    }
}
