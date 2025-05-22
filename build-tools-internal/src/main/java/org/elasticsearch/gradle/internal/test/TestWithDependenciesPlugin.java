/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test;

import org.apache.commons.lang.StringUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.ProjectDependency;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.attributes.Attribute;
import org.gradle.api.attributes.LibraryElements;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.SourceSetContainer;

import java.io.File;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Arrays.stream;

/**
 * A plugin to run tests that depend on other plugins or modules.
 * <p>
 * This plugin will add the plugin-metadata and properties files for each
 * dependency to the test source set.
 */
public class TestWithDependenciesPlugin implements Plugin<Project> {
    @Override
    public void apply(final Project project) {
        ExtraPropertiesExtension extraProperties = project.getExtensions().getExtraProperties();
        if (extraProperties.has("isEclipse") && Boolean.valueOf(extraProperties.get("isEclipse").toString())) {
            /* The changes this plugin makes both break and aren't needed by
             * Eclipse. This is because Eclipse flattens main and test
             * dependencies into a single dependency. Because Eclipse is
             * "special".... */
            return;
        }

        Configuration testImplementationConfig = project.getConfigurations().getByName("testImplementation");
        testImplementationConfig.getDependencies().configureEach(dep -> {
            if (dep instanceof ProjectDependency && dep.getGroup().contains("plugin")) {
                addPluginResources(project, ((ProjectDependency) dep));
            }
        });
    }

    private static void addPluginResources(final Project project, final ProjectDependency projectDependency) {
        final File outputDir = new File(project.getBuildDir(), "/generated-test-resources/" + projectDependency.getName());
        String camelProjectName = stream(projectDependency.getName().split("-")).map(t -> StringUtils.capitalize(t))
            .collect(Collectors.joining());
        String taskName = "copy" + camelProjectName + "Metadata";
        String metadataConfiguration = "resolved" + camelProjectName + "Metadata";
        Configuration pluginMetadata = project.getConfigurations().maybeCreate(metadataConfiguration);
        pluginMetadata.getAttributes().attribute(Attribute.of("pluginMetadata", Boolean.class), true);
        pluginMetadata.getAttributes()
            .attribute(
                LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE,
                project.getObjects().named(LibraryElements.class, LibraryElements.RESOURCES)
            );
        DependencyHandler dependencyHandler = project.getDependencies();
        Dependency pluginMetadataDependency = dependencyHandler.project(Map.of("path", projectDependency.getPath()));
        dependencyHandler.add(metadataConfiguration, pluginMetadataDependency);
        project.getTasks().register(taskName, Copy.class, copy -> {
            copy.into(outputDir);
            copy.from(pluginMetadata);
        });

        Map<String, Object> map = Map.of("builtBy", taskName);
        SourceSetContainer sourceSetContainer = project.getExtensions().getByType(SourceSetContainer.class);
        sourceSetContainer.getByName("test").getOutput().dir(map, outputDir);
    }
}
