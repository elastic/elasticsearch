/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.gradle.internal.test.rest.InternalJavaRestTestPlugin;
import org.elasticsearch.gradle.internal.test.rest.LegacyJavaRestTestPlugin;
import org.elasticsearch.gradle.plugin.PluginBuildPlugin;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.ProjectDependency;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.SourceSet;
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
    private Project project;

    @Override
    public void apply(final Project project) {
        this.project = project;
        ExtraPropertiesExtension extraProperties = project.getExtensions().getExtraProperties();
        if (extraProperties.has("isEclipse") && Boolean.valueOf(extraProperties.get("isEclipse").toString())) {
            /* The changes this plugin makes both break and aren't needed by
             * Eclipse. This is because Eclipse flattens main and test
             * dependencies into a single dependency. Because Eclipse is
             * "special".... */
            return;
        }
        SourceSetContainer sourceSets = project.getExtensions().getByType(JavaPluginExtension.class).getSourceSets();
        project.getPlugins()
            .withType(
                LegacyJavaRestTestPlugin.class,
                legacyJavaRestTestPlugin -> processConfiguration(sourceSets.getByName(LegacyJavaRestTestPlugin.SOURCE_SET_NAME))
            );
        project.getPlugins()
            .withType(
                InternalJavaRestTestPlugin.class,
                internalJavaRestTestPlugin -> processConfiguration(sourceSets.getByName(InternalJavaRestTestPlugin.SOURCE_SET_NAME))
            );
        sourceSets.matching(sourceSet -> sourceSet.getName().equals("test")).configureEach(sourceSet -> processConfiguration(sourceSet));

    }

    private void processConfiguration(SourceSet sourceSet) {
        String implementationConfigurationName = sourceSet.getImplementationConfigurationName();
        Configuration implementationConfig = project.getConfigurations().getByName(implementationConfigurationName);
        implementationConfig.getDependencies().all(dep -> {
            if (dep instanceof ProjectDependency
                && ((ProjectDependency) dep).getDependencyProject().getPlugins().hasPlugin(PluginBuildPlugin.class)) {
                project.getGradle()
                    .projectsEvaluated(gradle -> addPluginResources(sourceSet, project, ((ProjectDependency) dep).getDependencyProject()));
            }
        });
    }

    private static void addPluginResources(SourceSet sourceSet, final Project project, final Project pluginProject) {
        final File outputDir = new File(project.getBuildDir(), "/generated-test-resources/" + pluginProject.getName());
        String camelProjectName = stream(pluginProject.getName().split("-")).map(t -> StringUtils.capitalize(t))
            .collect(Collectors.joining());
        String taskName = "copy" + camelProjectName + "Metadata";
        project.getTasks().register(taskName, Copy.class, copy -> {
            copy.into(outputDir);
            copy.from(pluginProject.getTasks().named("pluginProperties"));
            copy.from(pluginProject.file("src/main/plugin-metadata"));
        });

        Map<String, Object> map = Map.of("builtBy", taskName);
        sourceSet.getOutput().dir(map, outputDir);
    }
}
