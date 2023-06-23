/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.plugin;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.RegularFile;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;

public class StablePluginBuildPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(BasePluginBuildPlugin.class);

        project.getTasks().withType(GeneratePluginPropertiesTask.class).named("pluginProperties").configure(task -> {
            task.getIsStable().set(true);

            Provider<RegularFile> file = project.getLayout()
                .getBuildDirectory()
                .file("generated-descriptor/" + GeneratePluginPropertiesTask.STABLE_PROPERTIES_FILENAME);
            task.getOutputFile().set(file);
        });

        final var pluginNamedComponents = project.getTasks().register("pluginNamedComponents", GenerateNamedComponentsTask.class, t -> {

            SourceSet mainSourceSet = GradleUtils.getJavaSourceSets(project).findByName(SourceSet.MAIN_SOURCE_SET_NAME);
            FileCollection dependencyJars = project.getConfigurations().getByName(JavaPlugin.COMPILE_CLASSPATH_CONFIGURATION_NAME);
            FileCollection compiledPluginClasses = mainSourceSet.getOutput().getClassesDirs();
            FileCollection classPath = dependencyJars.plus(compiledPluginClasses);
            t.setClasspath(classPath);
        });
        Configuration pluginScannerConfig = project.getConfigurations().create("pluginScannerConfig");
        DependencyHandler dependencyHandler = project.getDependencies();
        pluginScannerConfig.defaultDependencies(
            deps -> deps.add(
                dependencyHandler.create("org.elasticsearch:elasticsearch-plugin-scanner:" + VersionProperties.getElasticsearch())
            )
        );
        pluginNamedComponents.configure(t -> { t.setPluginScannerClasspath(pluginScannerConfig); });

        final var pluginExtension = project.getExtensions().getByType(PluginPropertiesExtension.class);
        pluginExtension.getBundleSpec().from(pluginNamedComponents);
    }
}
