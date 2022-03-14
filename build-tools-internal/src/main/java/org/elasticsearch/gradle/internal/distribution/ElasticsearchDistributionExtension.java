/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.distribution;

import org.elasticsearch.gradle.plugin.PluginPropertiesExtension;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.tasks.AbstractCopyTask;
import org.gradle.api.tasks.TaskProvider;

import java.util.Map;
import java.util.concurrent.Callable;

import static org.elasticsearch.gradle.plugin.PluginBuildPlugin.EXPLODED_BUNDLE_CONFIG;

public class ElasticsearchDistributionExtension {

    private final Project project;

    public ElasticsearchDistributionExtension(Project project) {
        this.project = project;
    }

    private Configuration moduleZip(Project module) {
        Dependency dep = project.getDependencies().project(Map.of("path", module.getPath(), "configuration", EXPLODED_BUNDLE_CONFIG));
        Configuration config = project.getConfigurations().detachedConfiguration(dep);
        return config;
    }

    public void copyModule(TaskProvider<AbstractCopyTask> copyTask, Project module) {
        copyTask.configure(sync -> {
            Configuration moduleConfig = moduleZip(module);
            sync.dependsOn(moduleConfig);
            Callable<Object> callableSingleFile = () -> moduleConfig.getSingleFile();
            sync.from(callableSingleFile, spec -> {
                spec.setIncludeEmptyDirs(false);

                // these are handled separately in the log4j config tasks in the :distribution plugin
                spec.exclude("*/config/log4j2.properties");
                spec.exclude("config/log4j2.properties");
                String moduleName = resolveModuleName(module);
                spec.eachFile(d -> d.setRelativePath(d.getRelativePath().prepend("modules", moduleName)));
            });
        });
    }

    private static String resolveModuleName(Project module) {
        if (module.getPlugins().hasPlugin("elasticsearch.esplugin")) {
            return module.getExtensions().getByType(PluginPropertiesExtension.class).getName();
        }
        throw new GradleException("Cannot copy from project not applying the 'elasticsearch.esplugin' plugin");
    }
}
