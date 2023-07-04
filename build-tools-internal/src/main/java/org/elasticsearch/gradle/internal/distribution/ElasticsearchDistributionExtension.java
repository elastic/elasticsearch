/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.distribution;

import org.elasticsearch.gradle.plugin.PluginPropertiesExtension;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.tasks.AbstractCopyTask;
import org.gradle.api.tasks.TaskProvider;

import java.io.File;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

import static org.elasticsearch.gradle.plugin.BasePluginBuildPlugin.EXPLODED_BUNDLE_CONFIG;

public class ElasticsearchDistributionExtension {
    private static final Pattern CONFIG_BIN_REGEX_PATTERN = Pattern.compile("([^\\/]+\\/)?(config|bin)\\/.*");

    private final Project project;

    public ElasticsearchDistributionExtension(Project project) {
        this.project = project;
    }

    private Configuration moduleZip(Project module) {
        var moduleConfigurationCoords = Map.of("path", module.getPath(), "configuration", EXPLODED_BUNDLE_CONFIG);
        var dep = project.getDependencies().project(moduleConfigurationCoords);
        return project.getConfigurations().detachedConfiguration(dep);
    }

    public void copyModule(TaskProvider<? extends AbstractCopyTask> copyTask, Project module) {
        copyTask.configure(sync -> {
            var moduleConfig = moduleZip(module);
            sync.dependsOn(moduleConfig);
            Callable<File> callableSingleFile = () -> moduleConfig.getSingleFile();
            sync.from(callableSingleFile, spec -> {
                spec.setIncludeEmptyDirs(false);
                // these are handled separately in the log4j config tasks in the :distribution plugin
                spec.exclude("*/config/log4j2.properties");
                spec.exclude("config/log4j2.properties");

                // This adds a implicit dependency for 'module' to PluginBuildPlugin which is fine as we just fail
                // in case an invalid 'module' not applying this plugin is passed here
                var moduleName = module.getExtensions().getByType(PluginPropertiesExtension.class).getName();

                spec.eachFile(d -> {
                    if (CONFIG_BIN_REGEX_PATTERN.matcher(d.getRelativePath().getPathString()).matches() == false) {
                        d.setRelativePath(d.getRelativePath().prepend("modules", moduleName));
                    }
                });
            });
        });
    }
}
