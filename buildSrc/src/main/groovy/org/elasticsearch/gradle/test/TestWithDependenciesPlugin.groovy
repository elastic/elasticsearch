/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.test

import org.elasticsearch.gradle.plugin.PluginBuildPlugin
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.Dependency
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.tasks.Copy

/**
 * A plugin to run tests that depend on other plugins or modules.
 *
 * This plugin will add the plugin-metadata and properties files for each
 * dependency to the test source set.
 */
class TestWithDependenciesPlugin implements Plugin<Project> {

    @Override
    void apply(Project project) {
        if (project.isEclipse) {
            /* The changes this plugin makes both break and aren't needed by
             * Eclipse. This is because Eclipse flattens main and test
             * dependencies into a single dependency. Because Eclipse is
             * "special".... */
            return
        }

        project.configurations.testImplementation.dependencies.all { Dependency dep ->
            // this closure is run every time a compile dependency is added
            if (dep instanceof ProjectDependency && dep.dependencyProject.plugins.hasPlugin(PluginBuildPlugin)) {
                project.gradle.projectsEvaluated {
                    addPluginResources(project, dep.dependencyProject)
                }
            }
        }
    }

    private static addPluginResources(Project project, Project pluginProject) {
        String outputDir = "${project.buildDir}/generated-resources/${pluginProject.name}"
        String camelName = pluginProject.name.replaceAll(/-(\w)/) { _, c -> c.toUpperCase(Locale.ROOT) }
        String taskName = "copy" + camelName[0].toUpperCase(Locale.ROOT) + camelName.substring(1) + "Metadata"
        project.tasks.register(taskName, Copy.class) {
            into(outputDir)
            from(pluginProject.tasks.pluginProperties)
            from(pluginProject.file('src/main/plugin-metadata'))
        }

        project.sourceSets.test.output.dir(outputDir, builtBy: taskName)
    }
}
