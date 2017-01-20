/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

        project.configurations.testCompile.dependencies.all { Dependency dep ->
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
        String taskName = ClusterFormationTasks.pluginTaskName("copy", pluginProject.name, "Metadata")
        Copy copyPluginMetadata = project.tasks.create(taskName, Copy.class)
        copyPluginMetadata.into(outputDir)
        copyPluginMetadata.from(pluginProject.tasks.pluginProperties)
        copyPluginMetadata.from(pluginProject.file('src/main/plugin-metadata'))
        project.sourceSets.test.output.dir(outputDir, builtBy: taskName)
    }
}
