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

package org.elasticsearch.gradle.plugin

import org.elasticsearch.gradle.BuildPlugin
import org.elasticsearch.gradle.test.RestTestPlugin
import org.elasticsearch.gradle.test.RunTask
import org.elasticsearch.gradle.test.StandaloneRestTestPlugin
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.file.FileCopyDetails
import org.gradle.api.file.RelativePath
import org.gradle.api.tasks.bundling.Zip

class MetaPluginBuildPlugin implements Plugin<Project> {

    @Override
    void apply(Project project) {
        project.plugins.apply(StandaloneRestTestPlugin)
        project.plugins.apply(RestTestPlugin)

        createBundleTask(project)

        project.integTestCluster {
            dependsOn(project.bundlePlugin)
            plugin(project.path)
        }
        BuildPlugin.configurePomGeneration(project)
        project.afterEvaluate {
            PluginBuildPlugin.addZipPomGeneration(project)
        }

        RunTask run = project.tasks.create('run', RunTask)
        run.dependsOn(project.bundlePlugin)
        run.clusterConfig.plugin(project.path)
    }

    private static void createBundleTask(Project project) {

        MetaPluginPropertiesTask buildProperties = project.tasks.create('pluginProperties', MetaPluginPropertiesTask.class)

        // create the actual bundle task, which zips up all the files for the plugin
        Zip bundle = project.tasks.create(name: 'bundlePlugin', type: Zip, dependsOn: [buildProperties]) {
            into('elasticsearch') {
                from(buildProperties.descriptorOutput.parentFile) {
                    // plugin properties file
                    include(buildProperties.descriptorOutput.name)
                }
            }
            // due to how the renames work for each bundled plugin, we must exclude empty dirs or every subdir
            // within bundled plugin zips will show up at the root as an empty dir
            includeEmptyDirs = false

        }
        project.assemble.dependsOn(bundle)

        // also make the zip available as a configuration (used when depending on this project)
        project.configurations.create('zip')
        project.artifacts.add('zip', bundle)

        // a super hacky way to inject code to run at the end of each of the bundled plugin's configuration
        // to add itself back to this meta plugin zip
        project.afterEvaluate {
            buildProperties.extension.plugins.each { String bundledPluginProjectName ->
                Project bundledPluginProject = project.project(bundledPluginProjectName)
                bundledPluginProject.afterEvaluate {
                    bundle.configure {
                        dependsOn bundledPluginProject.bundlePlugin
                        from(project.zipTree(bundledPluginProject.bundlePlugin.outputs.files.singleFile)) {
                            eachFile { FileCopyDetails details ->
                                // paths in the individual plugins begin with elasticsearch, and we want to add in the
                                // bundled plugin name between that and each filename
                                details.relativePath = new RelativePath(true, 'elasticsearch', bundledPluginProjectName,
                                                                        details.relativePath.toString().replace('elasticsearch/', ''))
                            }
                        }
                    }
                }
            }
        }
    }
}
