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

import nebula.plugin.extraconfigurations.ProvidedBasePlugin
import org.elasticsearch.gradle.BuildPlugin
import org.elasticsearch.gradle.ElasticsearchProperties
import org.elasticsearch.gradle.test.RestIntegTestTask
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.tasks.bundling.Zip

/**
 * Encapsulates build configuration for an Elasticsearch plugin.
 */
class PluginBuildPlugin extends BuildPlugin {

    @Override
    void apply(Project project) {
        super.apply(project)
        project.pluginManager.apply(ProvidedBasePlugin)
        // TODO: add target compatibility (java version) to elasticsearch properties and set for the project
        configureDependencies(project)
        // this afterEvaluate must happen before the afterEvaluate added by integTest configure,
        // so that the file name resolution for installing the plugin will be setup
        project.afterEvaluate {
            project.jar.configure {
                baseName project.pluginProperties.extension.name
            }
            project.bundlePlugin.configure {
                baseName project.pluginProperties.extension.name
            }
            project.integTest.configure {
                dependsOn project.bundlePlugin
                cluster {
                    plugin 'installPlugin', project.bundlePlugin.outputs.files
                }
            }
        }
        Task bundle = configureBundleTask(project)
        RestIntegTestTask.configure(project)
        project.configurations.archives.artifacts.removeAll { it.archiveTask.is project.jar }
        project.configurations.getByName('default').extendsFrom = []
        project.artifacts {
            archives bundle
            'default' bundle
        }
    }

    static void configureDependencies(Project project) {
        String elasticsearchVersion = ElasticsearchProperties.version
        project.dependencies {
            provided "org.elasticsearch:elasticsearch:${elasticsearchVersion}"
            //compile project.configurations.provided
            testCompile "org.elasticsearch:test-framework:${elasticsearchVersion}"
        }
    }

    static Task configureBundleTask(Project project) {
        PluginPropertiesTask buildProperties = project.tasks.create(name: 'pluginProperties', type: PluginPropertiesTask)
        Task bundle = project.tasks.create(name: 'bundlePlugin', type: Zip, dependsOn: [project.jar, buildProperties])
        bundle.configure {
            from buildProperties
            from project.jar
            from bundle.project.configurations.runtime - bundle.project.configurations.provided
            from('src/main/packaging') // TODO: move all config/bin/_size/etc into packaging
            from('src/main') {
                include 'config/**'
                include 'bin/**'
            }
            from('src/site') {
                include '_site/**'
            }
        }
        project.assemble.dependsOn(bundle)
        return bundle
    }
}
