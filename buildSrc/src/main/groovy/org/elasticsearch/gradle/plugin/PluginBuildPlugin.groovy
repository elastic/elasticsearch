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
import org.elasticsearch.gradle.test.RestIntegTestTask
import org.elasticsearch.gradle.test.RunTask
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
        configureDependencies(project)
        // this afterEvaluate must happen before the afterEvaluate added by integTest configure,
        // so that the file name resolution for installing the plugin will be setup
        project.afterEvaluate {
            String name = project.pluginProperties.extension.name
            project.jar.baseName = name
            project.bundlePlugin.baseName = name
            project.integTest.dependsOn(project.bundlePlugin)
            project.integTest.clusterConfig.plugin(name, project.bundlePlugin.outputs.files)
            project.tasks.run.dependsOn(project.bundlePlugin)
            project.tasks.run.clusterConfig.plugin(name, project.bundlePlugin.outputs.files)
        }
        RestIntegTestTask.configure(project)
        RunTask.configure(project)
        Task bundle = configureBundleTask(project)
        project.configurations.archives.artifacts.removeAll { it.archiveTask.is project.jar }
        project.configurations.getByName('default').extendsFrom = []
        project.artifacts {
            archives bundle
            'default' bundle
        }
    }

    static void configureDependencies(Project project) {
        project.dependencies {
            provided "org.elasticsearch:elasticsearch:${project.versions.elasticsearch}"
            testCompile "org.elasticsearch:test-framework:${project.versions.elasticsearch}"
            // we "upgrade" these optional deps to provided for plugins, since they will run
            // with a full elasticsearch server that includes optional deps
            provided "com.spatial4j:spatial4j:${project.versions.spatial4j}"
            provided "com.vividsolutions:jts:${project.versions.jts}"
            provided "com.github.spullara.mustache.java:compiler:${project.versions.mustache}"
            provided "log4j:log4j:${project.versions.log4j}"
            provided "log4j:apache-log4j-extras:${project.versions.log4j}"
            provided "org.slf4j:slf4j-api:${project.versions.slf4j}"
            provided "net.java.dev.jna:jna:${project.versions.jna}"
        }
    }

    static Task configureBundleTask(Project project) {
        PluginPropertiesTask buildProperties = project.tasks.create(name: 'pluginProperties', type: PluginPropertiesTask)
        File pluginMetadata = project.file("src/main/plugin-metadata")
        project.sourceSets.test {
            output.dir(buildProperties.generatedResourcesDir, builtBy: 'pluginProperties')
            resources {
                srcDir pluginMetadata
            }
        }
        Task bundle = project.tasks.create(name: 'bundlePlugin', type: Zip, dependsOn: [project.jar, buildProperties])
        bundle.configure {
            from buildProperties
            from pluginMetadata
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
