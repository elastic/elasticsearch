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

import nebula.plugin.publishing.maven.MavenBasePublishPlugin
import nebula.plugin.publishing.maven.MavenManifestPlugin
import nebula.plugin.publishing.maven.MavenScmPlugin
import org.elasticsearch.gradle.BuildPlugin
import org.elasticsearch.gradle.test.RestIntegTestTask
import org.elasticsearch.gradle.test.RunTask
import org.gradle.api.Project
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.bundling.Zip

/**
 * Encapsulates build configuration for an Elasticsearch plugin.
 */
public class PluginBuildPlugin extends BuildPlugin {

    @Override
    public void apply(Project project) {
        super.apply(project)
        configureDependencies(project)
        // this afterEvaluate must happen before the afterEvaluate added by integTest creation,
        // so that the file name resolution for installing the plugin will be setup
        project.afterEvaluate {
            String name = project.pluginProperties.extension.name
            project.jar.baseName = name
            project.bundlePlugin.baseName = name

            project.integTest.dependsOn(project.bundlePlugin)
            project.tasks.run.dependsOn(project.bundlePlugin)
            if (project.path.startsWith(':modules:')) {
                project.integTest.clusterConfig.module(project)
                project.tasks.run.clusterConfig.module(project)
            } else {
                project.integTest.clusterConfig.plugin(name, project.bundlePlugin.outputs.files)
                project.tasks.run.clusterConfig.plugin(name, project.bundlePlugin.outputs.files)
                addPomGeneration(project)
            }

            project.namingConventions {
                // Plugins decalare extensions of ESIntegTestCase as "Tests" instead of IT.
                skipIntegTestInDisguise = true
            }
        }
        createIntegTestTask(project)
        createBundleTask(project)
        project.tasks.create('run', RunTask) // allow running ES with this plugin in the foreground of a build
    }

    private static void configureDependencies(Project project) {
        project.dependencies {
            provided "org.elasticsearch:elasticsearch:${project.versions.elasticsearch}"
            testCompile "org.elasticsearch.test:framework:${project.versions.elasticsearch}"
            // we "upgrade" these optional deps to provided for plugins, since they will run
            // with a full elasticsearch server that includes optional deps
            provided "org.locationtech.spatial4j:spatial4j:${project.versions.spatial4j}"
            provided "com.vividsolutions:jts:${project.versions.jts}"
            provided "log4j:log4j:${project.versions.log4j}"
            provided "log4j:apache-log4j-extras:${project.versions.log4j}"
            provided "net.java.dev.jna:jna:${project.versions.jna}"
        }
    }

    /** Adds an integTest task which runs rest tests */
    private static void createIntegTestTask(Project project) {
        RestIntegTestTask integTest = project.tasks.create('integTest', RestIntegTestTask.class)
        integTest.mustRunAfter(project.precommit, project.test)
        project.check.dependsOn(integTest)
    }

    /**
     * Adds a bundlePlugin task which builds the zip containing the plugin jars,
     * metadata, properties, and packaging files
     */
    private static void createBundleTask(Project project) {
        File pluginMetadata = project.file('src/main/plugin-metadata')

        // create a task to build the properties file for this plugin
        PluginPropertiesTask buildProperties = project.tasks.create('pluginProperties', PluginPropertiesTask.class)

        // add the plugin properties and metadata to test resources, so unit tests can
        // know about the plugin (used by test security code to statically initialize the plugin in unit tests)
        SourceSet testSourceSet = project.sourceSets.test
        testSourceSet.output.dir(buildProperties.generatedResourcesDir, builtBy: 'pluginProperties')
        testSourceSet.resources.srcDir(pluginMetadata)

        // create the actual bundle task, which zips up all the files for the plugin
        Zip bundle = project.tasks.create(name: 'bundlePlugin', type: Zip, dependsOn: [project.jar, buildProperties]) {
            from buildProperties // plugin properties file
            from pluginMetadata // metadata (eg custom security policy)
            from project.jar // this plugin's jar
            from project.configurations.runtime - project.configurations.provided // the dep jars
            // extra files for the plugin to go into the zip
            from('src/main/packaging') // TODO: move all config/bin/_size/etc into packaging
            from('src/main') {
                include 'config/**'
                include 'bin/**'
            }
            if (project.path.startsWith(':modules:') == false) {
                into('elasticsearch')
            }
        }
        project.assemble.dependsOn(bundle)

        // remove jar from the archives (things that will be published), and set it to the zip
        project.configurations.archives.artifacts.removeAll { it.archiveTask.is project.jar }
        project.artifacts.add('archives', bundle)

        // also make the zip the default artifact (used when depending on this project)
        project.configurations.getByName('default').extendsFrom = []
        project.artifacts.add('default', bundle)
    }

    /**
     * Adds the plugin jar and zip as publications.
     */
    protected static void addPomGeneration(Project project) {
        project.plugins.apply(MavenBasePublishPlugin.class)
        project.plugins.apply(MavenScmPlugin.class)

        project.publishing {
            publications {
                nebula {
                    artifact project.bundlePlugin
                    pom.withXml {
                        // overwrite the name/description in the pom nebula set up
                        Node root = asNode()
                        for (Node node : root.children()) {
                            if (node.name() == 'name') {
                                node.setValue(project.pluginProperties.extension.name)
                            } else if (node.name() == 'description') {
                                node.setValue(project.pluginProperties.extension.description)
                            }
                        }
                    }
                }
            }
        }

    }
}
