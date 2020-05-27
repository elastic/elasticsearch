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

import com.github.jengelman.gradle.plugins.shadow.ShadowPlugin
import org.elasticsearch.gradle.BuildPlugin
import org.elasticsearch.gradle.NoticeTask
import org.elasticsearch.gradle.Version
import org.elasticsearch.gradle.VersionProperties
import org.elasticsearch.gradle.info.BuildParams
import org.elasticsearch.gradle.test.rest.RestResourcesPlugin
import org.elasticsearch.gradle.test.RestIntegTestTask
import org.elasticsearch.gradle.testclusters.RunTask
import org.elasticsearch.gradle.testclusters.TestClustersPlugin
import org.gradle.api.InvalidUserDataException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.plugins.BasePlugin
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.TaskProvider
import org.gradle.api.tasks.bundling.Zip

import java.util.regex.Matcher
import java.util.regex.Pattern
/**
 * Encapsulates build configuration for an Elasticsearch plugin.
 */
class PluginBuildPlugin implements Plugin<Project> {

    public static final String PLUGIN_EXTENSION_NAME = 'esplugin'

    @Override
    void apply(Project project) {
        project.pluginManager.apply(BuildPlugin)
        project.pluginManager.apply(TestClustersPlugin)
        project.pluginManager.apply(RestResourcesPlugin)

        PluginPropertiesExtension extension = project.extensions.create(PLUGIN_EXTENSION_NAME, PluginPropertiesExtension, project)
        configureDependencies(project)

        boolean isXPackModule = project.path.startsWith(':x-pack:plugin')
        boolean isModule = project.path.startsWith(':modules:') || isXPackModule

        createIntegTestTask(project)
        createBundleTasks(project, extension)
        project.tasks.integTest.dependsOn(project.tasks.bundlePlugin)
        if (isModule) {
            project.testClusters.integTest.module(project.tasks.bundlePlugin.archiveFile)
        } else {
            project.testClusters.integTest.plugin(project.tasks.bundlePlugin.archiveFile)
        }

        project.afterEvaluate {
            project.extensions.getByType(PluginPropertiesExtension).extendedPlugins.each { pluginName ->
                // Auto add dependent modules to the test cluster
                if (project.findProject(":modules:${pluginName}") != null) {
                    project.integTest.dependsOn(project.project(":modules:${pluginName}").tasks.bundlePlugin)
                    project.testClusters.integTest.module(
                        project.project(":modules:${pluginName}").tasks.bundlePlugin.archiveFile
                    )
                }
            }
            PluginPropertiesExtension extension1 = project.getExtensions().getByType(PluginPropertiesExtension.class)
            configurePublishing(project, extension1)
            String name = extension1.name
            project.archivesBaseName = name
            project.description = extension1.description

            if (extension1.name == null) {
                throw new InvalidUserDataException('name is a required setting for esplugin')
            }
            if (extension1.description == null) {
                throw new InvalidUserDataException('description is a required setting for esplugin')
            }
            if (extension1.classname == null) {
                throw new InvalidUserDataException('classname is a required setting for esplugin')
            }

            Map<String, String> properties = [
                'name'                : extension1.name,
                'description'         : extension1.description,
                'version'             : extension1.version,
                'elasticsearchVersion': Version.fromString(VersionProperties.elasticsearch).toString(),
                'javaVersion'         : project.targetCompatibility as String,
                'classname'           : extension1.classname,
                'extendedPlugins'     : extension1.extendedPlugins.join(','),
                'hasNativeController' : extension1.hasNativeController,
                'requiresKeystore'    : extension1.requiresKeystore
            ]
            project.tasks.named('pluginProperties').configure {
                expand(properties)
                inputs.properties(properties)
            }
            if (isModule == false || isXPackModule) {
                addNoticeGeneration(project, extension1)
            }
        }

        project.tasks.named('testingConventions').configure {
            naming.clear()
            naming {
                Tests {
                    baseClass 'org.apache.lucene.util.LuceneTestCase'
                }
                IT {
                    baseClass 'org.elasticsearch.test.ESIntegTestCase'
                    baseClass 'org.elasticsearch.test.rest.ESRestTestCase'
                    baseClass 'org.elasticsearch.test.ESSingleNodeTestCase'
                }
            }
        }
        project.configurations.getByName('default')
            .extendsFrom(project.configurations.getByName('runtimeClasspath'))
        // allow running ES with this plugin in the foreground of a build
        project.tasks.register('run', RunTask) {
            dependsOn(project.tasks.bundlePlugin)
            useCluster project.testClusters.integTest
        }
    }


    private static void configurePublishing(Project project, PluginPropertiesExtension extension) {
        if (project.plugins.hasPlugin(MavenPublishPlugin)) {
            project.publishing.publications.nebula(MavenPublication).artifactId(extension.name)
        }
    }

    private static void configureDependencies(Project project) {
        project.dependencies {
            if (BuildParams.internal) {
                compileOnly project.project(':server')
                testCompile project.project(':test:framework')
            } else {
                compileOnly "org.elasticsearch:elasticsearch:${project.versions.elasticsearch}"
                testCompile "org.elasticsearch.test:framework:${project.versions.elasticsearch}"
            }
            // we "upgrade" these optional deps to provided for plugins, since they will run
            // with a full elasticsearch server that includes optional deps
            compileOnly "org.locationtech.spatial4j:spatial4j:${project.versions.spatial4j}"
            compileOnly "org.locationtech.jts:jts-core:${project.versions.jts}"
            compileOnly "org.apache.logging.log4j:log4j-api:${project.versions.log4j}"
            compileOnly "org.apache.logging.log4j:log4j-core:${project.versions.log4j}"
            compileOnly "org.elasticsearch:jna:${project.versions.jna}"
        }
    }

    /** Adds an integTest task which runs rest tests */
    private static void createIntegTestTask(Project project) {
        RestIntegTestTask integTest = project.tasks.create('integTest', RestIntegTestTask.class)
        integTest.mustRunAfter('precommit', 'test')
        project.check.dependsOn(integTest)
    }

    /**
     * Adds a bundlePlugin task which builds the zip containing the plugin jars,
     * metadata, properties, and packaging files
     */
    private static void createBundleTasks(Project project, PluginPropertiesExtension extension) {
        File pluginMetadata = project.file('src/main/plugin-metadata')
        File templateFile = new File(project.buildDir, "templates/plugin-descriptor.properties")

        // create tasks to build the properties file for this plugin
        TaskProvider<Task> copyPluginPropertiesTemplate = project.tasks.register('copyPluginPropertiesTemplate') {
            outputs.file(templateFile)
            doLast {
                InputStream resourceTemplate = PluginBuildPlugin.getResourceAsStream("/${templateFile.name}")
                templateFile.setText(resourceTemplate.getText('UTF-8'), 'UTF-8')
            }
        }

        TaskProvider<Copy> buildProperties = project.tasks.register('pluginProperties', Copy) {
            dependsOn(copyPluginPropertiesTemplate)
            from(templateFile)
            into("${project.buildDir}/generated-resources")
        }

        // add the plugin properties and metadata to test resources, so unit tests can
        // know about the plugin (used by test security code to statically initialize the plugin in unit tests)
        SourceSet testSourceSet = project.sourceSets.test
        testSourceSet.output.dir("${project.buildDir}/generated-resources", builtBy: buildProperties)
        testSourceSet.resources.srcDir(pluginMetadata)

        // create the actual bundle task, which zips up all the files for the plugin
        TaskProvider<Zip> bundle = project.tasks.register('bundlePlugin', Zip) {
            from buildProperties
            from pluginMetadata // metadata (eg custom security policy)
            /*
             * If the plugin is using the shadow plugin then we need to bundle
             * that shadow jar.
             */
            from { project.plugins.hasPlugin(ShadowPlugin) ? project.shadowJar : project.jar }
            from project.configurations.runtimeClasspath - project.configurations.compileOnly
            // extra files for the plugin to go into the zip
            from('src/main/packaging') // TODO: move all config/bin/_size/etc into packaging
            from('src/main') {
                include 'config/**'
                include 'bin/**'
            }
        }
        project.tasks.named(BasePlugin.ASSEMBLE_TASK_NAME).configure {
          dependsOn(bundle)
        }

        // also make the zip available as a configuration (used when depending on this project)
        project.configurations.create('zip')
        project.artifacts.add('zip', bundle)
    }

    /** Configure the pom for the main jar of this plugin */

    protected static void addNoticeGeneration(Project project, PluginPropertiesExtension extension) {
        File licenseFile = extension.licenseFile
        if (licenseFile != null) {
            project.tasks.named('bundlePlugin').configure {
                from(licenseFile.parentFile) {
                    include(licenseFile.name)
                    rename { 'LICENSE.txt' }
                }
            }
        }
        File noticeFile = extension.noticeFile
        if (noticeFile != null) {
            TaskProvider<NoticeTask> generateNotice = project.tasks.register('generateNotice', NoticeTask) {
                inputFile = noticeFile
            }
            project.tasks.named('bundlePlugin').configure {
                from(generateNotice)
            }
        }
    }
}
