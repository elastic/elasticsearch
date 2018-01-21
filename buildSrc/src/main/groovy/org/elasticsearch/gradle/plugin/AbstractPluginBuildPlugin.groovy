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
import org.elasticsearch.gradle.NoticeTask
import org.elasticsearch.gradle.test.RestIntegTestTask
import org.gradle.api.Project
import org.gradle.api.XmlProvider
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin

import java.util.regex.Matcher
import java.util.regex.Pattern

abstract class AbstractPluginBuildPlugin extends BuildPlugin {

    @Override
    void apply(Project project) {
        super.apply(project)
    }

    protected static void configureDependencies(Project project) {
        project.dependencies {
            provided "org.elasticsearch:elasticsearch:${project.versions.elasticsearch}"
            testCompile "org.elasticsearch.test:framework:${project.versions.elasticsearch}"
            // we "upgrade" these optional deps to provided for plugins, since they will run
            // with a full elasticsearch server that includes optional deps
            provided "org.locationtech.spatial4j:spatial4j:${project.versions.spatial4j}"
            provided "com.vividsolutions:jts:${project.versions.jts}"
            provided "org.apache.logging.log4j:log4j-api:${project.versions.log4j}"
            provided "org.apache.logging.log4j:log4j-core:${project.versions.log4j}"
            provided "org.elasticsearch:jna:${project.versions.jna}"
        }
    }

    /** Adds an integTest task which runs rest tests */
    protected static void createIntegTestTask(Project project) {
        RestIntegTestTask integTest = project.tasks.create('integTest', RestIntegTestTask.class)
        integTest.mustRunAfter(project.precommit, project.test)
        project.check.dependsOn(integTest)
    }

    static final Pattern GIT_PATTERN = Pattern.compile(/git@([^:]+):([^\.]+)\.git/)

    /** Find the reponame. */
    protected static String urlFromOrigin(String origin) {
        if (origin == null) {
            return null // best effort, the url doesnt really matter, it is just required by maven central
        }
        if (origin.startsWith('https')) {
            return origin
        }
        Matcher matcher = GIT_PATTERN.matcher(origin)
        if (matcher.matches()) {
            return "https://${matcher.group(1)}/${matcher.group(2)}"
        } else {
            return origin // best effort, the url doesnt really matter, it is just required by maven central
        }
    }

    /** Adds a task to generate a pom file for the zip distribution. */
    protected void addZipPomGeneration(Project project) {
        project.plugins.apply(MavenPublishPlugin.class)

        project.publishing {
            publications {
                zip(MavenPublication) {
                    artifact project.bundlePlugin
                }
                /* HUGE HACK: the underlying maven publication library refuses to deploy any attached artifacts
                 * when the packaging type is set to 'pom'. But Sonatype's OSS repositories require source files
                 * for artifacts that are of type 'zip'. We already publish the source and javadoc for Elasticsearch
                 * under the various other subprojects. So here we create another publication using the same
                 * name that has the "real" pom, and rely on the fact that gradle will execute the publish tasks
                 * in alphabetical order. This lets us publish the zip file and even though the pom says the
                 * type is 'pom' instead of 'zip'. We cannot setup a dependency between the tasks because the
                 * publishing tasks are created *extremely* late in the configuration phase, so that we cannot get
                 * ahold of the actual task. Furthermore, this entire hack only exists so we can make publishing to
                 * maven local work, since we publish to maven central externally. */
                zipReal(MavenPublication) {
                    artifactId = project.pluginProperties.extension.name
                    pom.withXml { XmlProvider xml ->
                        Node root = xml.asNode()
                        root.appendNode('name', project.pluginProperties.extension.name)
                        root.appendNode('description', project.pluginProperties.extension.description)
                        root.appendNode('url', urlFromOrigin(project.scminfo.origin))
                        Node scmNode = root.appendNode('scm')
                        scmNode.appendNode('url', project.scminfo.origin)
                    }
                }
            }
        }
    }

    protected void addNoticeGeneration(Project project) {
        File licenseFile = project.pluginProperties.extension.licenseFile
        if (licenseFile != null) {
            project.bundlePlugin.from(licenseFile.parentFile) {
                include(licenseFile.name)
            }
        }
        File noticeFile = project.pluginProperties.extension.noticeFile
        if (noticeFile != null) {
            NoticeTask generateNotice = project.tasks.create('generateNotice', NoticeTask.class)
            generateNotice.inputFile = noticeFile
            project.bundlePlugin.from(generateNotice)
        }
    }

}
