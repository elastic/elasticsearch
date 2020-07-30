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
package org.elasticsearch.gradle

import groovy.transform.CompileStatic
import org.elasticsearch.gradle.info.GlobalBuildInfoPlugin
import org.elasticsearch.gradle.precommit.PrecommitTasks
import org.gradle.api.GradleException
import org.gradle.api.InvalidUserDataException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.file.CopySpec
import org.gradle.api.plugins.ExtraPropertiesExtension
import org.gradle.api.tasks.bundling.Jar

/**
 * Encapsulates build configuration for elasticsearch projects.
 */
@CompileStatic
class BuildPlugin implements Plugin<Project> {

    @Override
    void apply(Project project) {
        // make sure the global build info plugin is applied to the root project
        project.rootProject.pluginManager.apply(GlobalBuildInfoPlugin)

        if (project.pluginManager.hasPlugin('elasticsearch.standalone-rest-test')) {
            throw new InvalidUserDataException('elasticsearch.standalone-test, '
                    + 'elasticsearch.standalone-rest-test, and elasticsearch.build '
                    + 'are mutually exclusive')
        }
        project.pluginManager.apply('elasticsearch.java')
        configureLicenseAndNotice(project)
        project.pluginManager.apply('elasticsearch.publish')
        project.pluginManager.apply(DependenciesInfoPlugin)

        project.extensions.getByType(ExtraPropertiesExtension).set('versions', VersionProperties.versions)
        PrecommitTasks.create(project, true)
    }

    static void configureLicenseAndNotice(Project project) {
        ExtraPropertiesExtension ext = project.extensions.getByType(ExtraPropertiesExtension)
        ext.set('licenseFile',  null)
        ext.set('noticeFile', null)
        // add license/notice files
        project.afterEvaluate {
            project.tasks.withType(Jar).configureEach { Jar jarTask ->
                if (ext.has('licenseFile') == false || ext.get('licenseFile') == null || ext.has('noticeFile') == false || ext.get('noticeFile') == null) {
                    throw new GradleException("Must specify license and notice file for project ${project.path}")
                }

                File licenseFile = ext.get('licenseFile') as File
                File noticeFile = ext.get('noticeFile') as File

                jarTask.metaInf { CopySpec spec ->
                    spec.from(licenseFile.parent) { CopySpec from ->
                        from.include licenseFile.name
                        from.rename { 'LICENSE.txt' }
                    }
                    spec.from(noticeFile.parent) { CopySpec from ->
                        from.include noticeFile.name
                        from.rename { 'NOTICE.txt' }
                    }
                }
            }
        }
    }
}
