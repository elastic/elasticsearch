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
import org.apache.commons.io.IOUtils
import org.elasticsearch.gradle.info.GlobalBuildInfoPlugin
import org.elasticsearch.gradle.precommit.PrecommitTasks
import org.elasticsearch.gradle.test.ErrorReportingTestListener
import org.elasticsearch.gradle.testclusters.ElasticsearchCluster
import org.elasticsearch.gradle.testclusters.TestClustersPlugin
import org.elasticsearch.gradle.util.GradleUtils
import org.gradle.api.*
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.Dependency
import org.gradle.api.artifacts.ModuleDependency
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.artifacts.dsl.RepositoryHandler
import org.gradle.api.artifacts.repositories.ExclusiveContentRepository
import org.gradle.api.artifacts.repositories.IvyArtifactRepository
import org.gradle.api.artifacts.repositories.IvyPatternRepositoryLayout
import org.gradle.api.artifacts.repositories.MavenArtifactRepository
import org.gradle.api.credentials.HttpHeaderCredentials
import org.gradle.api.execution.TaskActionListener
import org.gradle.api.file.CopySpec
import org.gradle.api.plugins.ExtraPropertiesExtension
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.tasks.bundling.Jar
import org.gradle.api.tasks.testing.Test
import org.gradle.util.GradleVersion

import java.nio.charset.StandardCharsets

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

        project.getTasks().register("buildResources", ExportElasticsearchBuildResourcesTask)

        project.extensions.getByType(ExtraPropertiesExtension).set('versions', VersionProperties.versions)
        PrecommitTasks.create(project, true)
        configureFips140(project)
    }

    static void configureFips140(Project project) {
        // Common config when running with a FIPS-140 runtime JVM
        if (inFipsJvm()) {
            ExportElasticsearchBuildResourcesTask buildResources = project.tasks.getByName('buildResources') as ExportElasticsearchBuildResourcesTask
            File securityProperties = buildResources.copy("fips_java.security")
            File securityPolicy = buildResources.copy("fips_java.policy")
            File bcfksKeystore = buildResources.copy("cacerts.bcfks")
            // This configuration can be removed once system modules are available
            GradleUtils.maybeCreate(project.configurations, 'extraJars') {
                project.dependencies.add('extraJars', "org.bouncycastle:bc-fips:1.0.1")
                project.dependencies.add('extraJars', "org.bouncycastle:bctls-fips:1.0.9")
            }
            project.pluginManager.withPlugin("elasticsearch.testclusters") {
                NamedDomainObjectContainer<ElasticsearchCluster> testClusters = project.extensions.findByName(TestClustersPlugin.EXTENSION_NAME) as NamedDomainObjectContainer<ElasticsearchCluster>
                testClusters.all { ElasticsearchCluster cluster ->
                    for (File dep : project.getConfigurations().getByName("extraJars").getFiles()){
                        cluster.extraJarFile(dep)
                    }
                    cluster.extraConfigFile("fips_java.security", securityProperties)
                    cluster.extraConfigFile("fips_java.policy", securityPolicy)
                    cluster.extraConfigFile("cacerts.bcfks", bcfksKeystore)
                    cluster.systemProperty('java.security.properties', '=${ES_PATH_CONF}/fips_java.security')
                    cluster.systemProperty('java.security.policy', '=${ES_PATH_CONF}/fips_java.policy')
                    cluster.systemProperty('javax.net.ssl.trustStore', '${ES_PATH_CONF}/cacerts.bcfks')
                    cluster.systemProperty('javax.net.ssl.trustStorePassword', 'password')
                    cluster.systemProperty('javax.net.ssl.keyStorePassword', 'password')
                    cluster.systemProperty('javax.net.ssl.keyStoreType', 'BCFKS')
                }
            }
            project.tasks.withType(Test).configureEach { Test task ->
                task.dependsOn(buildResources)
                task.systemProperty('javax.net.ssl.trustStorePassword', 'password')
                task.systemProperty('javax.net.ssl.keyStorePassword', 'password')
                task.systemProperty('javax.net.ssl.trustStoreType', 'BCFKS')
                // Using the key==value format to override default JVM security settings and policy
                // see also: https://docs.oracle.com/javase/8/docs/technotes/guides/security/PolicyFiles.html
                task.systemProperty('java.security.properties', String.format(Locale.ROOT, "=%s", securityProperties.toString()))
                task.systemProperty('java.security.policy', String.format(Locale.ROOT, "=%s", securityPolicy.toString()))
                task.systemProperty('javax.net.ssl.trustStore', bcfksKeystore.toString())
            }

        }
    }

    private static inFipsJvm(){
        return Boolean.parseBoolean(System.getProperty("tests.fips.enabled"));
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
