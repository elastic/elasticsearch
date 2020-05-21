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
import org.gradle.api.Action
import org.gradle.api.GradleException
import org.gradle.api.InvalidUserDataException
import org.gradle.api.NamedDomainObjectContainer
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
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
import org.gradle.api.plugins.ExtraPropertiesExtension
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.tasks.testing.Test
import org.gradle.authentication.http.HttpHeaderAuthentication
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
        String minimumGradleVersion = null
        InputStream is = getClass().getResourceAsStream("/minimumGradleVersion")
        try {
            minimumGradleVersion = IOUtils.toString(is, StandardCharsets.UTF_8.toString())
        } finally {
            is.close()
        }
        if (GradleVersion.current() < GradleVersion.version(minimumGradleVersion.trim())) {
            throw new GradleException(
                    "Gradle ${minimumGradleVersion}+ is required to use elasticsearch.build plugin"
            )
        }
        project.pluginManager.apply('elasticsearch.java')
        project.pluginManager.apply('elasticsearch.publish')
        project.pluginManager.apply(DependenciesInfoPlugin)

        // apply global test task failure listener
        project.rootProject.pluginManager.apply(TestFailureReportingPlugin)

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

    /**
     * Makes dependencies non-transitive.
     *
     * Gradle allows setting all dependencies as non-transitive very easily.
     * Sadly this mechanism does not translate into maven pom generation. In order
     * to effectively make the pom act as if it has no transitive dependencies,
     * we must exclude each transitive dependency of each direct dependency.
     *
     * Determining the transitive deps of a dependency which has been resolved as
     * non-transitive is difficult because the process of resolving removes the
     * transitive deps. To sidestep this issue, we create a configuration per
     * direct dependency version. This specially named and unique configuration
     * will contain all of the transitive dependencies of this particular
     * dependency. We can then use this configuration during pom generation
     * to iterate the transitive dependencies and add excludes.
     */
    static void configureConfigurations(Project project) {
        // we want to test compileOnly deps!
        project.configurations.getByName(JavaPlugin.TEST_COMPILE_CONFIGURATION_NAME).extendsFrom(project.configurations.getByName(JavaPlugin.COMPILE_ONLY_CONFIGURATION_NAME))

        // we are not shipping these jars, we act like dumb consumers of these things
        if (project.path.startsWith(':test:fixtures') || project.path == ':build-tools') {
            return
        }
        // fail on any conflicting dependency versions
        project.configurations.all({ Configuration configuration ->
            if (configuration.name.endsWith('Fixture')) {
                // just a self contained test-fixture configuration, likely transitive and hellacious
                return
            }
            configuration.resolutionStrategy {
                failOnVersionConflict()
            }
        })

        // force all dependencies added directly to compile/testCompile to be non-transitive, except for ES itself
        Closure disableTransitiveDeps = { Dependency dep ->
            if (dep instanceof ModuleDependency && !(dep instanceof ProjectDependency)
                    && dep.group.startsWith('org.elasticsearch') == false) {
                dep.transitive = false
            }
        }

        project.configurations.getByName(JavaPlugin.COMPILE_CONFIGURATION_NAME).dependencies.all(disableTransitiveDeps)
        project.configurations.getByName(JavaPlugin.TEST_COMPILE_CONFIGURATION_NAME).dependencies.all(disableTransitiveDeps)
        project.configurations.getByName(JavaPlugin.COMPILE_ONLY_CONFIGURATION_NAME).dependencies.all(disableTransitiveDeps)
        project.configurations.getByName(JavaPlugin.RUNTIME_ONLY_CONFIGURATION_NAME).dependencies.all(disableTransitiveDeps)
    }

    private static class TestFailureReportingPlugin implements Plugin<Project> {
        @Override
        void apply(Project project) {
            if (project != project.rootProject) {
                throw new IllegalStateException("${this.class.getName()} can only be applied to the root project.")
            }

            project.gradle.addListener(new TaskActionListener() {
                @Override
                void beforeActions(Task task) {

                }

                @Override
                void afterActions(Task task) {
                    if (task instanceof Test) {
                        ErrorReportingTestListener listener = task.extensions.findByType(ErrorReportingTestListener)
                        if (listener != null && listener.getFailedTests().size() > 0) {
                            task.logger.lifecycle("\nTests with failures:")
                            listener.getFailedTests().each {
                                task.logger.lifecycle(" - ${it.getFullName()}")
                            }
                        }
                    }
                }
            })
        }
    }

    private static inFipsJvm(){
        return Boolean.parseBoolean(System.getProperty("tests.fips.enabled"));
    }
}
