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

import groovy.transform.CompileStatic
import org.elasticsearch.gradle.BuildPlugin
import org.elasticsearch.gradle.ElasticsearchJavaPlugin
import org.elasticsearch.gradle.ExportElasticsearchBuildResourcesTask
import org.elasticsearch.gradle.RepositoriesSetupPlugin
import org.elasticsearch.gradle.info.BuildParams
import org.elasticsearch.gradle.info.GlobalBuildInfoPlugin
import org.elasticsearch.gradle.precommit.PrecommitTasks
import org.elasticsearch.gradle.testclusters.TestClustersPlugin
import org.gradle.api.InvalidUserDataException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.Configuration
import org.gradle.api.plugins.JavaBasePlugin
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.testing.Test
import org.gradle.plugins.ide.eclipse.model.EclipseModel
import org.gradle.plugins.ide.idea.model.IdeaModel

/**
 * Configures the build to compile tests against Elasticsearch's test framework
 * and run REST tests. Use BuildPlugin if you want to build main code as well
 * as tests.
 */
@CompileStatic
class StandaloneRestTestPlugin implements Plugin<Project> {

    @Override
    void apply(Project project) {
        if (project.pluginManager.hasPlugin('elasticsearch.build')) {
            throw new InvalidUserDataException('elasticsearch.standalone-test '
                + 'elasticsearch.standalone-rest-test, and elasticsearch.build '
                + 'are mutually exclusive')
        }
        project.rootProject.pluginManager.apply(GlobalBuildInfoPlugin)
        project.pluginManager.apply(JavaBasePlugin)
        project.pluginManager.apply(TestClustersPlugin)
        project.pluginManager.apply(RepositoriesSetupPlugin)
        project.pluginManager.apply(RestTestBasePlugin)

        project.getTasks().register("buildResources", ExportElasticsearchBuildResourcesTask)
        ElasticsearchJavaPlugin.configureInputNormalization(project)
        ElasticsearchJavaPlugin.configureCompile(project)


        project.extensions.getByType(JavaPluginExtension).sourceCompatibility = BuildParams.minimumRuntimeVersion
        project.extensions.getByType(JavaPluginExtension).targetCompatibility = BuildParams.minimumRuntimeVersion

        // only setup tests to build
        SourceSetContainer sourceSets = project.extensions.getByType(SourceSetContainer)
        SourceSet testSourceSet = sourceSets.create('test')

        project.tasks.withType(Test).configureEach { Test test ->
            test.testClassesDirs = testSourceSet.output.classesDirs
            test.classpath = testSourceSet.runtimeClasspath
        }

        // create a compileOnly configuration as others might expect it
        project.configurations.create("compileOnly")
        project.dependencies.add('testImplementation', project.project(':test:framework'))

        EclipseModel eclipse = project.extensions.getByType(EclipseModel)
        eclipse.classpath.sourceSets = [testSourceSet]
        eclipse.classpath.plusConfigurations = [project.configurations.getByName(JavaPlugin.TEST_RUNTIME_CLASSPATH_CONFIGURATION_NAME)]

        IdeaModel idea = project.extensions.getByType(IdeaModel)
        idea.module.testSourceDirs += testSourceSet.java.srcDirs
        idea.module.scopes.put('TEST', [plus: [project.configurations.getByName(JavaPlugin.TEST_RUNTIME_CLASSPATH_CONFIGURATION_NAME)]] as Map<String, Collection<Configuration>>)

        PrecommitTasks.create(project, false)
    }
}
