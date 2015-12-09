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

import com.carrotsearch.gradle.junit4.RandomizedTestingPlugin
import org.elasticsearch.gradle.BuildPlugin
import org.elasticsearch.gradle.VersionProperties
import org.elasticsearch.gradle.precommit.PrecommitTasks
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaBasePlugin
import org.gradle.plugins.ide.eclipse.model.EclipseClasspath

/** Configures the build to have a rest integration test.  */
public class StandaloneTestBasePlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.pluginManager.apply(JavaBasePlugin)
        project.pluginManager.apply(RandomizedTestingPlugin)

        BuildPlugin.globalBuildInfo(project)
        BuildPlugin.configureRepositories(project)

        // only setup tests to build
        project.sourceSets.create('test')
        project.dependencies.add('testCompile', "org.elasticsearch:test-framework:${VersionProperties.elasticsearch}")

        project.eclipse.classpath.sourceSets = [project.sourceSets.test]
        project.eclipse.classpath.plusConfigurations = [project.configurations.testRuntime]
        project.idea.module.testSourceDirs += project.sourceSets.test.java.srcDirs
        project.idea.module.scopes['TEST'] = [plus: [project.configurations.testRuntime]]

        PrecommitTasks.create(project, false)
        project.check.dependsOn(project.precommit)
    }
}
