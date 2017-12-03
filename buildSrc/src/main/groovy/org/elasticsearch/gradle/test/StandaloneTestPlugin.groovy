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

import com.carrotsearch.gradle.junit4.RandomizedTestingTask
import org.elasticsearch.gradle.BuildPlugin
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaBasePlugin

/**
 * Configures the build to compile against Elasticsearch's test framework and
 * run integration and unit tests. Use BuildPlugin if you want to build main
 * code as well as tests. */
public class StandaloneTestPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.pluginManager.apply(StandaloneRestTestPlugin)

        Map testOptions = [
            name: 'test',
            type: RandomizedTestingTask,
            dependsOn: 'testClasses',
            group: JavaBasePlugin.VERIFICATION_GROUP,
            description: 'Runs unit tests that are separate'
        ]
        RandomizedTestingTask test = project.tasks.create(testOptions)
        test.configure(BuildPlugin.commonTestConfig(project))
        BuildPlugin.configureCompile(project)
        test.classpath = project.sourceSets.test.runtimeClasspath
        test.testClassesDir project.sourceSets.test.output.classesDir
        test.mustRunAfter(project.precommit)
        project.check.dependsOn(test)
    }
}
