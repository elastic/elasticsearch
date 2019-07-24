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
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaBasePlugin
import org.gradle.api.tasks.testing.Test

/**
 * Configures the build to compile against Elasticsearch's test framework and
 * run integration and unit tests. Use BuildPlugin if you want to build main
 * code as well as tests. */
@CompileStatic
class StandaloneTestPlugin implements Plugin<Project> {

    @Override
    void apply(Project project) {
        project.pluginManager.apply(StandaloneRestTestPlugin)

        Test test = project.tasks.create('test', Test)
        test.group = JavaBasePlugin.VERIFICATION_GROUP
        test.description = 'Runs unit tests that are separate'

        BuildPlugin.configureCompile(project)
        test.mustRunAfter(project.tasks.getByName('precommit'))
        project.tasks.getByName('check').dependsOn(test)
    }
}
