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
import org.elasticsearch.gradle.testclusters.TestClustersPlugin
import org.gradle.api.InvalidUserDataException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaBasePlugin

/**
 * Adds support for starting an Elasticsearch cluster before running integration
 * tests. Used in conjunction with {@link StandaloneRestTestPlugin} for qa
 * projects and in conjunction with {@link BuildPlugin} for testing the rest
 * client.
 */
@CompileStatic
class RestTestPlugin implements Plugin<Project> {
    List<String> REQUIRED_PLUGINS = [
        'elasticsearch.build',
        'elasticsearch.standalone-rest-test']

    @Override
    void apply(Project project) {
        if (false == REQUIRED_PLUGINS.any { project.pluginManager.hasPlugin(it) }) {
            throw new InvalidUserDataException('elasticsearch.rest-test '
                + 'requires either elasticsearch.build or '
                + 'elasticsearch.standalone-rest-test')
        }
        project.getPlugins().apply(RestTestBasePlugin.class);
        project.pluginManager.apply(TestClustersPlugin)
        RestIntegTestTask integTest = project.tasks.create('integTest', RestIntegTestTask.class)
        integTest.description = 'Runs rest tests against an elasticsearch cluster.'
        integTest.group = JavaBasePlugin.VERIFICATION_GROUP
        integTest.mustRunAfter(project.tasks.named('precommit'))
        project.tasks.named('check').configure { it.dependsOn(integTest) }
    }
}
