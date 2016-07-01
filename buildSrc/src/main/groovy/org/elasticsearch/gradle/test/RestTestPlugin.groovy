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

import org.gradle.api.Plugin
import org.gradle.api.Project

/** A plugin to add rest integration tests. Used for qa projects.  */
public class RestTestPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.pluginManager.apply(StandaloneTestBasePlugin)

        RestIntegTestTask integTest = project.tasks.create('integTest', RestIntegTestTask.class)
        integTest.cluster.distribution = 'zip' // rest tests should run with the real zip
        integTest.mustRunAfter(project.precommit)
        project.check.dependsOn(integTest)
    }
}
