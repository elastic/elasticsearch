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
import org.gradle.api.plugins.JavaBasePlugin

/** A plugin to test command line options like --version etc. */
public class CommandLineTestPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        // is where "check" comes from
        project.pluginManager.apply(JavaBasePlugin)
        CommandLineTestTask integTest = project.tasks.create('commandLineTest', CommandLineTestTask.class)
        project.check.dependsOn(integTest)
    }
}
