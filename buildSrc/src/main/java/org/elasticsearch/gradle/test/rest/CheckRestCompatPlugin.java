/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.test.rest;

import org.elasticsearch.gradle.ElasticsearchJavaPlugin;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.tasks.TaskProvider;

/**
 * Lifecycle plugin to anchor all Rest compatibility testing to a single command. Usage: `checkRestCompat`
 */
public class CheckRestCompatPlugin implements Plugin<Project> {

    public static final String CHECK_TASK_NAME = "checkRestCompat";

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(ElasticsearchJavaPlugin.class);

        TaskProvider<Task> checkRestCompatTask = project.getTasks().register(CHECK_TASK_NAME, (thisCheckTask) -> {
            thisCheckTask.setDescription("Runs all REST compatibility checks.");
            thisCheckTask.setGroup("verification");
        });

        project.getTasks().named(JavaBasePlugin.CHECK_TASK_NAME).configure(check -> {
            Object bwcEnabled = project.getExtensions().getExtraProperties().getProperties().get("bwc_tests_enabled");
            if (bwcEnabled == null || (Boolean) bwcEnabled) {
                check.dependsOn(checkRestCompatTask);
            }
        });
    }
}
