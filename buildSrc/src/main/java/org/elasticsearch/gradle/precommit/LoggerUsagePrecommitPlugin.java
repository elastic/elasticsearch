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

package org.elasticsearch.gradle.precommit;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.info.BuildParams;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.tasks.TaskProvider;

public class LoggerUsagePrecommitPlugin extends PrecommitPlugin {
    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        Object dependency = BuildParams.isInternal()
            ? project.project(":test:logger-usage")
            : ("org.elasticsearch.test:logger-usage:" + VersionProperties.getElasticsearch());

        Configuration loggerUsageConfig = project.getConfigurations().create("loggerUsagePlugin");
        project.getDependencies().add("loggerUsagePlugin", dependency);
        TaskProvider<LoggerUsageTask> loggerUsage = project.getTasks().register("loggerUsageCheck", LoggerUsageTask.class);
        loggerUsage.configure(t -> t.setClasspath(loggerUsageConfig));

        return loggerUsage;
    }
}
