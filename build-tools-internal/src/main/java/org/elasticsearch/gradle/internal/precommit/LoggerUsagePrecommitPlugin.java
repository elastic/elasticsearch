/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit;

import org.elasticsearch.gradle.internal.InternalPlugin;
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.tasks.TaskProvider;

public class LoggerUsagePrecommitPlugin extends PrecommitPlugin implements InternalPlugin {
    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        Configuration loggerUsageConfig = project.getConfigurations().create("loggerUsagePlugin");
        // this makes it easier to test by not requiring this project to be always available in our
        // test sample projects
        if (project.findProject(":test:logger-usage") != null) {
            project.getDependencies().add("loggerUsagePlugin", project.project(":test:logger-usage"));
        }
        TaskProvider<LoggerUsageTask> loggerUsage = project.getTasks().register("loggerUsageCheck", LoggerUsageTask.class);
        loggerUsage.configure(t -> t.setClasspath(loggerUsageConfig));
        return loggerUsage;
    }
}
