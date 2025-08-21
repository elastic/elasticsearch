/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.precommit;

import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin;
import org.elasticsearch.gradle.jarhell.JarHellPlugin;
import org.elasticsearch.gradle.jarhell.JarHellTask;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.tasks.TaskProvider;

public class JarHellPrecommitPlugin extends PrecommitPlugin {
    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        project.getPluginManager().apply(JarHellPlugin.class);

        if (project.getPath().equals(":libs:core") == false) {
            // ideally we would configure this as a default dependency. But Default dependencies do not work correctly
            // with gradle project dependencies as they're resolved to late in the build and don't setup according task
            // dependencies properly
            var elasticsearchCoreProject = project.findProject(":libs:core");
            if (elasticsearchCoreProject != null) {
                project.getDependencies().add("jarHell", elasticsearchCoreProject);
            }
        }

        return project.getTasks().withType(JarHellTask.class).named("jarHell");
    }
}
