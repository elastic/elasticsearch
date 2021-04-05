/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.precommit;

import org.elasticsearch.gradle.info.BuildParams;
import org.elasticsearch.gradle.util.Util;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;

public class JarHellPrecommitPlugin extends PrecommitPlugin {
    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        Configuration jarHellConfig = project.getConfigurations().create("jarHell");
        if (BuildParams.isInternal() && project.getPath().equals(":libs:elasticsearch-core") == false) {
            // ideally we would configure this as a default dependency. But Default dependencies do not work correctly
            // with gradle project dependencies as they're resolved to late in the build and don't setup according task
            // dependencies properly
            project.getDependencies().add("jarHell", project.project(":libs:elasticsearch-core"));
        }
        TaskProvider<JarHellTask> jarHell = project.getTasks().register("jarHell", JarHellTask.class);
        jarHell.configure(t -> {
            SourceSet testSourceSet = Util.getJavaTestSourceSet(project).get();
            t.setClasspath(testSourceSet.getRuntimeClasspath());
            t.setJarHellRuntimeClasspath(jarHellConfig);
        });

        return jarHell;
    }
}
