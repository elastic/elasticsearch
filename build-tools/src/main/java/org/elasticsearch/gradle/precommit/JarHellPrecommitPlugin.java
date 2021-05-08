/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.precommit;

import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;

public class JarHellPrecommitPlugin extends PrecommitPlugin {
    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        Configuration jarHellConfig = project.getConfigurations().create("jarHell");
        TaskProvider<JarHellTask> jarHell = project.getTasks().register("jarHell", JarHellTask.class);
        jarHell.configure(t -> {
            SourceSet testSourceSet = getJavaTestSourceSet(project);
            t.setClasspath(testSourceSet.getRuntimeClasspath());
            t.setJarHellRuntimeClasspath(jarHellConfig);
        });

        return jarHell;
    }

    /**
     * @param project The project to look for test Java resources.
     */
    private static SourceSet getJavaTestSourceSet(Project project) {
        return GradleUtils.getJavaSourceSets(project).findByName(SourceSet.TEST_SOURCE_SET_NAME);
    }
}
