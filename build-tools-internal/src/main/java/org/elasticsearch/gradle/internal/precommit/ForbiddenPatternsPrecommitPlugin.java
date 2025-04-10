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
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.TaskProvider;

import javax.inject.Inject;

public class ForbiddenPatternsPrecommitPlugin extends PrecommitPlugin {

    public static final String FORBIDDEN_PATTERNS_TASK_NAME = "forbiddenPatterns";
    private final ProviderFactory providerFactory;

    @Inject
    public ForbiddenPatternsPrecommitPlugin(ProviderFactory providerFactory) {
        this.providerFactory = providerFactory;
    }

    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        return project.getTasks().register(FORBIDDEN_PATTERNS_TASK_NAME, ForbiddenPatternsTask.class, forbiddenPatternsTask -> {
            GradleUtils.getJavaSourceSets(project).configureEach(sourceSet -> {
                forbiddenPatternsTask.getSourceFolders().add(sourceSet.getAllSource());
                forbiddenPatternsTask.dependsOn(sourceSet.getProcessResourcesTaskName());
            });
            forbiddenPatternsTask.getRootDir().set(project.getRootDir());
        });
    }
}
