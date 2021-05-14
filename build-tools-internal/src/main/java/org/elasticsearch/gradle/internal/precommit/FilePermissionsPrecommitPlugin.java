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
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.TaskProvider;

import javax.inject.Inject;
import java.util.stream.Collectors;

public class FilePermissionsPrecommitPlugin extends PrecommitPlugin implements InternalPlugin {

    public static final String FILEPERMISSIONS_TASK_NAME = "filepermissions";
    private ProviderFactory providerFactory;

    @Inject
    public FilePermissionsPrecommitPlugin(ProviderFactory providerFactory) {
        this.providerFactory = providerFactory;
    }

    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        return project.getTasks()
            .register(
                FILEPERMISSIONS_TASK_NAME,
                FilePermissionsTask.class,
                filePermissionsTask -> filePermissionsTask.getSources()
                    .addAll(
                        providerFactory.provider(
                            () -> GradleUtils.getJavaSourceSets(project).stream().map(s -> s.getAllSource()).collect(Collectors.toList())
                        )
                    )
            );
    }
}
