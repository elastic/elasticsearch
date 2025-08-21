/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.docker;

import org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.provider.Provider;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.gradle.internal.util.ParamsUtils.loadBuildParams;

/**
 * Plugin providing {@link DockerSupportService} for detecting Docker installations and determining requirements for Docker-based
 * Elasticsearch build tasks.
 */
public class DockerSupportPlugin implements Plugin<Project> {
    public static final String DOCKER_SUPPORT_SERVICE_NAME = "dockerSupportService";
    public static final String DOCKER_ON_LINUX_EXCLUSIONS_FILE = ".ci/dockerOnLinuxExclusions";

    @Override
    public void apply(Project project) {
        if (project != project.getRootProject()) {
            throw new IllegalStateException(this.getClass().getName() + " can only be applied to the root project.");
        }
        project.getPlugins().apply(GlobalBuildInfoPlugin.class);
        var buildParams = loadBuildParams(project).get();

        Provider<DockerSupportService> dockerSupportServiceProvider = project.getGradle()
            .getSharedServices()
            .registerIfAbsent(DOCKER_SUPPORT_SERVICE_NAME, DockerSupportService.class, spec -> spec.parameters(params -> {
                params.setExclusionsFile(new File(project.getRootDir(), DOCKER_ON_LINUX_EXCLUSIONS_FILE));
                params.getIsCI().set(buildParams.getCi());
            }));

        // Ensure that if we are trying to run any DockerBuildTask tasks, we assert an available Docker installation exists
        project.getGradle().getTaskGraph().whenReady(graph -> {
            List<String> dockerTasks = graph.getAllTasks()
                .stream()
                .filter(task -> task instanceof DockerBuildTask)
                .map(Task::getPath)
                .collect(Collectors.toList());

            if (dockerTasks.isEmpty() == false) {
                dockerSupportServiceProvider.get().failIfDockerUnavailable(dockerTasks);
            }
        });
    }

}
