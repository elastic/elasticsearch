/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.docker;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.provider.Provider;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import static org.elasticsearch.gradle.internal.util.ParamsUtils.applyGlobalBuildInfoIfRoot;
import static org.elasticsearch.gradle.internal.util.ParamsUtils.loadBuildParams;

/**
 * Plugin providing {@link DockerSupportService} for detecting Docker installations and determining requirements for Docker-based
 * Elasticsearch build tasks.
 * <p>
 * The service is shared per build, so the plugin is applied to each project that needs it rather than to the root project.
 */
public class DockerSupportPlugin implements Plugin<Project> {
    public static final String DOCKER_SUPPORT_SERVICE_NAME = "dockerSupportService";
    public static final String DOCKER_ON_LINUX_EXCLUSIONS_FILE = ".ci/dockerOnLinuxExclusions";

    private final ProjectLayout projectLayout;

    @Inject
    public DockerSupportPlugin(ProjectLayout projectLayout) {
        this.projectLayout = projectLayout;
    }

    @Override
    public void apply(Project project) {
        applyGlobalBuildInfoIfRoot(project);
        var buildParams = loadBuildParams(project).get();

        Provider<DockerSupportService> dockerSupportServiceProvider = project.getGradle()
            .getSharedServices()
            .registerIfAbsent(DOCKER_SUPPORT_SERVICE_NAME, DockerSupportService.class, spec -> spec.parameters(params -> {
                params.setExclusionsFile(projectLayout.getSettingsDirectory().file(DOCKER_ON_LINUX_EXCLUSIONS_FILE).getAsFile());
                params.getIsCI().set(buildParams.getCi());
            }));

        // Every task that ends up in the task graph has been realized, so tracking realized tasks avoids realizing
        // the ones that are not going to run.
        List<DockerBuildTask> realizedDockerTasks = new ArrayList<>();
        project.getTasks().withType(DockerBuildTask.class).configureEach(realizedDockerTasks::add);

        // Ensure that if we are trying to run Docker build tasks, we assert an available Docker installation exists.
        // Only this project's tasks are inspected; looking at tasks of other projects is forbidden by Isolated Projects.
        project.getGradle().getTaskGraph().whenReady(graph -> {
            List<String> dockerTasks = realizedDockerTasks.stream().filter(graph::hasTask).map(Task::getPath).toList();
            if (dockerTasks.isEmpty() == false) {
                dockerSupportServiceProvider.get().failIfDockerUnavailable(dockerTasks);
            }
        });
    }

}
