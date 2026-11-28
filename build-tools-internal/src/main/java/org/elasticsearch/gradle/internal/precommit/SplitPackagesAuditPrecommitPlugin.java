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
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.component.ProjectComponentIdentifier;
import org.gradle.api.artifacts.result.ResolvedArtifactResult;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SplitPackagesAuditPrecommitPlugin extends PrecommitPlugin {
    public static final String TASK_NAME = "splitPackagesAudit";

    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        TaskProvider<SplitPackagesAuditTask> task = project.getTasks().register(TASK_NAME, SplitPackagesAuditTask.class);
        task.configure(t -> {
            Configuration compileClasspath = project.getConfigurations().getByName(JavaPlugin.COMPILE_CLASSPATH_CONFIGURATION_NAME);
            t.setClasspath(compileClasspath);
            // Derive owning projects from the resolved artifacts of this project's own classpath. This names the
            // owner of project dependencies regardless of where their build directory lives (relocated build
            // directories, included builds) without inspecting any other Project instance.
            t.getClasspathProjectOwners()
                .set(
                    compileClasspath.getIncoming()
                        .getArtifacts()
                        .getResolvedArtifacts()
                        .map(SplitPackagesAuditPrecommitPlugin::projectOwnersByFile)
                );
            SourceSet mainSourceSet = GradleUtils.getJavaSourceSets(project).findByName(SourceSet.MAIN_SOURCE_SET_NAME);
            // getSrcDirs() below is fed from every source directory of the main source set, which can
            // include directories produced by other tasks (e.g. the protobuf plugin's
            // processProtoResources generates build/generated/proto-resources/main). Depend on the
            // combined source directories so those generators run first; getSourceDirectories()
            // carries the builtBy task dependencies that Gradle 9.9 requires us to declare explicitly.
            t.dependsOn(mainSourceSet.getAllSource().getSourceDirectories());
            t.getSrcDirs().set(project.provider(() -> mainSourceSet.getAllSource().getSrcDirs()));
        });
        return task;
    }

    private static Map<File, String> projectOwnersByFile(Set<ResolvedArtifactResult> artifacts) {
        Map<File, String> owners = new HashMap<>();
        for (ResolvedArtifactResult artifact : artifacts) {
            if (artifact.getId().getComponentIdentifier() instanceof ProjectComponentIdentifier projectId) {
                owners.put(artifact.getFile(), projectId.getBuildTreePath());
            }
        }
        return owners;
    }
}
