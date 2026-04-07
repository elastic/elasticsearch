/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.precommit;

import org.elasticsearch.gradle.internal.ExportElasticsearchBuildResourcesTask;
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin;
import org.elasticsearch.gradle.internal.info.BuildParameterExtension;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.specs.Specs;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;

import java.io.File;
import java.util.Set;

import static de.thetaphi.forbiddenapis.gradle.ForbiddenApisPlugin.FORBIDDEN_APIS_TASK_NAME;
import static org.elasticsearch.gradle.internal.precommit.CheckForbiddenApisTask.BUNDLED_SIGNATURE_DEFAULTS;

public class ForbiddenApisPrecommitPlugin extends PrecommitPlugin {

    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        project.getPluginManager().apply(JavaBasePlugin.class);
        var buildParams = project.getRootProject().getExtensions().getByType(BuildParameterExtension.class);
        // Create a convenience task for all checks (this does not conflict with extension, as it has higher priority in DSL):
        var forbiddenTask = project.getTasks()
            .register(FORBIDDEN_APIS_TASK_NAME, task -> { task.setDescription("Runs forbidden-apis checks."); });

        TaskProvider<ExportElasticsearchBuildResourcesTask> resourcesTask = project.getTasks()
            .register("forbiddenApisResources", ExportElasticsearchBuildResourcesTask.class);
        File resourcesDir = project.getLayout().getBuildDirectory().dir("forbidden-apis-config").get().getAsFile();
        resourcesTask.configure(t -> {
            t.setOutputDir(resourcesDir);
            t.copy("forbidden/jdk-signatures.txt");
            t.copy("forbidden/jdk-deprecated.txt");
            t.copy("forbidden/es-all-signatures.txt");
            t.copy("forbidden/es-test-signatures.txt");
            t.copy("forbidden/http-signatures.txt");
            t.copy("forbidden/es-server-signatures.txt");
        });

        project.getExtensions().getByType(SourceSetContainer.class).configureEach(sourceSet -> {
            String sourceSetTaskName = sourceSet.getTaskName(FORBIDDEN_APIS_TASK_NAME, null);
            var sourceSetTask = project.getTasks().register(sourceSetTaskName, CheckForbiddenApisTask.class, t -> {
                t.setDescription("Runs forbidden-apis checks on '${sourceSet.name}' classes.");
                t.setResourcesDir(resourcesDir);
                t.getOutputs().upToDateWhen(Specs.SATISFIES_ALL);
                t.setClassesDirs(sourceSet.getOutput().getClassesDirs());
                t.dependsOn(resourcesTask);
                t.setClasspath(sourceSet.getRuntimeClasspath().plus(sourceSet.getCompileClasspath()));
                t.setTargetCompatibility(buildParams.getMinimumRuntimeVersion().getMajorVersion());
                t.getBundledSignatures().set(BUNDLED_SIGNATURE_DEFAULTS);
                t.setSignaturesFiles(
                    project.files(
                        resourcesDir.toPath().resolve("forbidden/jdk-signatures.txt"),
                        resourcesDir.toPath().resolve("forbidden/es-all-signatures.txt"),
                        resourcesDir.toPath().resolve("forbidden/jdk-deprecated.txt")
                    )
                );
                t.getSuppressAnnotations().set(Set.of("**.SuppressForbidden"));
                if (t.getName().endsWith("Test")) {
                    t.setSignaturesFiles(
                        t.getSignaturesFiles()
                            .plus(
                                project.files(
                                    resourcesDir.toPath().resolve("forbidden/es-test-signatures.txt"),
                                    resourcesDir.toPath().resolve("forbidden/http-signatures.txt")
                                )
                            )
                    );
                } else {
                    t.setSignaturesFiles(
                        t.getSignaturesFiles().plus(project.files(resourcesDir.toPath().resolve("forbidden/es-server-signatures.txt")))
                    );
                }
            });
            forbiddenTask.configure(t -> t.dependsOn(sourceSetTask));
        });
        return forbiddenTask;
    }
}
