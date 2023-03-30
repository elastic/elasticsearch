/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit;

import de.thetaphi.forbiddenapis.gradle.CheckForbiddenApisExtension;
import groovy.lang.Closure;

import org.elasticsearch.gradle.internal.ExportElasticsearchBuildResourcesTask;
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin;
import org.elasticsearch.gradle.internal.info.BuildParams;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.specs.Specs;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static de.thetaphi.forbiddenapis.gradle.ForbiddenApisPlugin.FORBIDDEN_APIS_EXTENSION_NAME;
import static de.thetaphi.forbiddenapis.gradle.ForbiddenApisPlugin.FORBIDDEN_APIS_TASK_NAME;

public class ForbiddenApisPrecommitPlugin extends PrecommitPlugin {
    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        project.getPluginManager().apply(JavaBasePlugin.class);

        // create Extension for defaults:
        var checkForbiddenApisExtension = project.getExtensions()
            .create(FORBIDDEN_APIS_EXTENSION_NAME, CheckForbiddenApisExtension.class, project);

        // Create a convenience task for all checks (this does not conflict with extension, as it has higher priority in DSL):
        var forbiddenTask = project.getTasks()
            .register(FORBIDDEN_APIS_TASK_NAME, task -> { task.setDescription("Runs forbidden-apis checks."); });

        JavaPluginExtension javaPluginExtension = project.getExtensions().getByType(JavaPluginExtension.class);
        // Define our tasks (one for each SourceSet):

        TaskProvider<ExportElasticsearchBuildResourcesTask> resourcesTask = project.getTasks()
            .register("forbiddenApisResources", ExportElasticsearchBuildResourcesTask.class);
        Path resourcesDir = project.getBuildDir().toPath().resolve("forbidden-apis-config");
        resourcesTask.configure(t -> {
            t.setOutputDir(resourcesDir.toFile());
            t.copy("forbidden/jdk-signatures.txt");
            t.copy("forbidden/jdk-deprecated.txt");
            t.copy("forbidden/es-all-signatures.txt");
            t.copy("forbidden/es-test-signatures.txt");
            t.copy("forbidden/hppc-signatures.txt");
            t.copy("forbidden/http-signatures.txt");
            t.copy("forbidden/es-server-signatures.txt");
        });

        project.getExtensions().getByType(SourceSetContainer.class).configureEach(sourceSet -> {
            String sourceSetTaskName = sourceSet.getTaskName(FORBIDDEN_APIS_TASK_NAME, null);
            var sourceSetTask = project.getTasks().register(sourceSetTaskName, CheckForbiddenApisTask.class, t -> {
                t.setDescription("Runs forbidden-apis checks on '${sourceSet.name}' classes.");
                t.getOutputs().upToDateWhen(Specs.SATISFIES_ALL);
                t.setClassesDirs(sourceSet.getOutput().getClassesDirs());
                t.dependsOn(resourcesTask);
                t.setClasspath(sourceSet.getRuntimeClasspath().plus(sourceSet.getCompileClasspath()).plus(sourceSet.getOutput()));
                t.setTargetCompatibility(BuildParams.getMinimumRuntimeVersion().getMajorVersion());
                t.setBundledSignatures(Set.of("jdk-unsafe", "jdk-non-portable", "jdk-system-out"));
                t.setSignaturesFiles(
                    project.files(
                        resourcesDir.resolve("forbidden/jdk-signatures.txt"),
                        resourcesDir.resolve("forbidden/es-all-signatures.txt"),
                        resourcesDir.resolve("forbidden/jdk-deprecated.txt")
                    )
                );
                t.setSuppressAnnotations(Set.of("**.SuppressForbidden"));
                if (t.getName().endsWith("Test")) {
                    t.setSignaturesFiles(
                        t.getSignaturesFiles()
                            .plus(
                                project.files(
                                    resourcesDir.resolve("forbidden/es-test-signatures.txt"),
                                    resourcesDir.resolve("forbidden/http-signatures.txt")
                                )
                            )
                    );
                } else {
                    t.setSignaturesFiles(
                        t.getSignaturesFiles().plus(project.files(resourcesDir.resolve("forbidden/es-server-signatures.txt")))
                    );
                }
                ExtraPropertiesExtension ext = t.getExtensions().getExtraProperties();
                ext.set("replaceSignatureFiles", new Closure<Void>(t) {
                    @Override
                    public Void call(Object... names) {
                        List<Path> resources = new ArrayList<>(names.length);
                        for (Object name : names) {
                            resources.add(resourcesDir.resolve("forbidden/" + name + ".txt"));
                        }
                        t.setSignaturesFiles(project.files(resources));
                        return null;
                    }

                });
                ext.set("addSignatureFiles", new Closure<Void>(t) {
                    @Override
                    public Void call(Object... names) {
                        List<Path> resources = new ArrayList<>(names.length);
                        for (Object name : names) {
                            resources.add(resourcesDir.resolve("forbidden/" + name + ".txt"));
                        }
                        t.setSignaturesFiles(t.getSignaturesFiles().plus(project.files(resources)));
                        return null;
                    }
                });

            });
            forbiddenTask.configure(t -> t.dependsOn(sourceSetTask));
        });
        return forbiddenTask;
    }
}
