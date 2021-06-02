/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit;

import de.thetaphi.forbiddenapis.gradle.CheckForbiddenApis;
import de.thetaphi.forbiddenapis.gradle.ForbiddenApisPlugin;
import groovy.lang.Closure;
import org.elasticsearch.gradle.internal.ExportElasticsearchBuildResourcesTask;
import org.elasticsearch.gradle.internal.info.BuildParams;
import org.elasticsearch.gradle.internal.InternalPlugin;
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.JavaVersion;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ForbiddenApisPrecommitPlugin extends PrecommitPlugin implements InternalPlugin {
    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        project.getPluginManager().apply(ForbiddenApisPlugin.class);

        TaskProvider<ExportElasticsearchBuildResourcesTask> resourcesTask = project.getTasks()
            .register("forbiddenApisResources", ExportElasticsearchBuildResourcesTask.class);
        Path resourcesDir = project.getBuildDir().toPath().resolve("forbidden-apis-config");
        resourcesTask.configure(t -> {
            t.setOutputDir(resourcesDir.toFile());
            t.copy("forbidden/jdk-signatures.txt");
            t.copy("forbidden/es-all-signatures.txt");
            t.copy("forbidden/es-test-signatures.txt");
            t.copy("forbidden/http-signatures.txt");
            t.copy("forbidden/es-server-signatures.txt");
            t.copy("forbidden/snakeyaml-signatures.txt");
        });
        project.getTasks().withType(CheckForbiddenApis.class).configureEach(t -> {
            t.dependsOn(resourcesTask);

            assert t.getName().startsWith(ForbiddenApisPlugin.FORBIDDEN_APIS_TASK_NAME);
            String sourceSetName;
            if (ForbiddenApisPlugin.FORBIDDEN_APIS_TASK_NAME.equals(t.getName())) {
                sourceSetName = "main";
            } else {
                // parse out the sourceSetName
                char[] chars = t.getName().substring(ForbiddenApisPlugin.FORBIDDEN_APIS_TASK_NAME.length()).toCharArray();
                chars[0] = Character.toLowerCase(chars[0]);
                sourceSetName = new String(chars);
            }

            SourceSetContainer sourceSets = GradleUtils.getJavaSourceSets(project);
            SourceSet sourceSet = sourceSets.getByName(sourceSetName);
            t.setClasspath(project.files(sourceSet.getRuntimeClasspath()).plus(sourceSet.getCompileClasspath()));

            t.setTargetCompatibility(BuildParams.getRuntimeJavaVersion().getMajorVersion());
            if (BuildParams.getRuntimeJavaVersion().compareTo(JavaVersion.VERSION_14) > 0) {
                // TODO: forbidden apis does not yet support java 15, rethink using runtime version
                t.setTargetCompatibility(JavaVersion.VERSION_14.getMajorVersion());
            }
            t.setBundledSignatures(Set.of("jdk-unsafe", "jdk-deprecated", "jdk-non-portable", "jdk-system-out"));
            t.setSignaturesFiles(
                project.files(resourcesDir.resolve("forbidden/jdk-signatures.txt"), resourcesDir.resolve("forbidden/es-all-signatures.txt"))
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
        TaskProvider<Task> forbiddenApis = project.getTasks().named("forbiddenApis");
        forbiddenApis.configure(t -> t.setGroup(""));
        return forbiddenApis;
    }
}
