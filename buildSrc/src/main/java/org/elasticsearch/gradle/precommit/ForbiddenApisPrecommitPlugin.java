/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.precommit;

import de.thetaphi.forbiddenapis.gradle.CheckForbiddenApis;
import de.thetaphi.forbiddenapis.gradle.ForbiddenApisPlugin;
import groovy.lang.Closure;
import org.elasticsearch.gradle.ExportElasticsearchBuildResourcesTask;
import org.elasticsearch.gradle.info.BuildParams;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.JavaVersion;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ForbiddenApisPrecommitPlugin extends PrecommitPlugin {
    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        project.getPluginManager().apply(ForbiddenApisPlugin.class);

        TaskProvider<ExportElasticsearchBuildResourcesTask> buildResources = project.getTasks()
            .named("buildResources", ExportElasticsearchBuildResourcesTask.class);
        project.getTasks().withType(CheckForbiddenApis.class).configureEach(t -> {
            ExportElasticsearchBuildResourcesTask buildResourcesTask = buildResources.get();
            t.dependsOn(buildResources);

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
                project.files(
                    buildResourcesTask.copy("forbidden/jdk-signatures.txt"),
                    buildResourcesTask.copy("forbidden/es-all-signatures.txt")
                )
            );
            t.setSuppressAnnotations(Set.of("**.SuppressForbidden"));
            if (t.getName().endsWith("Test")) {
                t.setSignaturesFiles(
                    t.getSignaturesFiles()
                        .plus(
                            project.files(
                                buildResourcesTask.copy("forbidden/es-test-signatures.txt"),
                                buildResourcesTask.copy("forbidden/http-signatures.txt")
                            )
                        )
                );
            } else {
                t.setSignaturesFiles(
                    t.getSignaturesFiles().plus(project.files(buildResourcesTask.copy("forbidden/es-server-signatures.txt")))
                );
            }
            ExtraPropertiesExtension ext = t.getExtensions().getExtraProperties();
            ext.set("replaceSignatureFiles", new Closure<Void>(t) {
                @Override
                public Void call(Object... names) {
                    List<File> resources = new ArrayList<>(names.length);
                    for (Object name : names) {
                        resources.add(buildResourcesTask.copy("forbidden/" + name + ".txt"));
                    }
                    t.setSignaturesFiles(project.files(resources));
                    return null;
                }

            });
            ext.set("addSignatureFiles", new Closure<Void>(t) {
                @Override
                public Void call(Object... names) {
                    List<File> resources = new ArrayList<>(names.length);
                    for (Object name : names) {
                        resources.add(buildResourcesTask.copy("forbidden/" + name + ".txt"));
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
