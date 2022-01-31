/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.modules;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetOutput;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Jar;

import java.util.Set;

public class JavaModulesPlugin implements Plugin<Project> {
    @Override
    public void apply(Project project) {
        project.getPluginManager().withPlugin("java-base", p -> {
            project.getConfigurations().register("moduleApi");

            SourceSet main = project.getExtensions().getByType(JavaPluginExtension.class).getSourceSets().getByName("main");
            SourceSetOutput mainOutput = main.getOutput();
            TaskProvider<ReadModuleExports> exportModuleInfo = project.getTasks().register("exportModuleInfo", ReadModuleExports.class);
            exportModuleInfo.configure(e -> { e.setClassFiles(mainOutput.getClassesDirs()); });

            TaskProvider<Jar> moduleApiJar = project.getTasks().register("modulesApiJar", Jar.class, t -> {
                t.dependsOn(exportModuleInfo);
                t.getArchiveAppendix().set("api");
                t.from(mainOutput);
                t.include(e -> {
                    Set<String> exports = exportModuleInfo.get().getExports();
                    String path = e.getRelativePath().getPathString();
                    System.out.println("Checking: " + path);
                    if (path.endsWith(".class")) {
                        int lastSlash = path.lastIndexOf('/');
                        if (lastSlash == -1) {
                            lastSlash = 0;
                        }
                        String packagePath = path.substring(0, lastSlash);
                        System.out.println("Package: " + packagePath);
                        if (exports.contains(packagePath) == false) {
                            System.out.println("Filtering: " + path);
                            return false;
                        }
                    }
                    return true;
                });
            });
            project.getArtifacts().add("moduleApi", moduleApiJar);
        });
    }
}
