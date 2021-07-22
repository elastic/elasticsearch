/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.TaskProvider;

import java.util.List;
import java.util.stream.Collectors;

public class DependenciesGraphPlugin implements Plugin<Project> {

    public void apply(Project project) {
        project.getRootProject().getPluginManager().apply(DependenciesGraphHookPlugin.class);
        final String url = System.getenv("SCA_URL");
        final String token = System.getenv("SCA_TOKEN");
        TaskProvider<DependenciesGraphTask> depsGraph = project.getTasks().register("dependenciesGraph", DependenciesGraphTask.class);
        depsGraph.configure(t -> {
            project.getPlugins().withType(JavaPlugin.class, p -> {
                t.setRuntimeConfiguration(project.getConfigurations().getByName(JavaPlugin.RUNTIME_CLASSPATH_CONFIGURATION_NAME));
                t.setToken(token);
                t.setUrl(url);
            });
        });
    }

    static class DependenciesGraphHookPlugin implements Plugin<Project> {

        @Override
        public void apply(Project project) {
            if (project != project.getRootProject()) {
                throw new IllegalStateException(this.getClass().getName() + " can only be applied to the root project.");
            }
            final String url = System.getenv("SCA_URL");
            final String token = System.getenv("SCA_TOKEN");
            project.getGradle().getTaskGraph().whenReady(graph -> {
                List<String> depGraphTasks = graph.getAllTasks()
                    .stream()
                    .filter(t -> t instanceof DependenciesGraphTask)
                    .map(Task::getPath)
                    .collect(Collectors.toList());
                if (depGraphTasks.size() > 0) {
                    if (url == null || token == null) {
                        // If there are more than one DependenciesGraphTasks to run, print the message only for one of
                        // them as the resolving action is the same for all
                        throw new GradleException(
                            "The environment variables SCA_URL and SCA_TOKEN need to be set before task "
                                + depGraphTasks.get(0)
                                + " can run"
                        );
                    }
                }
            });

        }
    }
}
