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

package org.elasticsearch.gradle;

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
