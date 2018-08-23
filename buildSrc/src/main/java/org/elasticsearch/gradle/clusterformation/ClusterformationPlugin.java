/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.gradle.clusterformation;

import groovy.lang.Closure;
import org.elasticsearch.GradleServicesAdapter;
import org.elasticsearch.gradle.Distribution;
import org.elasticsearch.gradle.Version;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.execution.TaskActionListener;
import org.gradle.api.execution.TaskExecutionListener;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.tasks.Sync;
import org.gradle.api.tasks.TaskState;

import java.io.File;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClusterformationPlugin implements Plugin<Project> {

    private static final String LIST_TASK_NAME = "listElasticSearchClusters";
    private static final String NODE_EXTENSION_NAME = "elasticsearchNodes";

    private static final String HELPER_CONFIGURATION_NAME = "_internalClusterFormationConfiguration";
    private static final String SYNC_ARTIFACTS_TASK_NAME = "syncClusterFormationArtifacts";

    private final Logger logger =  Logging.getLogger(ClusterformationPlugin.class);

    @Override
    public void apply(Project project) {
        Project rootProject = project.getRootProject();

        // Create an extensions that allows describing clusters
        NamedDomainObjectContainer<? extends ElasticsearchConfigurationInternal> container = project.container(
            ElasticsearchNode.class,
            name -> new ElasticsearchNode(
                name,
                GradleServicesAdapter.getInstance(project),
                getArtifactsDir(project),
                new File(project.getBuildDir(), NODE_EXTENSION_NAME)
            )
        );
        project.getExtensions().add(NODE_EXTENSION_NAME, container);

        // Utility task to list all clusters defined
        Task listTask = project.getTasks().create(LIST_TASK_NAME);
        listTask.setGroup("ES cluster formation");
        listTask.setDescription("Lists all ES clusters configured for this project");
        listTask.doLast((Task task) ->
            container.forEach((ElasticsearchConfiguration cluster) ->
                logger.lifecycle("   * {}: {}", cluster.getName(), cluster.getDistribution())
            )
        );

        Map<Task, List<ElasticsearchConfigurationInternal>> taskToCluster = new HashMap<>();

        // register an extension for all current and future tasks, so that any task can declare that it wants to use a
        // specific cluster.
        project.getTasks().all((Task task) ->
            task.getExtensions().findByType(ExtraPropertiesExtension.class)
                .set(
                    "useCluster",
                    new Closure<Void>(this, task) {
                        public void doCall(ElasticsearchConfiguration conf) {
                            taskToCluster.computeIfAbsent(task, k -> new ArrayList<>()).add(
                                (ElasticsearchConfigurationInternal) conf
                            );
                            Object thisObject = this.getThisObject();
                            if (thisObject instanceof Task == false) {
                                throw new AssertionError("Expected " + thisObject + " to be an instance of " +
                                    "Task, but got: " + thisObject.getClass());
                            }
                            ((Task) thisObject).dependsOn(rootProject.getTasks().getByName(SYNC_ARTIFACTS_TASK_NAME));
                        }
                    })
        );

        // When the project evaluated we know of all tasks that use clusters.
        // Each of these have to depend on the artifacts being synced.
        project.afterEvaluate(ip -> {
            container.forEach(esConfig -> {
                // declare dependencies against artifacts needed by cluster formation.
                // this causes a download if the  artifacts are not in the Gradle cache, but that is no different than Gradle
                // dependencies in general
                esConfig.assertValid();
                String dependency = MessageFormat.format(
                    "org.elasticsearch.distribution.{0}:{1}:{2}@{0}",
                    esConfig.getDistribution().getExtension(),
                    esConfig.getDistribution().getFileName(),
                    esConfig.getVersion()
                );
                logger.info("Cluster {} depends on {}", esConfig.getName(), dependency);
                rootProject.getDependencies().add(HELPER_CONFIGURATION_NAME, dependency);
            });
        });

        // Have a single common location to set up the required artifacts
        if (rootProject.getConfigurations().findByName(HELPER_CONFIGURATION_NAME) == null) {
            Configuration helperConfiguration = rootProject.getConfigurations().create(HELPER_CONFIGURATION_NAME);
            helperConfiguration.setDescription(
                "Internal helper configuration used by cluster configuration to download " +
                    "ES distributions and plugins."
            );

            Sync syncTask = rootProject.getTasks().create(SYNC_ARTIFACTS_TASK_NAME, Sync.class);
            syncTask.from(helperConfiguration);
            syncTask.into(getArtifactsDir(rootProject));

            project.getGradle().getTaskGraph().whenReady(taskExecutionGraph ->
                taskExecutionGraph.getAllTasks()
                    .forEach(task ->
                        taskToCluster.getOrDefault(task, Collections.emptyList()).forEach(ElasticsearchConfigurationInternal::claim)
                    )
            );
            project.getGradle().addListener(
                new TaskActionListener() {
                    @Override
                    public void beforeActions(Task task) {
                        // we only start the cluster before the actions, so we'll not start it if the task is up-to-date
                        taskToCluster.getOrDefault(task, Collections.emptyList()).forEach(ElasticsearchConfigurationInternal::start);
                    }
                    @Override
                    public void afterActions(Task task) {}
                }
            );
            project.getGradle().addListener(
                new TaskExecutionListener() {
                    @Override
                    public void afterExecute(Task task, TaskState state) {
                        // always unclaim the cluster, even if _this_ task is up-to-date, as others might not have been
                        // and caused the cluster to start.
                        if (state.getFailure() != null) {
                            // If the task fails, and other tasks use this cluster, the other task will likely never be
                            // executed at all, so we will never get to un-claim and terminate it.
                            // The downside is that with multi project builds if that other  task is in a different
                            // project and executing right now, we may terminate the cluster while it's running it.
                            taskToCluster.getOrDefault(task, Collections.emptyList()).forEach(
                                ElasticsearchConfigurationInternal::forceStop
                            );
                        } else {
                            taskToCluster.getOrDefault(task, Collections.emptyList()).forEach(
                                ElasticsearchConfigurationInternal::unClaimAndStop
                            );
                        }
                    }
                    @Override
                    public void beforeExecute(Task task) {}
                }
            );
        }
    }

    private static File getArtifactsDir(Project project) {
        return new File(project.getRootProject().getBuildDir(), "clusterformation-artifacts");
    }

    static File getArtifact(File sharedArtifactsDir, Distribution distro, Version version) {
        return new File(sharedArtifactsDir, distro.getFileName() + "-" + version + "." + distro.getExtension());
    }

    @SuppressWarnings("unchecked")
    public static NamedDomainObjectContainer<ElasticsearchConfiguration> getNodeExtension(Project project) {
        return (NamedDomainObjectContainer<ElasticsearchConfiguration>)
            project.getExtensions().getByName(NODE_EXTENSION_NAME);
    }

}
