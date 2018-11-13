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
package org.elasticsearch.gradle.testclusters;

import groovy.lang.Closure;
import org.elasticsearch.GradleServicesAdapter;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.execution.TaskActionListener;
import org.gradle.api.execution.TaskExecutionListener;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.tasks.TaskState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class TestClustersPlugin implements Plugin<Project> {

    private static final String LIST_TASK_NAME = "listTestClusters";
    private static final String NODE_EXTENSION_NAME = "testClusters";
    public static final String PROPERTY_TESTCLUSTERS_RUN_ONCE = "_testclusters_run_once";

    private final Logger logger =  Logging.getLogger(TestClustersPlugin.class);

    // this is static because we need a single mapping across multi project builds, as some of the listeners we use,
    // like task graph are singletons across multi project builds.
    private static final Map<Task, List<ElasticsearchNode>> usedClusters = new ConcurrentHashMap<>();
    private static final Map<ElasticsearchNode, Integer> claimsInventory = new ConcurrentHashMap<>();
    private static final Set<ElasticsearchNode> runningClusters = Collections.synchronizedSet(new HashSet<>());

    @Override
    public void apply(Project project) {
        // enable the DSL to describe clusters
        NamedDomainObjectContainer<ElasticsearchNode> container = createTestClustersContainerExtension(project);

        // provide a task to be able to list defined clusters.
        createListClustersTask(project, container);

        // create DSL for tasks to mark clusters these use
        createUseClusterTaskExtension(project);

        // There's a single Gradle instance for multi project builds, this means that some configuration needs to be
        // done only once even if the plugin is applied multiple times as a part of multi project build
        ExtraPropertiesExtension rootProperties = project.getRootProject().getExtensions().getExtraProperties();
        if (rootProperties.has(PROPERTY_TESTCLUSTERS_RUN_ONCE) == false) {
            rootProperties.set(PROPERTY_TESTCLUSTERS_RUN_ONCE, true);
            // When running in the Daemon it's possible for this to hold references to past
            usedClusters.clear();
            claimsInventory.clear();
            runningClusters.clear();

            // When we know what tasks will run, we claim the clusters of those task to differentiate between clusters
            // that are defined in the build script and the ones that will actually be used in this invocation of gradle
            // we use this information to determine when the last task that required the cluster executed so that we can
            // terminate the cluster right away and free up resources.
            configureClaimClustersHook(project);

            // Before each task, we determine if a cluster needs to be started for that task.
            configureStartClustersHook(project);

            // After each task we determine if there are clusters that are no longer needed.
            configureStopClustersHook(project);
        }
    }

    private NamedDomainObjectContainer<ElasticsearchNode> createTestClustersContainerExtension(Project project) {
        // Create an extensions that allows describing clusters
        NamedDomainObjectContainer<ElasticsearchNode> container = project.container(
            ElasticsearchNode.class,
            name -> new ElasticsearchNode(
                name,
                GradleServicesAdapter.getInstance(project)
            )
        );
        project.getExtensions().add(NODE_EXTENSION_NAME, container);
        return container;
    }


    private void createListClustersTask(Project project, NamedDomainObjectContainer<ElasticsearchNode> container) {
        Task listTask = project.getTasks().create(LIST_TASK_NAME);
        listTask.setGroup("ES cluster formation");
        listTask.setDescription("Lists all ES clusters configured for this project");
        listTask.doLast((Task task) ->
            container.forEach(cluster ->
                logger.lifecycle("   * {}: {}", cluster.getName(), cluster.getDistribution())
            )
        );
    }

    private void createUseClusterTaskExtension(Project project) {
        // register an extension for all current and future tasks, so that any task can declare that it wants to use a
        // specific cluster.
        project.getTasks().all((Task task) ->
            task.getExtensions().findByType(ExtraPropertiesExtension.class)
                .set(
                    "useCluster",
                    new Closure<Void>(this, task) {
                        public void doCall(ElasticsearchNode node) {
                            usedClusters.computeIfAbsent(task, k -> new ArrayList<>()).add(node);
                        }
                    })
        );
    }

    private void configureClaimClustersHook(Project project) {
        project.getGradle().getTaskGraph().whenReady(taskExecutionGraph ->
            taskExecutionGraph.getAllTasks()
                .forEach(task ->
                    usedClusters.getOrDefault(task, Collections.emptyList()).forEach(each -> {
                        synchronized (claimsInventory) {
                            claimsInventory.put(each, claimsInventory.getOrDefault(each, 0) + 1);
                        }
                        each.freeze();
                    })
                )
        );
    }

    private void configureStartClustersHook(Project project) {
        project.getGradle().addListener(
            new TaskActionListener() {
                @Override
                public void beforeActions(Task task) {
                    // we only start the cluster before the actions, so we'll not start it if the task is up-to-date
                    final List<ElasticsearchNode> clustersToStart;
                    synchronized (runningClusters) {
                        clustersToStart = usedClusters.getOrDefault(task,Collections.emptyList()).stream()
                            .filter(each -> runningClusters.contains(each) == false)
                            .collect(Collectors.toList());
                        runningClusters.addAll(clustersToStart);
                    }
                    clustersToStart.forEach(ElasticsearchNode::start);

                }
                @Override
                public void afterActions(Task task) {}
            }
        );
    }

    private void configureStopClustersHook(Project project) {
        project.getGradle().addListener(
            new TaskExecutionListener() {
                @Override
                public void afterExecute(Task task, TaskState state) {
                    // always unclaim the cluster, even if _this_ task is up-to-date, as others might not have been
                    // and caused the cluster to start.
                    List<ElasticsearchNode> clustersUsedByTask = usedClusters.getOrDefault(
                        task,
                        Collections.emptyList()
                    );
                    if (state.getFailure() != null) {
                        // If the task fails, and other tasks use this cluster, the other task will likely never be
                        // executed at all, so we will never get to un-claim and terminate it.
                        // The downside is that with multi project builds if that other  task is in a different
                        // project and executing right now, we may terminate the cluster while it's running it.
                        clustersUsedByTask.forEach(each -> each.stop(true));
                    } else {
                        clustersUsedByTask.forEach(each -> {
                            synchronized (claimsInventory) {
                                claimsInventory.put(each, claimsInventory.get(each) - 1);
                            }
                        });
                        final List<ElasticsearchNode> stoppable;
                        synchronized (runningClusters) {
                            stoppable = claimsInventory.entrySet().stream()
                                .filter(entry -> entry.getValue() == 0)
                                .filter(entry -> runningClusters.contains(entry.getKey()))
                                .map(Map.Entry::getKey)
                                .collect(Collectors.toList());
                        }
                        stoppable.forEach(each -> each.stop(false));
                    }
                }
                @Override
                public void beforeExecute(Task task) {}
            }
        );
    }

}
