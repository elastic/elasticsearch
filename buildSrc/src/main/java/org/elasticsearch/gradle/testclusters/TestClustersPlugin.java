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
import org.elasticsearch.gradle.DistributionDownloadPlugin;
import org.elasticsearch.gradle.ElasticsearchDistribution;
import org.elasticsearch.gradle.test.RestTestRunnerTask;
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

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TestClustersPlugin implements Plugin<Project> {

    private static final String LIST_TASK_NAME = "listTestClusters";
    public static final String EXTENSION_NAME = "testClusters";

    private static final Logger logger =  Logging.getLogger(TestClustersPlugin.class);
    private static final String TESTCLUSTERS_INSPECT_FAILURE = "testclusters.inspect.failure";

    private final Map<Task, List<ElasticsearchCluster>> usedClusters = new HashMap<>();
    private final Map<ElasticsearchCluster, Integer> claimsInventory = new HashMap<>();
    private final Set<ElasticsearchCluster> runningClusters = new HashSet<>();
    private final Boolean allowClusterToSurvive = Boolean.valueOf(System.getProperty(TESTCLUSTERS_INSPECT_FAILURE, "false"));

    @Override
    public void apply(Project project) {
        project.getPlugins().apply(DistributionDownloadPlugin.class);

        // enable the DSL to describe clusters
        NamedDomainObjectContainer<ElasticsearchCluster> container = createTestClustersContainerExtension(project);

        TestClustersCleanupExtension.createExtension(project);

        // provide a task to be able to list defined clusters.
        createListClustersTask(project, container);

        // create DSL for tasks to mark clusters these use
        createUseClusterTaskExtension(project, container);

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

    private NamedDomainObjectContainer<ElasticsearchCluster> createTestClustersContainerExtension(Project project) {
        NamedDomainObjectContainer<ElasticsearchDistribution> distros = DistributionDownloadPlugin.getContainer(project);

        // Create an extensions that allows describing clusters
        NamedDomainObjectContainer<ElasticsearchCluster> container = project.container(
            ElasticsearchCluster.class,
            name -> new ElasticsearchCluster(
                project.getPath(),
                name,
                project,
                i -> distros.create(name + "-" + i),
                new File(project.getBuildDir(), "testclusters")
            )
        );
        project.getExtensions().add(EXTENSION_NAME, container);
        return container;
    }


    private void createListClustersTask(Project project, NamedDomainObjectContainer<ElasticsearchCluster> container) {
        Task listTask = project.getTasks().create(LIST_TASK_NAME);
        listTask.setGroup("ES cluster formation");
        listTask.setDescription("Lists all ES clusters configured for this project");
        listTask.doLast((Task task) ->
            container.forEach(cluster ->
                logger.lifecycle("   * {}: {}", cluster.getName(), cluster.getNumberOfNodes())
            )
        );
    }

    private void createUseClusterTaskExtension(Project project, NamedDomainObjectContainer<ElasticsearchCluster> container) {
        // register an extension for all current and future tasks, so that any task can declare that it wants to use a
        // specific cluster.
        project.getTasks().all((Task task) ->
            task.getExtensions().findByType(ExtraPropertiesExtension.class)
                .set(
                    "useCluster",
                    new Closure<Void>(project, task) {
                        public void doCall(ElasticsearchCluster cluster) {
                            if (container.contains(cluster) == false) {
                                throw new TestClustersException(
                                    "Task " + task.getPath() + " can't use test cluster from" +
                                    " another project " + cluster
                                );
                            }
                            Object thisObject = this.getThisObject();
                            if (thisObject instanceof Task == false) {
                                throw new AssertionError("Expected " + thisObject + " to be an instance of " +
                                    "Task, but got: " + thisObject.getClass());
                            }
                            usedClusters.computeIfAbsent(task, k -> new ArrayList<>()).add(cluster);
                            for (ElasticsearchNode node : cluster.getNodes()) {
                                ((Task) thisObject).dependsOn(node.getDistribution().getExtracted());
                            }
                            if (thisObject instanceof RestTestRunnerTask) {
                                ((RestTestRunnerTask) thisObject).testCluster(cluster);
                            }
                        }
                    })
        );
    }

    private void configureClaimClustersHook(Project project) {
        // Once we know all the tasks that need to execute, we claim all the clusters that belong to those and count the
        // claims so we'll know when it's safe to stop them.
        project.getGradle().getTaskGraph().whenReady(taskExecutionGraph -> {
            Set<String> forExecution = taskExecutionGraph.getAllTasks().stream()
                .map(Task::getPath)
                .collect(Collectors.toSet());

            usedClusters.forEach((task, listOfClusters) ->
                listOfClusters.forEach(elasticsearchCluster -> {
                    if (forExecution.contains(task.getPath())) {
                        elasticsearchCluster.freeze();
                        claimsInventory.put(elasticsearchCluster, claimsInventory.getOrDefault(elasticsearchCluster, 0) + 1);
                    }
                }));
            if (claimsInventory.isEmpty() == false) {
                logger.info("Claims inventory: {}", claimsInventory);
            }
        });
    }

    private void configureStartClustersHook(Project project) {
        project.getGradle().addListener(
            new TaskActionListener() {
                @Override
                public void beforeActions(Task task) {
                    // we only start the cluster before the actions, so we'll not start it if the task is up-to-date
                    List<ElasticsearchCluster> neededButNotRunning = usedClusters.getOrDefault(
                        task,
                        Collections.emptyList()
                    )
                        .stream()
                        .filter(cluster -> runningClusters.contains(cluster) == false)
                        .collect(Collectors.toList());

                    project.getRootProject().getExtensions()
                        .getByType(TestClustersCleanupExtension.class)
                        .getCleanupThread()
                        .watch(neededButNotRunning);
                    neededButNotRunning
                        .forEach(elasticsearchCluster -> {
                            elasticsearchCluster.start();
                            runningClusters.add(elasticsearchCluster);
                        });
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
                    List<ElasticsearchCluster> clustersUsedByTask = usedClusters.getOrDefault(
                        task,
                        Collections.emptyList()
                    );
                    if (clustersUsedByTask.isEmpty()) {
                        return;
                    }
                    logger.info("Clusters were used, stopping and releasing permits");
                    final int permitsToRelease;
                    if (state.getFailure() != null) {
                        // If the task fails, and other tasks use this cluster, the other task will likely never be
                        // executed at all, so we will never be called again to un-claim and terminate it.
                        clustersUsedByTask.forEach(cluster -> stopCluster(cluster, true));
                        permitsToRelease = clustersUsedByTask.stream()
                            .map(cluster -> cluster.getNumberOfNodes())
                            .reduce(Integer::sum).get();
                    } else {
                        clustersUsedByTask.forEach(
                            cluster -> claimsInventory.put(cluster, claimsInventory.getOrDefault(cluster, 0) - 1)
                        );
                        List<ElasticsearchCluster> stoppingClusers = claimsInventory.entrySet().stream()
                            .filter(entry -> entry.getValue() == 0)
                            .filter(entry -> runningClusters.contains(entry.getKey()))
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toList());
                        stoppingClusers.forEach(cluster -> {
                            stopCluster(cluster, false);
                            runningClusters.remove(cluster);
                        });

                        project.getRootProject().getExtensions()
                            .getByType(TestClustersCleanupExtension.class)
                            .getCleanupThread()
                            .unWatch(stoppingClusers);
                    }
                }
                @Override
                public void beforeExecute(Task task) {}
            }
        );
    }

    private void stopCluster(ElasticsearchCluster cluster, boolean taskFailed) {
        if (allowClusterToSurvive) {
            logger.info("Not stopping clusters, disabled by property");
            if (taskFailed) {
                // task failed or this is the last one to stop
                for (int i=1 ; ; i += i) {
                    logger.lifecycle(
                        "No more test clusters left to run, going to sleep because {} was set," +
                            " interrupt (^C) to stop clusters.", TESTCLUSTERS_INSPECT_FAILURE
                    );
                    try {
                        Thread.sleep(1000 * i);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
        cluster.stop(taskFailed);
    }
}
