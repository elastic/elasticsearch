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
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.execution.TaskActionListener;
import org.gradle.api.execution.TaskExecutionListener;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.tasks.TaskState;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TestClustersPlugin implements Plugin<Project> {

    private static final String LIST_TASK_NAME = "listTestClusters";
    private static final String NODE_EXTENSION_NAME = "testClusters";
    static final String HELPER_CONFIGURATION_NAME = "testclusters";
    private static final String SYNC_ARTIFACTS_TASK_NAME = "syncTestClustersArtifacts";
    private static final int EXECUTOR_SHUTDOWN_TIMEOUT = 1;
    private static final TimeUnit EXECUTOR_SHUTDOWN_TIMEOUT_UNIT = TimeUnit.MINUTES;

    private static final Logger logger =  Logging.getLogger(TestClustersPlugin.class);

    // this is static because we need a single mapping across multi project builds, as some of the listeners we use,
    // like task graph are singletons across multi project builds.
    private static final Map<Task, List<ElasticsearchNode>> usedClusters = new ConcurrentHashMap<>();
    private static final Map<ElasticsearchNode, Integer> claimsInventory = new ConcurrentHashMap<>();
    private static final Set<ElasticsearchNode> runningClusters = Collections.synchronizedSet(new HashSet<>());
    private static volatile  ExecutorService executorService;

    @Override
    public void apply(Project project) {
        Project rootProject = project.getRootProject();

        // enable the DSL to describe clusters
        NamedDomainObjectContainer<ElasticsearchNode> container = createTestClustersContainerExtension(project);

        // provide a task to be able to list defined clusters.
        createListClustersTask(project, container);

        // create DSL for tasks to mark clusters these use
        createUseClusterTaskExtension(project);

        // There's a single Gradle instance for multi project builds, this means that some configuration needs to be
        // done only once even if the plugin is applied multiple times as a part of multi project build
        if (rootProject.getConfigurations().findByName(HELPER_CONFIGURATION_NAME) == null) {
            // We use a single configuration on the root project to resolve all testcluster dependencies ( like distros )
            // at once, only once without the need to repeat it for each project. This pays off assuming that most
            // projects use the same dependencies.
            Configuration helperConfiguration = project.getRootProject().getConfigurations().create(HELPER_CONFIGURATION_NAME);
            helperConfiguration.setDescription(
                "Internal helper configuration used by cluster configuration to download " +
                    "ES distributions and plugins."
            );

            // When running in the Daemon it's possible for this to hold references to past
            usedClusters.clear();
            claimsInventory.clear();
            runningClusters.clear();


            // We have a single task to sync the helper configuration to "artifacts dir"
            // the clusters will look for artifacts there based on the naming conventions.
            // Tasks that use a cluster will add this as a dependency automatically so it's guaranteed to run early in
            // the build.
            rootProject.getTasks().create(SYNC_ARTIFACTS_TASK_NAME, SyncTestClustersConfiguration.class);

            // When we know what tasks will run, we claim the clusters of those task to differentiate between clusters
            // that are defined in the build script and the ones that will actually be used in this invocation of gradle
            // we use this information to determine when the last task that required the cluster executed so that we can
            // terminate the cluster right away and free up resources.
            configureClaimClustersHook(project);

            // Before each task, we determine if a cluster needs to be started for that task.
            configureStartClustersHook(project);

            // After each task we determine if there are clusters that are no longer needed.
            configureStopClustersHook(project);

            // configure hooks to make sure no test cluster processes survive the build
            configureCleanupHooks(project);

            // Since we have everything modeled in the DSL, add all the required dependencies e.x. the distribution to the
            // configuration so the user doesn't have to repeat this.
            autoConfigureClusterDependencies(project, rootProject, container);
        }
    }

    private NamedDomainObjectContainer<ElasticsearchNode> createTestClustersContainerExtension(Project project) {
        // Create an extensions that allows describing clusters
        NamedDomainObjectContainer<ElasticsearchNode> container = project.container(
            ElasticsearchNode.class,
            name -> new ElasticsearchNode(
                project.getPath(),
                name,
                GradleServicesAdapter.getInstance(project),
                SyncTestClustersConfiguration.getTestClustersConfigurationExtractDir(project),
                new File(project.getBuildDir(), "testclusters")
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

    private static void createUseClusterTaskExtension(Project project) {
        // register an extension for all current and future tasks, so that any task can declare that it wants to use a
        // specific cluster.
        project.getTasks().all((Task task) ->
            task.getExtensions().findByType(ExtraPropertiesExtension.class)
                .set(
                    "useCluster",
                    new Closure<Void>(project, task) {
                        public void doCall(ElasticsearchNode node) {
                            Object thisObject = this.getThisObject();
                            if (thisObject instanceof Task == false) {
                                throw new AssertionError("Expected " + thisObject + " to be an instance of " +
                                    "Task, but got: " + thisObject.getClass());
                            }
                            usedClusters.computeIfAbsent(task, k -> new ArrayList<>()).add(node);
                            ((Task) thisObject).dependsOn(
                                project.getRootProject().getTasks().getByName(SYNC_ARTIFACTS_TASK_NAME)
                            );
                        }
                    })
        );
    }

    private static void configureClaimClustersHook(Project project) {
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

    private static void configureStartClustersHook(Project project) {
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

    private static void configureStopClustersHook(Project project) {
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
                            runningClusters.removeAll(stoppable);
                        }
                        stoppable.forEach(each -> each.stop(false));
                    }
                }
                @Override
                public void beforeExecute(Task task) {}
            }
        );
    }

    static File getTestClustersBuildDir(Project project) {
        return new File(project.getRootProject().getBuildDir(), "testclusters");
    }

    /**
     * Boilerplate to get testClusters container extension
     *
     * Equivalent to project.testClusters in the DSL
     */
    @SuppressWarnings("unchecked")
    public static NamedDomainObjectContainer<ElasticsearchNode> getNodeExtension(Project project) {
        return (NamedDomainObjectContainer<ElasticsearchNode>)
            project.getExtensions().getByName(NODE_EXTENSION_NAME);
    }

    private static void autoConfigureClusterDependencies(
        Project project,
        Project rootProject,
        NamedDomainObjectContainer<ElasticsearchNode> container
    ) {
        // When the project evaluated we know of all tasks that use clusters.
        // Each of these have to depend on the artifacts being synced.
        // We need afterEvaluate here despite the fact that container is a domain object, we can't implement this with
        // all because fields can change after the fact.
        project.afterEvaluate(ip -> container.forEach(esNode -> {
            // declare dependencies against artifacts needed by cluster formation.
            String dependency = String.format(
                "org.elasticsearch.distribution.zip:%s:%s@zip",
                esNode.getDistribution().getFileName(),
                esNode.getVersion()
            );
            logger.info("Cluster {} depends on {}", esNode.getName(), dependency);
            rootProject.getDependencies().add(HELPER_CONFIGURATION_NAME, dependency);
        }));
    }

    private static void configureCleanupHooks(Project project) {
        synchronized (runningClusters) {
            if (executorService == null || executorService.isTerminated()) {
                executorService = Executors.newSingleThreadExecutor();
            } else {
                throw new IllegalStateException("Trying to configure executor service twice");
            }
        }
        // When the Gradle daemon is used, it will interrupt all threads when the build concludes.
        executorService.submit(() -> {
            while (true) {
                try {
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException interrupted) {
                    shutDownAllClusters();
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        });

        project.getGradle().buildFinished(buildResult -> {
            logger.info("Build finished");
            shutdownExecutorService();
        });
        // When the Daemon is not used, or runs into issues, rely on a shutdown hook
        // When the daemon is used, but does not work correctly and eventually dies off (e.x. due to non interruptable
        // thread in the build) process will be stopped eventually when the daemon dies.
        Runtime.getRuntime().addShutdownHook(new Thread(TestClustersPlugin::shutDownAllClusters));
    }

    private static void shutdownExecutorService() {
        executorService.shutdownNow();
        try {
            if (executorService.awaitTermination(EXECUTOR_SHUTDOWN_TIMEOUT, EXECUTOR_SHUTDOWN_TIMEOUT_UNIT) == false) {
                throw new IllegalStateException(
                    "Failed to shut down executor service after " +
                    EXECUTOR_SHUTDOWN_TIMEOUT + " " + EXECUTOR_SHUTDOWN_TIMEOUT_UNIT
                );
            }
        } catch (InterruptedException e) {
            logger.info("Wait for testclusters shutdown interrupted", e);
            Thread.currentThread().interrupt();
        }
    }

    private static void shutDownAllClusters() {
        logger.info("Shutting down all test clusters", new RuntimeException());
        synchronized (runningClusters) {
            runningClusters.forEach(each -> each.stop(true));
            runningClusters.clear();
        }
    }


}
