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

import org.elasticsearch.gradle.DistributionDownloadPlugin;
import org.elasticsearch.gradle.ReaperPlugin;
import org.elasticsearch.gradle.ReaperService;
import org.elasticsearch.gradle.tool.Boilerplate;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.execution.TaskActionListener;
import org.gradle.api.execution.TaskExecutionListener;
import org.gradle.api.invocation.Gradle;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskState;

import java.io.File;

public class TestClustersPlugin implements Plugin<Project> {

    public static final String EXTENSION_NAME = "testClusters";
    public static final String THROTTLE_SERVICE_NAME = "testClustersThrottle";

    private static final String LIST_TASK_NAME = "listTestClusters";
    private static final String REGISTRY_SERVICE_NAME = "testClustersRegistry";
    private static final Logger logger = Logging.getLogger(TestClustersPlugin.class);

    @Override
    public void apply(Project project) {
        project.getPlugins().apply(DistributionDownloadPlugin.class);
        project.getRootProject().getPluginManager().apply(ReaperPlugin.class);

        ReaperService reaper = project.getRootProject().getExtensions().getByType(ReaperService.class);

        // enable the DSL to describe clusters
        NamedDomainObjectContainer<ElasticsearchCluster> container = createTestClustersContainerExtension(project, reaper);

        // provide a task to be able to list defined clusters.
        createListClustersTask(project, container);

        // register cluster registry as a global build service
        project.getGradle().getSharedServices().registerIfAbsent(REGISTRY_SERVICE_NAME, TestClustersRegistry.class, spec -> {});

        // register throttle so we only run at most max-workers/2 nodes concurrently
        project.getGradle()
            .getSharedServices()
            .registerIfAbsent(
                THROTTLE_SERVICE_NAME,
                TestClustersThrottle.class,
                spec -> spec.getMaxParallelUsages().set(Math.max(1, project.getGradle().getStartParameter().getMaxWorkerCount() / 2))
            );

        // register cluster hooks
        project.getRootProject().getPluginManager().apply(TestClustersHookPlugin.class);
    }

    private NamedDomainObjectContainer<ElasticsearchCluster> createTestClustersContainerExtension(Project project, ReaperService reaper) {
        // Create an extensions that allows describing clusters
        NamedDomainObjectContainer<ElasticsearchCluster> container = project.container(
            ElasticsearchCluster.class,
            name -> new ElasticsearchCluster(project.getPath(), name, project, reaper, new File(project.getBuildDir(), "testclusters"))
        );
        project.getExtensions().add(EXTENSION_NAME, container);
        return container;
    }

    private void createListClustersTask(Project project, NamedDomainObjectContainer<ElasticsearchCluster> container) {
        Task listTask = project.getTasks().create(LIST_TASK_NAME);
        listTask.setGroup("ES cluster formation");
        listTask.setDescription("Lists all ES clusters configured for this project");
        listTask.doLast(
            (Task task) -> container.forEach(cluster -> logger.lifecycle("   * {}: {}", cluster.getName(), cluster.getNumberOfNodes()))
        );
    }

    static class TestClustersHookPlugin implements Plugin<Project> {
        @Override
        public void apply(Project project) {
            if (project != project.getRootProject()) {
                throw new IllegalStateException(this.getClass().getName() + " can only be applied to the root project.");
            }

            Provider<TestClustersRegistry> registryProvider = Boilerplate.getBuildService(
                project.getGradle().getSharedServices(),
                REGISTRY_SERVICE_NAME
            );
            TestClustersRegistry registry = registryProvider.get();

            // When we know what tasks will run, we claim the clusters of those task to differentiate between clusters
            // that are defined in the build script and the ones that will actually be used in this invocation of gradle
            // we use this information to determine when the last task that required the cluster executed so that we can
            // terminate the cluster right away and free up resources.
            configureClaimClustersHook(project.getGradle(), registry);

            // Before each task, we determine if a cluster needs to be started for that task.
            configureStartClustersHook(project.getGradle(), registry);

            // After each task we determine if there are clusters that are no longer needed.
            configureStopClustersHook(project.getGradle(), registry);
        }

        private static void configureClaimClustersHook(Gradle gradle, TestClustersRegistry registry) {
            // Once we know all the tasks that need to execute, we claim all the clusters that belong to those and count the
            // claims so we'll know when it's safe to stop them.
            gradle.getTaskGraph().whenReady(taskExecutionGraph -> {
                taskExecutionGraph.getAllTasks()
                    .stream()
                    .filter(task -> task instanceof TestClustersAware)
                    .map(task -> (TestClustersAware) task)
                    .flatMap(task -> task.getClusters().stream())
                    .forEach(registry::claimCluster);
            });
        }

        private static void configureStartClustersHook(Gradle gradle, TestClustersRegistry registry) {
            gradle.addListener(new TaskActionListener() {
                @Override
                public void beforeActions(Task task) {
                    if (task instanceof TestClustersAware == false) {
                        return;
                    }
                    // we only start the cluster before the actions, so we'll not start it if the task is up-to-date
                    TestClustersAware awareTask = (TestClustersAware) task;
                    awareTask.beforeStart();
                    awareTask.getClusters().forEach(registry::maybeStartCluster);
                }

                @Override
                public void afterActions(Task task) {}
            });
        }

        private static void configureStopClustersHook(Gradle gradle, TestClustersRegistry registry) {
            gradle.addListener(new TaskExecutionListener() {
                @Override
                public void afterExecute(Task task, TaskState state) {
                    if (task instanceof TestClustersAware == false) {
                        return;
                    }
                    // always unclaim the cluster, even if _this_ task is up-to-date, as others might not have been
                    // and caused the cluster to start.
                    ((TestClustersAware) task).getClusters().forEach(cluster -> registry.stopCluster(cluster, state.getFailure() != null));
                }

                @Override
                public void beforeExecute(Task task) {}
            });
        }
    }
}
