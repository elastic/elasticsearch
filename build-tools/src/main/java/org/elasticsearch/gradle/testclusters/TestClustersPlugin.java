/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.testclusters;

import org.elasticsearch.gradle.DistributionDownloadPlugin;
import org.elasticsearch.gradle.ReaperPlugin;
import org.elasticsearch.gradle.ReaperService;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Action;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.execution.TaskActionListener;
import org.gradle.api.execution.TaskExecutionListener;
import org.gradle.api.file.ArchiveOperations;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.invocation.Gradle;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.TaskState;
import org.gradle.internal.jvm.Jvm;
import org.gradle.process.ExecOperations;

import javax.inject.Inject;
import java.io.File;

import static org.elasticsearch.gradle.util.GradleUtils.noop;

public class TestClustersPlugin implements Plugin<Project> {

    public static final String EXTENSION_NAME = "testClusters";
    public static final String THROTTLE_SERVICE_NAME = "testClustersThrottle";

    private static final String LIST_TASK_NAME = "listTestClusters";
    private static final String REGISTRY_SERVICE_NAME = "testClustersRegistry";
    private static final Logger logger = Logging.getLogger(TestClustersPlugin.class);
    private final ProviderFactory providerFactory;
    private Provider<File> runtimeJavaProvider;

    @Inject
    protected FileSystemOperations getFileSystemOperations() {
        throw new UnsupportedOperationException();
    }

    @Inject
    protected ArchiveOperations getArchiveOperations() {
        throw new UnsupportedOperationException();
    }

    @Inject
    protected ExecOperations getExecOperations() {
        throw new UnsupportedOperationException();
    }

    @Inject
    public TestClustersPlugin(ProviderFactory providerFactory) {
        this.providerFactory = providerFactory;
    }

    public void setRuntimeJava(Provider<File> runtimeJava) {
        this.runtimeJavaProvider = runtimeJava;
    }

    @Override
    public void apply(Project project) {
        project.getPlugins().apply(DistributionDownloadPlugin.class);
        project.getRootProject().getPluginManager().apply(ReaperPlugin.class);
        Provider<ReaperService> reaperServiceProvider = GradleUtils.getBuildService(
            project.getGradle().getSharedServices(),
            ReaperPlugin.REAPER_SERVICE_NAME
        );
        runtimeJavaProvider = providerFactory.provider(
            () -> System.getenv("RUNTIME_JAVA_HOME") == null ? Jvm.current().getJavaHome() : new File(System.getenv("RUNTIME_JAVA_HOME"))
        );
        // enable the DSL to describe clusters
        NamedDomainObjectContainer<ElasticsearchCluster> container = createTestClustersContainerExtension(project, reaperServiceProvider);

        // provide a task to be able to list defined clusters.
        createListClustersTask(project, container);

        // register cluster registry as a global build service
        project.getGradle().getSharedServices().registerIfAbsent(REGISTRY_SERVICE_NAME, TestClustersRegistry.class, noop());

        // register throttle so we only run at most max-workers/2 nodes concurrently
        project.getGradle()
            .getSharedServices()
            .registerIfAbsent(
                THROTTLE_SERVICE_NAME,
                TestClustersThrottle.class,
                spec -> spec.getMaxParallelUsages().set(Math.max(1, project.getGradle().getStartParameter().getMaxWorkerCount() * 2 / 3))
            );

        // register cluster hooks
        project.getRootProject().getPluginManager().apply(TestClustersHookPlugin.class);
    }

    private NamedDomainObjectContainer<ElasticsearchCluster> createTestClustersContainerExtension(
        Project project,
        Provider<ReaperService> reaper
    ) {
        // Create an extensions that allows describing clusters
        NamedDomainObjectContainer<ElasticsearchCluster> container = project.container(ElasticsearchCluster.class, name -> {
            return new ElasticsearchCluster(
                project.getPath(),
                name,
                project,
                reaper,
                getFileSystemOperations(),
                getArchiveOperations(),
                getExecOperations(),
                new File(project.getBuildDir(), "testclusters"),
                runtimeJavaProvider
            );
        });
        project.getExtensions().add(EXTENSION_NAME, container);
        container.all(cluster -> cluster.systemProperty("ingest.geoip.downloader.enabled.default", "false"));
        return container;
    }

    private void createListClustersTask(Project project, NamedDomainObjectContainer<ElasticsearchCluster> container) {
        // Task is never up to date so we can pass an lambda for the task action
        project.getTasks().register(LIST_TASK_NAME, task -> {
            task.setGroup("ES cluster formation");
            task.setDescription("Lists all ES clusters configured for this project");
            task.doLast(
                (Task t) -> container.forEach(cluster -> logger.lifecycle("   * {}: {}", cluster.getName(), cluster.getNumberOfNodes()))
            );
        });

    }

    static class TestClustersHookPlugin implements Plugin<Project> {
        @Override
        public void apply(Project project) {
            if (project != project.getRootProject()) {
                throw new IllegalStateException(this.getClass().getName() + " can only be applied to the root project.");
            }

            Provider<TestClustersRegistry> registryProvider = GradleUtils.getBuildService(
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
