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
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.file.ArchiveOperations;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.internal.file.FileOperations;
import org.gradle.api.invocation.Gradle;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;
import org.gradle.build.event.BuildEventsListenerRegistry;
import org.gradle.internal.jvm.Jvm;
import org.gradle.process.ExecOperations;
import org.gradle.tooling.events.FinishEvent;
import org.gradle.tooling.events.OperationCompletionListener;
import org.gradle.tooling.events.task.TaskFailureResult;
import org.gradle.tooling.events.task.TaskFinishEvent;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import javax.inject.Inject;

import static org.elasticsearch.gradle.util.GradleUtils.noop;

public class TestClustersPlugin implements Plugin<Project> {

    public static final String EXTENSION_NAME = "testClusters";
    public static final String THROTTLE_SERVICE_NAME = "testClustersThrottle";

    private static final String LIST_TASK_NAME = "listTestClusters";
    public static final String REGISTRY_SERVICE_NAME = "testClustersRegistry";
    private static final Logger logger = Logging.getLogger(TestClustersPlugin.class);
    private final ProviderFactory providerFactory;
    private Provider<File> runtimeJavaProvider;
    private Function<Version, Boolean> isReleasedVersion = v -> true;

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
    protected FileOperations getFileOperations() {
        throw new UnsupportedOperationException();
    }

    @Inject
    public TestClustersPlugin(ProviderFactory providerFactory) {
        this.providerFactory = providerFactory;
    }

    public void setRuntimeJava(Provider<File> runtimeJava) {
        this.runtimeJavaProvider = runtimeJava;
    }

    public void setIsReleasedVersion(Function<Version, Boolean> isReleasedVersion) {
        this.isReleasedVersion = isReleasedVersion;
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
                spec -> spec.getMaxParallelUsages().set(Math.max(1, project.getGradle().getStartParameter().getMaxWorkerCount() / 2))
            );

        // register cluster hooks
        project.getRootProject().getPluginManager().apply(TestClustersHookPlugin.class);
    }

    private NamedDomainObjectContainer<ElasticsearchCluster> createTestClustersContainerExtension(
        Project project,
        Provider<ReaperService> reaper
    ) {
        // Create an extensions that allows describing clusters
        NamedDomainObjectContainer<ElasticsearchCluster> container = project.container(
            ElasticsearchCluster.class,
            name -> new ElasticsearchCluster(
                project.getPath(),
                name,
                project,
                reaper,
                getFileSystemOperations(),
                getArchiveOperations(),
                getExecOperations(),
                getFileOperations(),
                new File(project.getBuildDir(), "testclusters"),
                runtimeJavaProvider,
                isReleasedVersion
            )
        );
        project.getExtensions().add(EXTENSION_NAME, container);
        container.configureEach(cluster -> cluster.systemProperty("ingest.geoip.downloader.enabled.default", "false"));
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

    static abstract class TestClustersHookPlugin implements Plugin<Project> {
        @Inject
        public abstract BuildEventsListenerRegistry getEventsListenerRegistry();

        @Inject
        public TestClustersHookPlugin() {}

        public void apply(Project project) {
            if (project != project.getRootProject()) {
                throw new IllegalStateException(this.getClass().getName() + " can only be applied to the root project.");
            }
            Provider<TestClustersRegistry> registryProvider = GradleUtils.getBuildService(
                project.getGradle().getSharedServices(),
                REGISTRY_SERVICE_NAME
            );

            Provider<TaskEventsService> testClusterTasksService = project.getGradle()
                .getSharedServices()
                .registerIfAbsent("testClusterTasksService", TaskEventsService.class, spec -> {});

            TestClustersRegistry registry = registryProvider.get();

            // When we know what tasks will run, we claim the clusters of those task to differentiate between clusters
            // that are defined in the build script and the ones that will actually be used in this invocation of gradle
            // we use this information to determine when the last task that required the cluster executed so that we can
            // terminate the cluster right away and free up resources.
            configureClaimClustersHook(project.getGradle(), registry);

            // Before each task, we determine if a cluster needs to be started for that task.
            configureStartClustersHook(project.getGradle(), registry, testClusterTasksService);

            // After each task we determine if there are clusters that are no longer needed.
            getEventsListenerRegistry().onTaskCompletion(testClusterTasksService);
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

        private void configureStartClustersHook(
            Gradle gradle,
            TestClustersRegistry registry,
            Provider<TaskEventsService> testClusterTasksService
        ) {
            testClusterTasksService.get().registry(registry);
            gradle.getTaskGraph().whenReady(taskExecutionGraph -> {
                taskExecutionGraph.getAllTasks()
                    .stream()
                    .filter(task -> task instanceof TestClustersAware)
                    .map(task -> (TestClustersAware) task)
                    .forEach(awareTask -> {
                        testClusterTasksService.get().register(awareTask.getPath(), awareTask);
                        awareTask.doFirst(task -> {
                            awareTask.beforeStart();
                            awareTask.getClusters().forEach(awareTask.getRegistery().get()::maybeStartCluster);
                        });
                    });
            });
        }
    }

    public static void maybeStartCluster(ElasticsearchCluster cluster, Set<ElasticsearchCluster> runningClusters) {
        if (runningClusters.contains(cluster)) {
            return;
        }
        runningClusters.add(cluster);
        cluster.start();
    }

    static public abstract class TaskEventsService implements BuildService<BuildServiceParameters.None>, OperationCompletionListener {

        Map<String, TestClustersAware> tasksMap = new HashMap<>();
        private TestClustersRegistry registryProvider;

        public void register(String path, TestClustersAware task) {
            tasksMap.put(path, task);
        }

        public void registry(TestClustersRegistry registry) {
            registryProvider = registry;
        }

        @Override
        public void onFinish(FinishEvent finishEvent) {
            if (finishEvent instanceof TaskFinishEvent taskFinishEvent) {
                // Handle task finish event...
                String taskPath = taskFinishEvent.getDescriptor().getTaskPath();
                if (tasksMap.containsKey(taskPath)) {
                    TestClustersAware task = tasksMap.get(taskPath);
                    // unclaim the cluster if the task has been executed and the cluster has been claimed in the doFirst block.
                    if (task.getDidWork()) {
                        task.getClusters()
                            .forEach(
                                cluster -> registryProvider.stopCluster(cluster, taskFinishEvent.getResult() instanceof TaskFailureResult)
                            );
                    }
                }
            }
        }
    }
}
