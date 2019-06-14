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
import org.elasticsearch.gradle.BwcVersions;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.tool.Boilerplate;
import org.gradle.api.Action;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.repositories.MavenArtifactRepository;
import org.gradle.api.credentials.HttpHeaderCredentials;
import org.gradle.api.execution.TaskActionListener;
import org.gradle.api.execution.TaskExecutionListener;
import org.gradle.api.file.FileTree;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.tasks.TaskState;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TestClustersPlugin implements Plugin<Project> {

    private static final String LIST_TASK_NAME = "listTestClusters";
    public static final String EXTENSION_NAME = "testClusters";
    private static final String HELPER_CONFIGURATION_PREFIX = "testclusters";
    private static final String SYNC_ARTIFACTS_TASK_NAME = "syncTestClustersArtifacts";
    private static final int EXECUTOR_SHUTDOWN_TIMEOUT = 1;
    private static final TimeUnit EXECUTOR_SHUTDOWN_TIMEOUT_UNIT = TimeUnit.MINUTES;

    private static final Logger logger =  Logging.getLogger(TestClustersPlugin.class);
    private static final String TESTCLUSTERS_INSPECT_FAILURE = "testclusters.inspect.failure";

    private final Map<Task, List<ElasticsearchCluster>> usedClusters = new HashMap<>();
    private final Map<ElasticsearchCluster, Integer> claimsInventory = new HashMap<>();
    private final Set<ElasticsearchCluster> runningClusters =new HashSet<>();
    private final Thread shutdownHook = new Thread(this::shutDownAllClusters);
    private final Boolean allowClusterToSurvive = Boolean.valueOf(System.getProperty(TESTCLUSTERS_INSPECT_FAILURE, "false"));
    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    public static String getHelperConfigurationName(String version) {
        return HELPER_CONFIGURATION_PREFIX + "-" + version;
    }

    @Override
    public void apply(Project project) {
        Project rootProject = project.getRootProject();

        // enable the DSL to describe clusters
        NamedDomainObjectContainer<ElasticsearchCluster> container = createTestClustersContainerExtension(project);

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

        // configure hooks to make sure no test cluster processes survive the build
        configureCleanupHooks(project);

        // Since we have everything modeled in the DSL, add all the required dependencies e.x. the distribution to the
        // configuration so the user doesn't have to repeat this.
        autoConfigureClusterDependencies(project, rootProject, container);
    }

    private static File getExtractDir(Project project) {
        return new File(project.getRootProject().getBuildDir(), "testclusters/extract/");
    }

    private NamedDomainObjectContainer<ElasticsearchCluster> createTestClustersContainerExtension(Project project) {
        // Create an extensions that allows describing clusters
        NamedDomainObjectContainer<ElasticsearchCluster> container = project.container(
            ElasticsearchCluster.class,
            name -> new ElasticsearchCluster(
                project.getPath(),
                name,
                project,
                new File(project.getRootProject().getBuildDir(), "testclusters/extract"),
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
                            ((Task) thisObject).dependsOn(
                                project.getRootProject().getTasks().getByName(SYNC_ARTIFACTS_TASK_NAME)
                            );
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
                    usedClusters.getOrDefault(task, Collections.emptyList()).stream()
                        .filter(cluster -> runningClusters.contains(cluster) == false)
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
                    if (state.getFailure() != null) {
                        // If the task fails, and other tasks use this cluster, the other task will likely never be
                        // executed at all, so we will never get to un-claim and terminate it.
                        clustersUsedByTask.forEach(cluster -> stopCluster(cluster, true));
                    } else {
                        clustersUsedByTask.forEach(
                            cluster -> claimsInventory.put(cluster, claimsInventory.getOrDefault(cluster, 0) - 1)
                        );
                        claimsInventory.entrySet().stream()
                            .filter(entry -> entry.getValue() == 0)
                            .filter(entry -> runningClusters.contains(entry.getKey()))
                            .map(Map.Entry::getKey)
                            .forEach(cluster -> {
                                stopCluster(cluster, false);
                                runningClusters.remove(cluster);
                            });
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

    /**
     * Boilerplate to get testClusters container extension
     *
     * Equivalent to project.testClusters in the DSL
     */
    @SuppressWarnings("unchecked")
    public static NamedDomainObjectContainer<ElasticsearchCluster> getNodeExtension(Project project) {
        return (NamedDomainObjectContainer<ElasticsearchCluster>)
            project.getExtensions().getByName(EXTENSION_NAME);
    }

    private static void autoConfigureClusterDependencies(
        Project project,
        Project rootProject,
        NamedDomainObjectContainer<ElasticsearchCluster> container
    ) {
        // Download integ test distribution from maven central
        MavenArtifactRepository mavenCentral = project.getRepositories().mavenCentral();
        mavenCentral.content(spec -> {
            spec.includeGroupByRegex("org\\.elasticsearch\\.distribution\\..*");
        });

        // Other distributions from the download service
        project.getRepositories().add(
            project.getRepositories().ivy(spec -> {
                spec.setUrl("https://artifacts.elastic.co/downloads");
                spec.patternLayout(p -> p.artifact("elasticsearch/[module]-[revision](-[classifier]).[ext]"));
                HttpHeaderCredentials headerConfig = spec.getCredentials(HttpHeaderCredentials.class);
                headerConfig.setName("X-Elastic-No-KPI");
                headerConfig.setValue("1");
                spec.content(c-> c.includeGroupByRegex("org\\.elasticsearch\\.distribution\\..*"));
            })
        );

        // We have a single task to sync the helper configuration to "artifacts dir"
        // the clusters will look for artifacts there based on the naming conventions.
        // Tasks that use a cluster will add this as a dependency automatically so it's guaranteed to run early in
        // the build.
        Boilerplate.maybeCreate(rootProject.getTasks(), SYNC_ARTIFACTS_TASK_NAME, onCreate -> {
            onCreate.getOutputs().dir(getExtractDir(rootProject));
            onCreate.getInputs().files(
                project.getRootProject().getConfigurations().matching(conf -> conf.getName().startsWith(HELPER_CONFIGURATION_PREFIX))
            );
            onCreate.dependsOn(project.getRootProject().getConfigurations()
                .matching(conf -> conf.getName().startsWith(HELPER_CONFIGURATION_PREFIX))
            );
            // NOTE: Gradle doesn't allow a lambda here ( fails at runtime )
            onCreate.doFirst(new Action<Task>() {
                @Override
                public void execute(Task task) {
                    // Clean up the extract dir first to make sure we have no stale files from older
                    // previous builds of the same distribution
                    project.delete(getExtractDir(rootProject));
                }
            });
            onCreate.doLast(new Action<Task>() {
                    @Override
                    public void execute(Task task) {
                        project.getRootProject().getConfigurations()
                            .matching(config -> config.getName().startsWith(HELPER_CONFIGURATION_PREFIX))
                            .forEach(config -> project.copy(spec ->
                                config.getResolvedConfiguration()
                                    .getResolvedArtifacts()
                                    .forEach(resolvedArtifact -> {
                                        final FileTree files;
                                        File file = resolvedArtifact.getFile();
                                        if (file.getName().endsWith(".zip")) {
                                            files = project.zipTree(file);
                                        } else if (file.getName().endsWith("tar.gz")) {
                                            files = project.tarTree(file);
                                        } else {
                                            throw new IllegalArgumentException("Can't extract " + file + " unknown file extension");
                                        }
                                        logger.info("Extracting {}@{}", resolvedArtifact, config);
                                        spec.from(files, s -> s.into(resolvedArtifact.getModuleVersion().getId().getGroup()));
                                        spec.into(getExtractDir(project));
                                    }))
                            );
                    }
            });
        });

        // When the project evaluated we know of all tasks that use clusters.
        // Each of these have to depend on the artifacts being synced.
        // We need afterEvaluate here despite the fact that container is a domain object, we can't implement this with
        // all because fields can change after the fact.
        project.afterEvaluate(ip -> container.forEach(esCluster ->
            esCluster.eachVersionedDistribution((version, distribution) -> {
                Configuration helperConfiguration = Boilerplate.maybeCreate(
                    rootProject.getConfigurations(),
                    getHelperConfigurationName(version),
                    onCreate ->
                        // We use a single configuration on the root project to resolve all testcluster dependencies ( like distros )
                        // at once, only once without the need to repeat it for each project. This pays off assuming that most
                        // projects use the same dependencies.
                        onCreate.setDescription(
                            "Internal helper configuration used by cluster configuration to download " +
                                "ES distributions and plugins for " + version
                        )
                );
                BwcVersions.UnreleasedVersionInfo unreleasedInfo;
                final List<Version> unreleased;
                {
                    ExtraPropertiesExtension extraProperties = project.getExtensions().getExtraProperties();
                    if (extraProperties.has("bwcVersions")) {
                        Object bwcVersionsObj = extraProperties.get("bwcVersions");
                        if (bwcVersionsObj instanceof BwcVersions == false) {
                            throw new IllegalStateException("Expected project.bwcVersions to be of type VersionCollection " +
                                "but instead it was " + bwcVersionsObj.getClass());
                        }
                        final BwcVersions bwcVersions = (BwcVersions) bwcVersionsObj;
                        unreleased = ((BwcVersions) bwcVersionsObj).getUnreleased();
                        unreleasedInfo = bwcVersions.unreleasedInfo(Version.fromString(version));
                    } else {
                        logger.info("No version information available, assuming all versions used are released");
                        unreleased = Collections.emptyList();
                        unreleasedInfo = null;
                    }
                }
                if (unreleased.contains(Version.fromString(version))) {
                    Map<String, Object> projectNotation = new HashMap<>();
                    projectNotation.put("path", unreleasedInfo.gradleProjectPath);
                    projectNotation.put("configuration", distribution.getLiveConfiguration());
                    rootProject.getDependencies().add(
                        helperConfiguration.getName(),
                        project.getDependencies().project(projectNotation)
                    );
                } else {
                    rootProject.getDependencies().add(
                        helperConfiguration.getName(),
                        distribution.getGroup() + ":" +
                            distribution.getArtifactName() + ":" +
                            version +
                            (distribution.getClassifier().isEmpty() ? "" : ":" + distribution.getClassifier()) + "@" +
                            distribution.getFileExtension());

                }
            })));
    }

    private void configureCleanupHooks(Project project) {
        // When the Gradle daemon is used, it will interrupt all threads when the build concludes.
        // This is our signal to clean up
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

        // When the Daemon is not used, or runs into issues, rely on a shutdown hook
        // When the daemon is used, but does not work correctly and eventually dies off (e.x. due to non interruptible
        // thread in the build) process will be stopped eventually when the daemon dies.
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        // When we don't run into anything out of the ordinary, and the build completes, makes sure to clean up
        project.getGradle().buildFinished(buildResult -> {
            shutdownExecutorService();
            if (false == Runtime.getRuntime().removeShutdownHook(shutdownHook)) {
                logger.info("Trying to deregister shutdown hook when it was not registered.");
            }
        });
    }

    private void shutdownExecutorService() {
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

    private void shutDownAllClusters() {
        synchronized (runningClusters) {
            if (runningClusters.isEmpty()) {
                return;
            }
            Iterator<ElasticsearchCluster> iterator = runningClusters.iterator();
            while (iterator.hasNext()) {
                ElasticsearchCluster next = iterator.next();
                iterator.remove();
                next.stop(false);
            }
        }
    }

}
