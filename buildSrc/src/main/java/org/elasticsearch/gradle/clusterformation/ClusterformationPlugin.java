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

import org.elasticsearch.GradleServicesAdapter;
import org.elasticsearch.model.Distribution;
import org.elasticsearch.model.Version;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.Sync;

import java.io.File;
import java.text.MessageFormat;
import java.util.List;

public class ClusterformationPlugin implements Plugin<Project> {

    public static final String LIST_TASK_NAME = "listElasticSearchClusters";
    public static final String EXTENSION_NAME = "elasticSearchClusters";
    public static final String TASK_EXTENSION_NAME = "clusterFormation";

    private static final String HELPER_CONFIGURATION_NAME = "_internalClusterFormationConfiguration";
    public static final String SYNC_ARTIFACTS_TASK_NAME = "syncClusterFormationArtifacts";

    private final Logger logger =  Logging.getLogger(ClusterformationPlugin.class);

    @Override
    public void apply(Project project) {
        Project rootProject = project.getRootProject();

        // Create an extensions that allows describing clusters
        NamedDomainObjectContainer<? extends ElasticsearchConfiguration> container = project.container(
            ElasticsearchNode.class,
            (name) -> new ElasticsearchNode(name, GradleServicesAdapter.getInstance(project), getArtifactsDir(project))
        );
        project.getExtensions().add(EXTENSION_NAME, container);

        // register an extension for all current and future tasks, so that any task can declare that it wants to use a
        // specific cluster.
        project.getTasks().all((Task task) ->
            task.getExtensions().create(TASK_EXTENSION_NAME, ClusterFormationTaskExtension.class, task)
        );

        // Utility task to list all clusters defined
        Task listTask = project.getTasks().create(LIST_TASK_NAME);
        listTask.setGroup("ES cluster formation");
        listTask.setDescription("Lists all ES clusters configured for this project");
        listTask.doLast((Task task) ->
            container.forEach((ElasticsearchConfiguration cluster) ->
                logger.lifecycle("   * {}: {}", cluster.getName(), cluster.getDistribution())
            )
        );

        // Have a single common location to set up the required artifacts
        if (rootProject.getConfigurations().findByName(HELPER_CONFIGURATION_NAME) == null) {
            Configuration helperConfiguration = rootProject.getConfigurations().create(HELPER_CONFIGURATION_NAME);
            helperConfiguration.setDescription(
                "Internal helper configuration used by cluster configuration to download " +
                "ES distributions and plugins."
            );
            rootProject.getRepositories().add(
                rootProject.getRepositories().mavenCentral()
            );

            Sync syncTask = rootProject.getTasks().create(SYNC_ARTIFACTS_TASK_NAME, Sync.class);
            syncTask.from(helperConfiguration);
            syncTask.into(getArtifactsDir(rootProject));
        }

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
            ip.getTasks().forEach( task -> {
                if (getTaskExtension(task).getClaimedClusters().isEmpty() == false) {
                    task.dependsOn(rootProject.getTasks().getByName(SYNC_ARTIFACTS_TASK_NAME));
                }
            });
        });

        // Make sure we only claim the clusters for the tasks that will actually execute
        project.getGradle().getTaskGraph().whenReady(taskExecutionGraph ->
            taskExecutionGraph.getAllTasks().forEach(task -> {
                    List<ElasticsearchConfiguration> claimedClusters = getTaskExtension(task).getClaimedClusters();
                    claimedClusters.forEach(ElasticsearchConfiguration::claim);
            })
        );

        // create the listener to start the clusters on-demand and terminate as soon as  no longer claimed.
        // we need to use a task execution listener, as well as an action listener, so we don't start the task that
        // claimed it is up to date.
        project.getGradle().addListener(new ClusterFormationTaskExecutionListener());
    }

    private static File getArtifactsDir(Project project) {
        return new File(project.getRootProject().getBuildDir(), "clusterformation-artifacts");
    }

    public static File getArtifact(File sharedArtifactsDir, Distribution distro, Version version) {
        return new File(sharedArtifactsDir, distro.getFileName() + "-" + version + "." + distro.getExtension());
    }

    static ClusterFormationTaskExtension getTaskExtension(Task task) {
        return task.getExtensions().getByType(ClusterFormationTaskExtension.class);
    }

}
