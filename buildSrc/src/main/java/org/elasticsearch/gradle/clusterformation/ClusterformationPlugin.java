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
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

import java.io.File;
import java.util.List;

public class ClusterformationPlugin implements Plugin<Project> {

    public static final String LIST_TASK_NAME = "listElasticSearchClusters";
    public static final String EXTENSION_NAME = "elasticSearchClusters";
    public static final String TASK_EXTENSION_NAME = "clusterFormation";

    private static final String HELPER_CONFIGURATION_NAME = "_internalClusterFormationConfiguration";

    private final Logger logger =  Logging.getLogger(ClusterformationPlugin.class);

    @Override
    public void apply(Project project) {
        NamedDomainObjectContainer<? extends ElasticsearchConfiguration> container = project.container(
            ElasticsearchNode.class,
            (name) -> new ElasticsearchNode(name, GradleServicesAdapter.getInstance(project))
        );
        project.getExtensions().add(EXTENSION_NAME, container);

        Task listTask = project.getTasks().create(LIST_TASK_NAME);
        listTask.setGroup("ES cluster formation");
        listTask.setDescription("Lists all ES clusters configured for this project");
        listTask.doLast((Task task) ->
            container.forEach((ElasticsearchConfiguration cluster) ->
                logger.lifecycle("   * {}: {}", cluster.getName(), cluster.getDistribution())
            )
        );


        project.getRootProject().getConfigurations().create(HELPER_CONFIGURATION_NAME);
        getHelperConfiguration(project).setDescription("Internal helper configuration used by cluster configuration to download " +
            "ES distributions and plugins");
        project.getRepositories().add(
            project.getRepositories().mavenCentral()
        );
        // stage any artifacts needed by cluster formation.
        // this causes a download if the  artifacts are not in the Gradle cache, but that is no different than Gradle
        // dependencies in general
        // TODO: do this depending on the cluster configuration containers
        // TODO: write test for multi project builds ( multiple applies ) - and fix accordingly
        project.getRootProject().getDependencies().add(
            HELPER_CONFIGURATION_NAME,
            "org.elasticsearch.distribution.zip:elasticsearch:6.2.4@zip"
        );

        // register an extension for all current and future tasks, so that any task can declare that it wants to use a
        // specific cluster.
        project.getTasks().all((Task task) ->
            task.getExtensions().create(TASK_EXTENSION_NAME, ClusterFormationTaskExtension.class, task)
        );

        // Make sure we only claim the clusters for the tasks that will actually execute
        project.getGradle().getTaskGraph().whenReady(taskExecutionGraph ->
            taskExecutionGraph.getAllTasks().forEach(task ->
                {
                    List<ElasticsearchConfiguration> claimedClusters = getTaskExtension(task).getClaimedClusters();
                    claimedClusters.forEach(ElasticsearchConfiguration::claim);
                    // Stage the artifacts before tasks execute.
                    // TODO: this should be a task every other task that uses cluster formation depends on to benefit from up to date checks
                    project.sync(spec -> {
                        spec.from(getHelperConfiguration(project));
                        spec.into(new File(project.getRootProject().getBuildDir(), "clusterformation-artifacts"));
                    });
                }
            )
        );

        // create the listener to start the clusters on-demand and terminate when no longer claimed.
        // we need to use a task execution listener, as well as an action listener, so we don't start the task that
        // claimed it is up to date.
        project.getGradle().addListener(new ClusterFormationTaskExecutionListener());
    }

    static ClusterFormationTaskExtension getTaskExtension(Task task) {
        return task.getExtensions().getByType(ClusterFormationTaskExtension.class);
    }

    static Configuration getHelperConfiguration(Project project) {
        return project.getRootProject().getConfigurations().getByName(HELPER_CONFIGURATION_NAME);
    }

}
