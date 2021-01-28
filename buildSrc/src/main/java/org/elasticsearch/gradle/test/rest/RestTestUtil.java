/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.test.rest;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.info.BuildParams;
import org.elasticsearch.gradle.test.RestIntegTestTask;
import org.elasticsearch.gradle.testclusters.ElasticsearchCluster;
import org.elasticsearch.gradle.testclusters.TestClustersPlugin;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.AbstractArchiveTask;
import org.gradle.api.tasks.bundling.Zip;

/**
 * Utility class to configure the necessary tasks and dependencies.
 */
public class RestTestUtil {

    private RestTestUtil() {}

    public static ElasticsearchCluster createTestCluster(Project project, SourceSet sourceSet) {
        // eagerly create the testCluster container so it is easily available for configuration
        @SuppressWarnings("unchecked")
        NamedDomainObjectContainer<ElasticsearchCluster> testClusters = (NamedDomainObjectContainer<ElasticsearchCluster>) project
            .getExtensions()
            .getByName(TestClustersPlugin.EXTENSION_NAME);
        return testClusters.create(sourceSet.getName());
    }

    /**
     * Creates a task with the source set name of type {@link RestIntegTestTask}
     */
    public static Provider<RestIntegTestTask> registerTask(Project project, SourceSet sourceSet) {
        // lazily create the test task
        Provider<RestIntegTestTask> testProvider = project.getTasks().register(sourceSet.getName(), RestIntegTestTask.class, testTask -> {
            testTask.setGroup(JavaBasePlugin.VERIFICATION_GROUP);
            testTask.setDescription("Runs the REST tests against an external cluster");
            testTask.mustRunAfter(project.getTasks().named("test"));
            testTask.setTestClassesDirs(sourceSet.getOutput().getClassesDirs());
            testTask.setClasspath(sourceSet.getRuntimeClasspath());
            // if this a module or plugin, it may have an associated zip file with it's contents, add that to the test cluster
            project.getPluginManager().withPlugin("elasticsearch.esplugin", plugin -> {
                TaskProvider<Zip> bundle = project.getTasks().withType(Zip.class).named("bundlePlugin");
                testTask.dependsOn(bundle);
                if (GradleUtils.isModuleProject(project.getPath())) {
                    testTask.getClusters().forEach(c -> c.module(bundle.flatMap(AbstractArchiveTask::getArchiveFile)));
                } else {
                    testTask.getClusters().forEach(c -> c.plugin(bundle.flatMap(AbstractArchiveTask::getArchiveFile)));
                }
            });
        });

        return testProvider;
    }

    /**
     * Setup the dependencies needed for the REST tests.
     */
    public static void setupDependencies(Project project, SourceSet sourceSet) {
        BuildParams.withInternalBuild(
            () -> { project.getDependencies().add(sourceSet.getImplementationConfigurationName(), project.project(":test:framework")); }
        ).orElse(() -> {
            project.getDependencies()
                .add(
                    sourceSet.getImplementationConfigurationName(),
                    "org.elasticsearch.test:framework:" + VersionProperties.getElasticsearch()
                );
        });
    }

}
