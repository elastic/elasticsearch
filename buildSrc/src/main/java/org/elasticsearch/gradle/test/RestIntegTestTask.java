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

package org.elasticsearch.gradle.test;

import org.elasticsearch.gradle.SystemPropertyCommandLineArgumentProvider;
import org.elasticsearch.gradle.testclusters.ElasticsearchCluster;
import org.elasticsearch.gradle.testclusters.RestTestRunnerTask;
import org.elasticsearch.gradle.testclusters.TestClustersPlugin;
import org.gradle.api.Action;
import org.gradle.api.DefaultTask;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Project;
import org.gradle.api.Task;

public class RestIntegTestTask extends DefaultTask {

    protected RestTestRunnerTask runner;
    private static final String TESTS_REST_CLUSTER = "tests.rest.cluster";
    private static final String TESTS_CLUSTER = "tests.cluster";
    private static final String TESTS_CLUSTER_NAME = "tests.clustername";

    public RestIntegTestTask() {
        Project project = getProject();
        String name = getName();
        runner = project.getTasks().create(name + "Runner", RestTestRunnerTask.class);
        super.dependsOn(runner);
        @SuppressWarnings("unchecked")
        NamedDomainObjectContainer<ElasticsearchCluster> testClusters = (NamedDomainObjectContainer<ElasticsearchCluster>) project
            .getExtensions()
            .getByName(TestClustersPlugin.EXTENSION_NAME);
        ElasticsearchCluster cluster = testClusters.create(name);
        runner.useCluster(cluster);
        runner.include("**/*IT.class");
        runner.systemProperty("tests.rest.load_packaged", Boolean.FALSE.toString());
        if (System.getProperty(TESTS_REST_CLUSTER) == null) {
            if (System.getProperty(TESTS_CLUSTER) != null || System.getProperty(TESTS_CLUSTER_NAME) != null) {
                throw new IllegalArgumentException(
                    String.format("%s, %s, and %s must all be null or non-null", TESTS_REST_CLUSTER, TESTS_CLUSTER, TESTS_CLUSTER_NAME)
                );
            }
            SystemPropertyCommandLineArgumentProvider runnerNonInputProperties = (SystemPropertyCommandLineArgumentProvider) runner
                .getExtensions()
                .getByName("nonInputProperties");
            runnerNonInputProperties.systemProperty(TESTS_REST_CLUSTER, () -> String.join(",", cluster.getAllHttpSocketURI()));
            runnerNonInputProperties.systemProperty(TESTS_CLUSTER, () -> String.join(",", cluster.getAllTransportPortURI()));
            runnerNonInputProperties.systemProperty(TESTS_CLUSTER_NAME, cluster::getName);
        } else {
            if (System.getProperty(TESTS_CLUSTER) == null || System.getProperty(TESTS_CLUSTER_NAME) == null) {
                throw new IllegalArgumentException(
                    String.format("%s, %s, and %s must all be null or non-null", TESTS_REST_CLUSTER, TESTS_CLUSTER, TESTS_CLUSTER_NAME)
                );
            }
        }
        // this must run after all projects have been configured, so we know any project
        // references can be accessed as a fully configured
        project.getGradle().projectsEvaluated(x -> {
            if (isEnabled() == false) {
                runner.setEnabled(false);
            }
        });
    }

    @Override
    public Task dependsOn(Object... dependencies) {
        runner.dependsOn(dependencies);
        for (Object dependency : dependencies) {
            if (dependency instanceof Fixture) {
                runner.finalizedBy(((Fixture) dependency).getStopTask());
            }
        }
        return this;
    }

    @Override
    public void setDependsOn(Iterable<?> dependencies) {
        runner.setDependsOn(dependencies);
        for (Object dependency : dependencies) {
            if (dependency instanceof Fixture) {
                runner.finalizedBy(((Fixture) dependency).getStopTask());
            }
        }
    }

    public void runner(Action<? super RestTestRunnerTask> configure) {
        configure.execute(runner);
    }
}
