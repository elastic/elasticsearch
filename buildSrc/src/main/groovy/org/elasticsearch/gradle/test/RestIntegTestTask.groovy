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
package org.elasticsearch.gradle.test

import org.elasticsearch.gradle.testclusters.ElasticsearchCluster
import org.elasticsearch.gradle.testclusters.RestTestRunnerTask
import org.gradle.api.DefaultTask
import org.gradle.api.Task
import org.gradle.api.tasks.testing.Test

/**
 * A wrapper task around setting up a cluster and running rest tests.
 */
class RestIntegTestTask extends DefaultTask {

    protected Test runner

    RestIntegTestTask() {
        runner = project.tasks.create("${name}Runner", RestTestRunnerTask.class)
        super.dependsOn(runner)

        ElasticsearchCluster cluster = project.testClusters.create(name)
        runner.useCluster cluster

        runner.include('**/*IT.class')
        runner.systemProperty('tests.rest.load_packaged', 'false')

        if (System.getProperty("tests.rest.cluster") == null) {
            if (System.getProperty("tests.cluster") != null) {
                throw new IllegalArgumentException("tests.rest.cluster and tests.cluster must both be null or non-null")
            }

            runner.nonInputProperties.systemProperty('tests.rest.cluster', "${-> cluster.allHttpSocketURI.join(",")}")
            runner.nonInputProperties.systemProperty('tests.cluster', "${-> cluster.transportPortURI}")
            runner.nonInputProperties.systemProperty('tests.clustername', "${-> cluster.getName()}")
        } else {
            if (System.getProperty("tests.cluster") == null) {
                throw new IllegalArgumentException("tests.rest.cluster and tests.cluster must both be null or non-null")
            }
            // an external cluster was specified and all responsibility for cluster configuration is taken by the user
            runner.systemProperty('tests.rest.cluster', System.getProperty("tests.rest.cluster"))
            runner.systemProperty('test.cluster', System.getProperty("tests.cluster"))
        }

        // this must run after all projects have been configured, so we know any project
        // references can be accessed as a fully configured
        project.gradle.projectsEvaluated {
            if (enabled == false) {
                runner.enabled = false
                return // no need to add cluster formation tasks if the task won't run!
            }
        }
    }

    @Override
    public Task dependsOn(Object... dependencies) {
        runner.dependsOn(dependencies)
        for (Object dependency : dependencies) {
            if (dependency instanceof Fixture) {
                runner.finalizedBy(((Fixture)dependency).getStopTask())
            }
        }
        return this
    }

    @Override
    public void setDependsOn(Iterable<?> dependencies) {
        runner.setDependsOn(dependencies)
        for (Object dependency : dependencies) {
            if (dependency instanceof Fixture) {
                runner.finalizedBy(((Fixture)dependency).getStopTask())
            }
        }
    }

    public void runner(Closure configure) {
        project.tasks.getByName("${name}Runner").configure(configure)
    }

}
