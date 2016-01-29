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

import com.carrotsearch.gradle.junit4.RandomizedTestingTask
import org.elasticsearch.gradle.BuildPlugin
import org.gradle.api.Task
import org.gradle.api.internal.tasks.options.Option
import org.gradle.api.plugins.JavaBasePlugin
import org.gradle.api.tasks.Input
import org.gradle.util.ConfigureUtil

/**
 * Runs integration tests, but first starts an ES cluster,
 * and passes the ES cluster info as parameters to the tests.
 */
public class RestIntegTestTask extends RandomizedTestingTask {

    ClusterConfiguration clusterConfig = new ClusterConfiguration()

    /** Flag indicating whether the rest tests in the rest spec should be run. */
    @Input
    boolean includePackaged = false

    public RestIntegTestTask() {
        description = 'Runs rest tests against an elasticsearch cluster.'
        group = JavaBasePlugin.VERIFICATION_GROUP
        dependsOn(project.testClasses)
        classpath = project.sourceSets.test.runtimeClasspath
        testClassesDir = project.sourceSets.test.output.classesDir

        // start with the common test configuration
        configure(BuildPlugin.commonTestConfig(project))
        // override/add more for rest tests
        parallelism = '1'
        include('**/*IT.class')
        systemProperty('tests.rest.load_packaged', 'false')

        // copy the rest spec/tests into the test resources
        RestSpecHack.configureDependencies(project)
        project.afterEvaluate {
            dependsOn(RestSpecHack.configureTask(project, includePackaged))
        }
        // this must run after all projects have been configured, so we know any project
        // references can be accessed as a fully configured
        project.gradle.projectsEvaluated {
            NodeInfo node = ClusterFormationTasks.setup(project, this, clusterConfig)
            systemProperty('tests.rest.cluster', "${-> node.httpUri()}")
            // TODO: our "client" qa tests currently use the rest-test plugin. instead they should have their own plugin
            // that sets up the test cluster and passes this transport uri instead of http uri. Until then, we pass
            // both as separate sysprops
            systemProperty('tests.cluster', "${-> node.transportUri()}")
        }
    }

    @Option(
        option = "debug-jvm",
        description = "Enable debugging configuration, to allow attaching a debugger to elasticsearch."
    )
    public void setDebug(boolean enabled) {
        clusterConfig.debug = enabled;
    }

    @Input
    public void cluster(Closure closure) {
        ConfigureUtil.configure(closure, clusterConfig)
    }

    public ClusterConfiguration getCluster() {
        return clusterConfig
    }

    @Override
    public Task dependsOn(Object... dependencies) {
        super.dependsOn(dependencies)
        for (Object dependency : dependencies) {
            if (dependency instanceof Fixture) {
                finalizedBy(((Fixture)dependency).stopTask)
            }
        }
        return this
    }

    @Override
    public void setDependsOn(Iterable<?> dependencies) {
        super.setDependsOn(dependencies)
        for (Object dependency : dependencies) {
            if (dependency instanceof Fixture) {
                finalizedBy(((Fixture)dependency).stopTask)
            }
        }
    }
}
