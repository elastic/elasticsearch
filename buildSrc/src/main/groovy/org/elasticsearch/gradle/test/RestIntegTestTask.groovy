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
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.internal.tasks.options.Option
import org.gradle.api.plugins.JavaBasePlugin
import org.gradle.api.tasks.Input
import org.gradle.util.ConfigureUtil

/**
 * Runs integration tests, but first starts an ES cluster,
 * and passes the ES cluster info as parameters to the tests.
 */
class RestIntegTestTask extends RandomizedTestingTask {

    ClusterConfiguration clusterConfig = new ClusterConfiguration()

    @Input
    boolean includePackaged = false

    static RestIntegTestTask configure(Project project) {
        Map integTestOptions = [
            name: 'integTest',
            type: RestIntegTestTask,
            dependsOn: 'testClasses',
            group: JavaBasePlugin.VERIFICATION_GROUP,
            description: 'Runs rest tests against an elasticsearch cluster.'
        ]
        RestIntegTestTask integTest = project.tasks.create(integTestOptions)
        integTest.configure(BuildPlugin.commonTestConfig(project))
        integTest.configure {
            include '**/*IT.class'
            systemProperty 'tests.rest.load_packaged', 'false'
        }
        RandomizedTestingTask test = project.tasks.findByName('test')
        if (test != null) {
            integTest.classpath = test.classpath
            integTest.testClassesDir = test.testClassesDir
            integTest.mustRunAfter(test)
        }
        project.check.dependsOn(integTest)
        RestSpecHack.configureDependencies(project)
        project.afterEvaluate {
            integTest.dependsOn(RestSpecHack.configureTask(project, integTest.includePackaged))
        }
        return integTest
    }

    RestIntegTestTask() {
        project.afterEvaluate {
            Task test = project.tasks.findByName('test')
            if (test != null) {
                mustRunAfter(test)
            }
            ClusterFormationTasks.setup(project, this, clusterConfig)
            configure {
                parallelism '1'
                systemProperty 'tests.cluster', "localhost:${clusterConfig.transportPort}"
            }
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
    void cluster(Closure closure) {
        ConfigureUtil.configure(closure, clusterConfig)
    }

    ClusterConfiguration getCluster() {
        return clusterConfig
    }
}
