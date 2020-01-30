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

import org.elasticsearch.gradle.VersionProperties
import org.elasticsearch.gradle.info.BuildParams
import org.elasticsearch.gradle.testclusters.ElasticsearchCluster
import org.elasticsearch.gradle.testclusters.RestTestRunnerTask
import org.elasticsearch.gradle.tool.Boilerplate
import org.gradle.api.DefaultTask
import org.gradle.api.Task
import org.gradle.api.file.FileCopyDetails
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.testing.Test
import org.gradle.plugins.ide.idea.IdeaPlugin
/**
 * A wrapper task around setting up a cluster and running rest tests.
 */
class RestIntegTestTask extends DefaultTask {

    protected Test runner

    /** Flag indicating whether the rest tests in the rest spec should be run. */
    @Input
    Boolean includePackaged = false

    RestIntegTestTask() {
        runner = project.tasks.create("${name}Runner", RestTestRunnerTask.class)
        super.dependsOn(runner)

        ElasticsearchCluster cluster = project.testClusters.create(name)
        runner.useCluster cluster

        runner.include('**/*IT.class')
        runner.systemProperty('tests.rest.load_packaged', 'false')

        if (System.getProperty("tests.rest.cluster") == null) {
            if (System.getProperty("tests.cluster") != null || System.getProperty("tests.clustername") != null) {
                throw new IllegalArgumentException("tests.rest.cluster, tests.cluster, and tests.clustername must all be null or non-null")
            }

            runner.nonInputProperties.systemProperty('tests.rest.cluster', "${-> cluster.allHttpSocketURI.join(",")}")
            runner.nonInputProperties.systemProperty('tests.cluster', "${-> cluster.transportPortURI}")
            runner.nonInputProperties.systemProperty('tests.clustername', "${-> cluster.getName()}")
        } else {
            if (System.getProperty("tests.cluster") == null || System.getProperty("tests.clustername") == null) {
                throw new IllegalArgumentException("tests.rest.cluster, tests.cluster, and tests.clustername must all be null or non-null")
            }
            // an external cluster was specified and all responsibility for cluster configuration is taken by the user
            runner.systemProperty('tests.rest.cluster', System.getProperty("tests.rest.cluster"))
            runner.systemProperty('test.cluster', System.getProperty("tests.cluster"))
            runner.systemProperty('test.clustername', System.getProperty("tests.clustername"))
        }

        // copy the full rest spec (including modules, plugins, and x-pack) to a common location
        // optionally copy the core rest tests to project resource dir
        File currentCopyTo = new File(project.rootProject.buildDir, "rest-spec-api-current")
        runner.nonInputProperties.systemProperty('tests.rest.spec_root', currentCopyTo)
        Copy copyRestSpec = createCopyRestSpecTask(currentCopyTo)
        Copy copyModuleRestSpec = createCopyModulesRestSpecTask(currentCopyTo)
        Copy copyPluginRestSpec = createCopyPluginsRestSpecTask(currentCopyTo)
        Copy copyXpackPluginRestSpec = createCopyXpackPluginsRestSpecTask(currentCopyTo)
        copyModuleRestSpec.dependsOn(copyRestSpec)
        copyPluginRestSpec.dependsOn(copyRestSpec)
        copyXpackPluginRestSpec.dependsOn(copyRestSpec)
        project.sourceSets.test.output.builtBy(copyRestSpec)
        project.sourceSets.test.output.builtBy(copyModuleRestSpec)
        project.sourceSets.test.output.builtBy(copyPluginRestSpec)
        project.sourceSets.test.output.builtBy(copyXpackPluginRestSpec)

        // this must run after all projects have been configured, so we know any project
        // references can be accessed as a fully configured
        project.gradle.projectsEvaluated {
            if (enabled == false) {
                runner.enabled = false
                return // no need to add cluster formation tasks if the task won't run!
            }
        }
    }

    /** Sets the includePackaged property */
    public void includePackaged(boolean include) {
        includePackaged = include
    }

    @Override
    public Task dependsOn(Object... dependencies) {
        runner.dependsOn(dependencies)
        for (Object dependency : dependencies) {
            if (dependency instanceof Fixture) {
                runner.finalizedBy(((Fixture) dependency).getStopTask())
            }
        }
        return this
    }

    @Override
    public void setDependsOn(Iterable<?> dependencies) {
        runner.setDependsOn(dependencies)
        for (Object dependency : dependencies) {
            if (dependency instanceof Fixture) {
                runner.finalizedBy(((Fixture) dependency).getStopTask())
            }
        }
    }

    public void runner(Closure configure) {
        project.tasks.getByName("${name}Runner").configure(configure)
    }

    Copy createCopyRestSpecTask(File copyTo) {
        Boilerplate.maybeCreate(project.configurations, 'restSpec') {
            project.dependencies.add(
                    'restSpec',
                    BuildParams.internal ? project.project(':rest-api-spec') :
                            "org.elasticsearch:rest-api-spec:${VersionProperties.elasticsearch}"
            )
        }

        return Boilerplate.maybeCreate(project.tasks, 'copyRestSpec', Copy) { Copy copy ->
            copy.dependsOn project.configurations.restSpec
            copy.into(copyTo)
            copy.from({ project.zipTree(project.configurations.restSpec.singleFile) }) {
                includeEmptyDirs = false
                include 'rest-api-spec/**'
                filesMatching('rest-api-spec/test/**') { FileCopyDetails details ->
                    if (includePackaged) {
                        details.copyTo(new File(new File(project.sourceSets.test.output.resourcesDir, "rest-api-spec/test"), details.name))
                    }else{
                        details.exclude()
                    }
                }
            }

            if (project.plugins.hasPlugin(IdeaPlugin)) {
                project.idea {
                    module {
                        if (scopes.TEST != null) {
                            scopes.TEST.plus.add(project.configurations.restSpec)
                        }
                    }
                }
            }
        }
    }

    Copy createCopyModulesRestSpecTask(File copyTo) {
        return Boilerplate.maybeCreate(project.tasks, 'copyModulesRestSpecs', Copy) { Copy copy ->
            copy.into(new File(copyTo, "rest-api-spec/api"))
            copy.eachFile {
                path = name
            }
            copy.from({ project.findProject(':modules').projectDir }) {
                includeEmptyDirs = false
                include '**/src/**/rest-api-spec/api/**'
                exclude '**/examples/**'
            }
        }
    }

    Copy createCopyPluginsRestSpecTask(File copyTo) {
        return Boilerplate.maybeCreate(project.tasks, 'copyPluginsRestSpecs', Copy) { Copy copy ->
            copy.into(new File(copyTo, "rest-api-spec/api"))
            copy.eachFile {
                path = name
            }
            copy.from({ project.findProject(':plugins').projectDir }) {
                includeEmptyDirs = false
                include '**/src/**/rest-api-spec/api/**'
                exclude '**/examples/**'
            }
        }
    }

    Copy createCopyXpackPluginsRestSpecTask(File copyTo) {
        return Boilerplate.maybeCreate(project.tasks, 'copyXpackPluginsRestSpecs', Copy) { Copy copy ->
            copy.into(new File(copyTo, "rest-api-spec/api"))
            copy.eachFile {
                path = name
            }
            copy.from({ project.findProject(':x-pack:plugin').projectDir }) {
                includeEmptyDirs = false
                include '**/src/**/rest-api-spec/api/**'
                exclude '**/examples/**'
            }
        }
    }
}
