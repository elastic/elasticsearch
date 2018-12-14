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
import org.elasticsearch.gradle.VersionProperties
import org.gradle.api.DefaultTask
import org.gradle.api.Task
import org.gradle.api.execution.TaskExecutionAdapter
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.TaskState
import org.gradle.api.tasks.options.Option
import org.gradle.plugins.ide.idea.IdeaPlugin

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.stream.Stream
/**
 * A wrapper task around setting up a cluster and running rest tests.
 */
public class RestIntegTestTask extends DefaultTask {

    protected ClusterConfiguration clusterConfig

    protected RandomizedTestingTask runner

    protected Task clusterInit

    /** Info about nodes in the integ test cluster. Note this is *not* available until runtime. */
    List<NodeInfo> nodes

    /** Flag indicating whether the rest tests in the rest spec should be run. */
    @Input
    Boolean includePackaged = false

    public RestIntegTestTask() {
        runner = project.tasks.create("${name}Runner", RandomizedTestingTask.class)
        super.dependsOn(runner)
        clusterInit = project.tasks.create(name: "${name}Cluster#init", dependsOn: project.testClasses)
        runner.dependsOn(clusterInit)
        runner.classpath = project.sourceSets.test.runtimeClasspath
        runner.testClassesDirs = project.sourceSets.test.output.classesDirs
        clusterConfig = project.extensions.create("${name}Cluster", ClusterConfiguration.class, project)

        // override/add more for rest tests
        runner.parallelism = '1'
        runner.include('**/*IT.class')
        runner.systemProperty('tests.rest.load_packaged', 'false')

        if (System.getProperty("tests.rest.cluster") == null) {
            if (System.getProperty("tests.cluster") != null) {
                throw new IllegalArgumentException("tests.rest.cluster and tests.cluster must both be null or non-null")
            }
            // we pass all nodes to the rest cluster to allow the clients to round-robin between them
            // this is more realistic than just talking to a single node
            runner.systemProperty('tests.rest.cluster', "${-> nodes.collect{it.httpUri()}.join(",")}")
            runner.systemProperty('tests.config.dir', "${-> nodes[0].pathConf}")
            // TODO: our "client" qa tests currently use the rest-test plugin. instead they should have their own plugin
            // that sets up the test cluster and passes this transport uri instead of http uri. Until then, we pass
            // both as separate sysprops
            runner.systemProperty('tests.cluster', "${-> nodes[0].transportUri()}")

            // dump errors and warnings from cluster log on failure
            TaskExecutionAdapter logDumpListener = new TaskExecutionAdapter() {
                @Override
                void afterExecute(Task task, TaskState state) {
                    if (state.failure != null) {
                        for (NodeInfo nodeInfo : nodes) {
                            printLogExcerpt(nodeInfo)
                        }
                    }
                }
            }
            runner.doFirst {
                project.gradle.addListener(logDumpListener)
            }
            runner.doLast {
                project.gradle.removeListener(logDumpListener)
            }
        } else {
            if (System.getProperty("tests.cluster") == null) {
                throw new IllegalArgumentException("tests.rest.cluster and tests.cluster must both be null or non-null")
            }
            // an external cluster was specified and all responsibility for cluster configuration is taken by the user
            runner.systemProperty('tests.rest.cluster', System.getProperty("tests.rest.cluster"))
            runner.systemProperty('test.cluster', System.getProperty("tests.cluster"))
        }

        // copy the rest spec/tests into the test resources
        Task copyRestSpec = createCopyRestSpecTask()
        runner.dependsOn(copyRestSpec)
        
        // this must run after all projects have been configured, so we know any project
        // references can be accessed as a fully configured
        project.gradle.projectsEvaluated {
            if (enabled == false) {
                runner.enabled = false
                clusterInit.enabled = false
                return // no need to add cluster formation tasks if the task won't run!
            }
            // only create the cluster if needed as otherwise an external cluster to use was specified
            if (System.getProperty("tests.rest.cluster") == null) {
                nodes = ClusterFormationTasks.setup(project, "${name}Cluster", runner, clusterConfig)
            }
            super.dependsOn(runner.finalizedBy)
        }
    }

    /** Sets the includePackaged property */
    public void includePackaged(boolean include) {
        includePackaged = include
    }

    @Option(
        option = "debug-jvm",
        description = "Enable debugging configuration, to allow attaching a debugger to elasticsearch."
    )
    public void setDebug(boolean enabled) {
        clusterConfig.debug = enabled;
    }

    public List<NodeInfo> getNodes() {
        return nodes
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

    @Override
    public Task mustRunAfter(Object... tasks) {
        clusterInit.mustRunAfter(tasks)
    }

    /** Print out an excerpt of the log from the given node. */
    protected static void printLogExcerpt(NodeInfo nodeInfo) {
        File logFile = new File(nodeInfo.homeDir, "logs/${nodeInfo.clusterName}.log")
        println("\nCluster ${nodeInfo.clusterName} - node ${nodeInfo.nodeNum} log excerpt:")
        println("(full log at ${logFile})")
        println('-----------------------------------------')
        Stream<String> stream = Files.lines(logFile.toPath(), StandardCharsets.UTF_8)
        try {
            boolean inStartup = true
            boolean inExcerpt = false
            int linesSkipped = 0
            for (String line : stream) {
                if (line.startsWith("[")) {
                    inExcerpt = false // clear with the next log message
                }
                if (line =~ /(\[WARN *\])|(\[ERROR *\])/) {
                    inExcerpt = true // show warnings and errors
                }
                if (inStartup || inExcerpt) {
                    if (linesSkipped != 0) {
                        println("... SKIPPED ${linesSkipped} LINES ...")
                    }
                    println(line)
                    linesSkipped = 0
                } else {
                    ++linesSkipped
                }
                if (line =~ /recovered \[\d+\] indices into cluster_state/) {
                    inStartup = false
                }
            }
        } finally {
            stream.close()
        }
        println('=========================================')

    }

    /**
     * Creates a task (if necessary) to copy the rest spec files.
     *
     * @param project The project to add the copy task to
     * @param includePackagedTests true if the packaged tests should be copied, false otherwise
     */
    Task createCopyRestSpecTask() {
        project.configurations {
            restSpec
        }
        project.dependencies {
            restSpec "org.elasticsearch:rest-api-spec:${VersionProperties.elasticsearch}"
        }
        Task copyRestSpec = project.tasks.findByName('copyRestSpec')
        if (copyRestSpec != null) {
            return copyRestSpec
        }
        Map copyRestSpecProps = [
                name     : 'copyRestSpec',
                type     : Copy,
                dependsOn: [project.configurations.restSpec, 'processTestResources']
        ]
        copyRestSpec = project.tasks.create(copyRestSpecProps) {
            into project.sourceSets.test.output.resourcesDir
        }
        project.afterEvaluate {
            copyRestSpec.from({ project.zipTree(project.configurations.restSpec.singleFile) }) {
                include 'rest-api-spec/api/**'
                if (includePackaged) {
                    include 'rest-api-spec/test/**'
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
        return copyRestSpec
    }
}
