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

import org.apache.tools.ant.taskdefs.condition.Os
import org.elasticsearch.gradle.ElasticsearchProperties
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.Delete
import org.gradle.api.tasks.Exec

/**
 * A helper for creating tasks to build a cluster that is used by a task, and tear down the cluster when the task is finished.
 */
class ClusterFormationTasks {

    /**
     * Adds dependent tasks to the given task to start a cluster with the given configuration.
     * Also adds a finalize task to stop the cluster.
     */
    static void setup(Project project, Task task, ClusterConfiguration config) {
        if (task.getEnabled() == false) {
            // no need to cluster formation if the task won't run!
            return
        }
        addZipConfiguration(project)
        File clusterDir = new File(project.buildDir, 'cluster' + File.separator + task.name)
        if (config.numNodes == 1) {
            addNodeStartupTasks(project, task, config, clusterDir)
            addNodeStopTask(project, task, clusterDir)
        } else {
            for (int i = 0; i < config.numNodes; ++i) {
                File nodeDir = new File(clusterDir, "node${i}")
                addNodeStartupTasks(project, task, config, nodeDir)
                addNodeStopTask(project, task, nodeDir)
            }
        }
    }

    static void addNodeStartupTasks(Project project, Task task, ClusterConfiguration config, File baseDir) {
        String clusterName = "${task.path.replace(':', '_').substring(1)}"
        File home = new File(baseDir, "elasticsearch-${ElasticsearchProperties.version}")
        List setupDependsOn = [project.configurations.elasticsearchZip]
        setupDependsOn.addAll(task.dependsOn)
        Task setup = project.tasks.create(name: task.name + '#setup', type: Copy, dependsOn: setupDependsOn) {
            from { project.zipTree(project.configurations.elasticsearchZip.singleFile) }
            into baseDir
        }
        // chain setup tasks to maintain their order
        setup = project.tasks.create(name: "${task.name}#clean", type: Delete, dependsOn: setup) {
            delete new File(home, 'plugins'), new File(home, 'data'), new File(home, 'logs')
        }
        setup = project.tasks.create(name: "${task.name}#configure", type: DefaultTask, dependsOn: setup) << {
            File configFile = new File(home, 'config' + File.separator + 'elasticsearch.yml')
            logger.info("Configuring ${configFile}")
            configFile.setText("cluster.name: ${clusterName}", 'UTF-8')
        }
        for (Map.Entry<String, String> command : config.setupCommands.entrySet()) {
            Task nextSetup = project.tasks.create(name: "${task.name}#${command.getKey()}", type: Exec, dependsOn: setup) {
                workingDir home
                if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                    executable 'cmd'
                    args '/C', 'call'
                } else {
                    executable 'sh'
                }
                args command.getValue()
                // only show output on failure, when not in info or debug mode
                if (logger.isInfoEnabled() == false) {
                    standardOutput = new ByteArrayOutputStream()
                    errorOutput = standardOutput
                    ignoreExitValue = true
                    doLast {
                        if (execResult.exitValue != 0) {
                            logger.error(standardOutput.toString())
                            throw new GradleException("Process '${command.getValue().join(' ')}' finished with non-zero exit value ${execResult.exitValue}")
                        }
                    }
                }
            }
            setup = nextSetup
        }

        File pidFile = pidFile(baseDir)
        List esArgs = [
            "-Des.http.port=${config.httpPort}",
            "-Des.transport.tcp.port=${config.transportPort}",
            "-Des.pidfile=${pidFile}",
            "-Des.path.repo=${home}/repo",
            "-Des.path.shared_data=${home}/../",
        ]
        esArgs.addAll(config.systemProperties.collect {key, value -> "-D${key}=${value}"})
        Closure esPostStartActions = { ant, logger ->
            ant.waitfor(maxwait: '30', maxwaitunit: 'second', checkevery: '500', checkeveryunit: 'millisecond', timeoutproperty: "failed${task.name}#start") {
                and {
                    resourceexists {
                        file file: pidFile.toString()
                    }
                    http(url: "http://localhost:${config.httpPort}")
                }
            }
            if (ant.properties.containsKey("failed${task.name}#start".toString())) {
                new File(home, 'logs' + File.separator + clusterName + '.log').eachLine {
                    line -> logger.error(line)
                }
                throw new GradleException('Failed to start elasticsearch')
            }
        }
        Task start;
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            // elasticsearch.bat is spawned as it has no daemon mode
            start = project.tasks.create(name: "${task.name}#start", type: DefaultTask, dependsOn: setup) << {
                // Fall back to Ant exec task as Gradle Exec task does not support spawning yet
                ant.exec(executable: 'cmd', spawn: true, dir: home) {
                    (['/C', 'call', 'bin/elasticsearch'] + esArgs).each { arg(value: it) }
                }
                esPostStartActions(ant, logger)
            }
        } else {
            start = project.tasks.create(name: "${task.name}#start", type: Exec, dependsOn: setup) {
                workingDir home
                executable 'sh'
                args 'bin/elasticsearch', '-d' // daemonize!
                args esArgs
                errorOutput = new ByteArrayOutputStream()
                doLast {
                    if (errorOutput.toString().isEmpty() == false) {
                        logger.error(errorOutput.toString())
                        new File(home, 'logs' + File.separator + clusterName + '.log').eachLine {
                            line -> logger.error(line)
                        }
                        throw new GradleException('Failed to start elasticsearch')
                    }
                    esPostStartActions(ant, logger)
                }
            }
        }
        task.dependsOn(start)
    }

    static void addNodeStopTask(Project project, Task task, File baseDir) {
        LazyPidReader pidFile = new LazyPidReader(pidFile: pidFile(baseDir))
        Task stop = project.tasks.create(name: task.name + '#stop', type: Exec) {
            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                executable 'Taskkill'
                args '/PID', pidFile, '/F'
            } else {
                executable 'kill'
                args '-9', pidFile
            }
            doLast {
                // TODO: wait for pid to close, or kill -9 and fail
            }
        }
        task.finalizedBy(stop)
    }

    /** Delays reading a pid file until needing to use the pid */
    static class LazyPidReader {
        File pidFile
        @Override
        String toString() {
            return pidFile.text.stripMargin()
        }
    }

    static File pidFile(File dir) {
        return new File(dir, 'es.pid')
    }

    static void addZipConfiguration(Project project) {
        String elasticsearchVersion = ElasticsearchProperties.version
        project.configurations {
            elasticsearchZip
        }
        project.dependencies {
            elasticsearchZip "org.elasticsearch.distribution.zip:elasticsearch:${elasticsearchVersion}@zip"
        }
    }
}
