package org.elasticsearch.gradle

import org.apache.maven.BuildFailureException
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.Exec

/**
 * A helper for creating tasks to build a cluster that is used by a task, and tear down the cluster when the task is finished.
 */
class ClusterFormationTasks {

    /**
     * Adds dependent tasks to the given task to start a cluster with the given configuration.
     * Also adds a finalize task to stop the cluster.
     */
    static void addTasks(Task task, ClusterConfiguration config) {
        addZipConfiguration(task.project)
        File clusterDir = new File(task.project.buildDir, 'cluster' + File.separator + task.name)
        if (config.numNodes == 1) {
            addNodeStartupTasks(task, config, clusterDir)
            addNodeStopTask(task, clusterDir)
        } else {
            for (int i = 0; i < config.numNodes; ++i) {
                File nodeDir = new File(clusterDir, "node${i}")
                addNodeStartupTasks(task, config, nodeDir)
                addNodeStopTask(task, nodeDir)
            }
        }
    }

    static void addNodeStartupTasks(Task task, ClusterConfiguration config, File baseDir) {
        Project project = task.project
        Task unzip = project.tasks.create(name: task.name + '#unzip', type: Copy, dependsOn: project.configurations.elasticsearchZip.buildDependencies) {
            from project.zipTree(project.configurations.elasticsearchZip.asPath)
            into baseDir
        }
        File home = new File(baseDir, "elasticsearch-${ElasticsearchProperties.version}")
        String clusterName = "test${task.path.replace(':', '_')}"

        OutputStream startupOutput = new ByteArrayOutputStream()
        Task setup = unzip // chain setup tasks to maintain their order
        for (Map.Entry<String, String> command : config.setupConfig.commands.entrySet()) {
            Task nextSetup = project.tasks.create(name: "${task.name}#${command.getKey()}", type: Exec, dependsOn: setup) {
                workingDir home
                environment 'JAVA_HOME', System.getProperty('JAVA_HOME')
                executable 'sh'
                args command.getValue()
                standardOutput = startupOutput
                errorOutput = startupOutput
            }
            setup = nextSetup
        }

        Task start = project.tasks.create(name: "${task.name}#start", type: Exec, dependsOn: setup) {
            workingDir home
            executable 'sh'
            environment 'JAVA_HOME', System.getProperty('JAVA_HOME')
            args 'bin/elasticsearch',
                    '-d', // daemonize!
                    "-Des.cluster.name=${clusterName}",
                    "-Des.http.port=${config.httpPort}",
                    "-Des.transport.tcp.port=${config.transportPort}",
                    "-Des.pidfile=${pidFile(baseDir)}"
            doLast {
                task.ant.waitfor(maxwait: '30', maxwaitunit: 'second', checkevery: '500', checkeveryunit: 'millisecond', timeoutproperty: "failed${task.name}#start") {
                    http(url: "http://localhost:${config.httpPort}")
                }
                if (task.ant.properties.containsKey("failed${task.name}#start")) {
                    new File(home, 'logs' + File.separator + clusterName).eachLine {
                        line -> task.logger.error(line)
                    }
                    throw new BuildFailureException('Failed to start elasticsearch')
                }
            }
        }
        task.dependsOn(start)
    }

    static void addNodeStopTask(Task task, File baseDir) {
        LazyPidReader pidFile = new LazyPidReader(pidFile: pidFile(baseDir))
        Task stop = task.project.tasks.create(name: task.name + '#stop', type: Exec) {
            executable 'kill'
            args '-9', pidFile
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
            elasticsearchZip "org.elasticsearch.distributions.zip:elasticsearch:${elasticsearchVersion}"
        }
    }
}
