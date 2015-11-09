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
import org.gradle.api.InvalidUserDataException
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.file.FileCollection
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.Delete
import org.gradle.api.tasks.Exec

/**
 * A helper for creating tasks to build a cluster that is used by a task, and tear down the cluster when the task is finished.
 */
class ClusterFormationTasks {

    /**
     * Adds dependent tasks to the given task to start and stop a cluster with the given configuration.
     */
    static void setup(Project project, Task task, ClusterConfiguration config) {
        if (task.getEnabled() == false) {
            // no need to add cluster formation tasks if the task won't run!
            return
        }
        configureDistributionDependency(project, config.distribution)
        for (int i = 0; i < config.numNodes; ++i) {
            File nodeDir = new File(project.buildDir, "cluster/${task.name} node${i}")
            configureTasks(project, task, config, nodeDir)
        }
    }

    /** Adds a dependency on the given distribution */
    static void configureDistributionDependency(Project project, String distro) {
        String elasticsearchVersion = ElasticsearchProperties.version
        String packaging = distro == 'tar' ? 'tar.gz' : distro
        project.configurations {
            elasticsearchDistro
        }
        project.dependencies {
            elasticsearchDistro "org.elasticsearch.distribution.${distro}:elasticsearch:${elasticsearchVersion}@${packaging}"
        }
    }

    /**
     * Adds dependent tasks to start an elasticsearch cluster before the given task is executed,
     * and stop it after it has finished executing.
     *
     * The setup of the cluster involves the following:
     * <ol>
     *   <li>Cleanup the extraction directory</li>
     *   <li>Extract a fresh copy of elasticsearch</li>
     *   <li>Write an elasticsearch.yml config file</li>
     *   <li>Copy plugins that will be installed to a temporary dir (which contains spaces)</li>
     *   <li>Install plugins</li>
     *   <li>Run additional setup commands</li>
     *   <li>Start elasticsearch<li>
     * </ol>
     */
    static void configureTasks(Project project, Task task, ClusterConfiguration config, File baseDir) {
        String clusterName = "${task.path.replace(':', '_').substring(1)}"
        File pidFile = pidFile(baseDir)
        File home = homeDir(baseDir, config.distribution)
        File cwd = new File(baseDir, "cwd")
        File pluginsTmpDir = new File(baseDir, "plugins tmp")

        // tasks are chained so their execution order is maintained
        Task setup = project.tasks.create(name: "${task.name}#clean", type: Delete, dependsOn: task.dependsOn.collect()) {
            delete home
            doLast {
                cwd.mkdirs()
            }
        }
        setup = configureCheckPreviousTask("${task.name}#checkPrevious", project, setup, pidFile)
        setup = configureStopTask("${task.name}#stopPrevious", project, setup, pidFile)
        setup = configureExtractTask("${task.name}#extract", project, setup, baseDir, config.distribution)
        setup = configureWriteConfigTask("${task.name}#configure", project, setup, home, config, clusterName, pidFile)
        setup = configureCopyPluginsTask("${task.name}#copyPlugins", project, setup, pluginsTmpDir, config)

        // install plugins
        for (Map.Entry<String, FileCollection> plugin : config.plugins.entrySet()) {
            // replace every dash followed by a character with just the uppercase character
            String camelName = plugin.getKey().replaceAll(/-(\w)/) { _, c -> c.toUpperCase(Locale.ROOT) }
            String taskName = "${task.name}#install${camelName[0].toUpperCase(Locale.ROOT) + camelName.substring(1)}"
            // delay reading the file location until execution time by wrapping in a closure within a GString
            String file = "${ -> new File(pluginsTmpDir, plugin.getValue().singleFile.getName()).toURI().toURL().toString() }"
            Object[] args = [new File(home, 'bin/plugin'), 'install', file]
            setup = configureExecTask(taskName, project, setup, cwd, args)
        }

        // extra setup commands
        for (Map.Entry<String, Object[]> command : config.setupCommands.entrySet()) {
            setup = configureExecTask("${task.name}#${command.getKey()}", project, setup, cwd, command.getValue())
        }

        Task start = configureStartTask("${task.name}#start", project, setup, cwd, config, clusterName, pidFile, home)
        task.dependsOn(start)

        Task stop = configureStopTask("${task.name}#stop", project, [], pidFile)
        task.finalizedBy(stop)
    }

    /** Adds a task to extract the elasticsearch distribution */
    static Task configureExtractTask(String name, Project project, Task setup, File baseDir, String distro) {
        List extractDependsOn = [project.configurations.elasticsearchDistro, setup]
        Task extract
        switch (distro) {
            case 'zip':
                extract = project.tasks.create(name: name, type: Copy, dependsOn: extractDependsOn) {
                    from { project.zipTree(project.configurations.elasticsearchDistro.singleFile) }
                    into baseDir
                }
                break;
            case 'tar':
                extract = project.tasks.create(name: name, type: Copy, dependsOn: extractDependsOn) {
                    from {
                        project.tarTree(project.resources.gzip(project.configurations.elasticsearchDistro.singleFile))
                    }
                    into baseDir
                }
                break;
            default:
                throw new InvalidUserDataException("Unknown distribution: ${distro}")
        }
        return extract
    }

    /** Adds a task to write elasticsearch.yml for the given node configuration */
    static Task configureWriteConfigTask(String name, Project project, Task setup, File home, ClusterConfiguration config, String clusterName, File pidFile) {
        Map esConfig = [
            'cluster.name'                    : clusterName,
            'http.port'                       : config.httpPort,
            'transport.tcp.port'              : config.transportPort,
            'pidfile'                         : pidFile,
            // TODO: make this work for multi node!
            'discovery.zen.ping.unicast.hosts': "localhost:${config.transportPort}",
            'path.repo'                       : "${home}/repo",
            'path.shared_data'                : "${home}/../",
            // Define a node attribute so we can test that it exists
            'node.testattr'                   : 'test',
            'repositories.url.allowed_urls'   : 'http://snapshot.test*'
        ]

        return project.tasks.create(name: name, type: DefaultTask, dependsOn: setup) << {
            File configFile = new File(home, 'config/elasticsearch.yml')
            logger.info("Configuring ${configFile}")
            configFile.setText(esConfig.collect { key, value -> "${key}: ${value}" }.join('\n'), 'UTF-8')
        }
    }

    /** Adds a task to copy plugins to a temp dir, which they will later be installed from. */
    static Task configureCopyPluginsTask(String name, Project project, Task setup, File pluginsTmpDir, ClusterConfiguration config) {
        if (config.plugins.isEmpty()) {
            return setup
        }
        // collect the files for plugins into a list, but wrap each in a closure to delay
        // looking for the filename until execution time
        List files = config.plugins.values().collect { plugin -> return { plugin.singleFile } }
        return project.tasks.create(name: name, type: Copy, dependsOn: setup) {
            into pluginsTmpDir
            from(*files) // spread the list into varargs
        }
    }

    /** Adds a task to execute a command to help setup the cluster */
    static Task configureExecTask(String name, Project project, Task setup, File cwd, Object[] execArgs) {
        return project.tasks.create(name: name, type: Exec, dependsOn: setup) {
            workingDir cwd
            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                executable 'cmd'
                args '/C', 'call'
            } else {
                executable 'sh'
            }
            args execArgs
            // only show output on failure, when not in info or debug mode
            if (logger.isInfoEnabled() == false) {
                standardOutput = new ByteArrayOutputStream()
                errorOutput = standardOutput
                ignoreExitValue = true
                doLast {
                    if (execResult.exitValue != 0) {
                        logger.error(standardOutput.toString())
                        throw new GradleException("Process '${execArgs.join(' ')}' finished with non-zero exit value ${execResult.exitValue}")
                    }
                }
            }
        }
    }

    /** Adds a task to start an elasticsearch node with the given configuration */
    static Task configureStartTask(String name, Project project, Task setup, File cwd, ClusterConfiguration config, String clusterName, File pidFile, File home) {
        Map esEnv = [
            'JAVA_HOME' : System.getProperty('java.home'),
            'ES_GC_OPTS': config.jvmArgs
        ]
        List esProps = config.systemProperties.collect { key, value -> "-D${key}=${value}" }
        for (Map.Entry<String, String> property : System.properties.entrySet()) {
            if (property.getKey().startsWith('es.')) {
                esProps.add("-D${property.getKey()}=${property.getValue()}")
            }
        }

        Closure esPostStartActions = { ant, logger ->
            ant.waitfor(maxwait: '30', maxwaitunit: 'second', checkevery: '500', checkeveryunit: 'millisecond', timeoutproperty: "failed${name.capitalize()}") {
                and {
                    resourceexists {
                        file file: pidFile.toString()
                    }
                    http(url: "http://localhost:${config.httpPort}")
                }
            }
            if (ant.properties.containsKey("failed${name}".toString())) {
                File logFile = new File(home, "logs/${clusterName}.log")
                if (logFile.exists()) {
                    logFile.eachLine { line -> logger.error(line) }
                }
                throw new GradleException('Failed to start elasticsearch')
            }
        }
        File esScript = new File(home, 'bin/elasticsearch')

        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            // elasticsearch.bat is spawned as it has no daemon mode
            return project.tasks.create(name: name, type: DefaultTask, dependsOn: setup) << {
                // Fall back to Ant exec task as Gradle Exec task does not support spawning yet
                ant.exec(executable: 'cmd', spawn: true, dir: cwd) {
                    esEnv.each { key, value -> env(key: key, value: value) }
                    (['/C', 'call', esScript] + esProps).each { arg(value: it) }
                }
                esPostStartActions(ant, logger)
            }
        } else {
            return project.tasks.create(name: name, type: Exec, dependsOn: setup) {
                workingDir cwd
                executable 'sh'
                args esScript, '-d' // daemonize!
                args esProps
                environment esEnv
                errorOutput = new ByteArrayOutputStream()
                doLast {
                    if (errorOutput.toString().isEmpty() == false) {
                        logger.error(errorOutput.toString())
                        File logFile = new File(home, "logs/${clusterName}.log")
                        if (logFile.exists()) {
                            logFile.eachLine { line -> logger.error(line) }
                        }
                        throw new GradleException('Failed to start elasticsearch')
                    }
                    esPostStartActions(ant, logger)
                }
            }
        }
    }

    /** Adds a task to check if the process with the given pidfile is actually elasticsearch */
    static Task configureCheckPreviousTask(String name, Project project, Object depends, File pidFile) {
        return project.tasks.create(name: name, type: Exec, dependsOn: depends) {
            onlyIf { pidFile.exists() }
            // the pid file won't actually be read until execution time, since the read is wrapped within an inner closure of the GString
            ext.pid = "${ -> pidFile.getText('UTF-8').trim()}"
            commandLine new File(System.getenv('JAVA_HOME'), 'bin/jps'), '-l'
            standardOutput = new ByteArrayOutputStream()
            doLast {
                String out = standardOutput.toString()
                if (out.contains("${pid} org.elasticsearch.bootstrap.Elasticsearch") == false) {
                    logger.error('jps -l')
                    logger.error(out)
                    logger.error("pid file: ${pidFile}")
                    logger.error("pid: ${pid}")
                    throw new GradleException("jps -l did not report any process with org.elasticsearch.bootstrap.Elasticsearch\n" +
                            "Did you run gradle clean? Maybe an old pid file is still lying around.")
                } else {
                    logger.info(out)
                }
            }
        }
    }

    /** Adds a task to kill an elasticsearch node with the given pidfile */
    static Task configureStopTask(String name, Project project, Object depends, File pidFile) {
        return project.tasks.create(name: name, type: Exec, dependsOn: depends) {
            onlyIf { pidFile.exists() }
            // the pid file won't actually be read until execution time, since the read is wrapped within an inner closure of the GString
            ext.pid = "${ -> pidFile.getText('UTF-8').trim()}"
            doFirst {
                logger.info("Shutting down external node with pid ${pid}")
            }
            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                executable 'Taskkill'
                args '/PID', pid, '/F'
            } else {
                executable 'kill'
                args '-9', pid
            }
            doLast {
                project.delete(pidFile)
            }
        }
    }

    /** Returns the directory elasticsearch home is contained in for the given distribution */
    static File homeDir(File baseDir, String distro) {
        String path
        switch (distro) {
            case 'zip':
            case 'tar':
                path = "elasticsearch-${ElasticsearchProperties.version}"
                break;
            default:
                throw new InvalidUserDataException("Unknown distribution: ${distro}")
        }
        return new File(baseDir, path)
    }

    static File pidFile(File dir) {
        return new File(dir, 'es.pid')
    }
}