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

import org.apache.tools.ant.DefaultLogger
import org.apache.tools.ant.taskdefs.condition.Os
import org.elasticsearch.gradle.VersionProperties
import org.gradle.api.*
import org.gradle.api.file.FileCollection
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.Delete
import org.gradle.api.tasks.Exec

import java.nio.file.Paths

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
        String elasticsearchVersion = VersionProperties.elasticsearch
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
            delete cwd
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
            String taskName = "${task.name}#install${camelName[0].toUpperCase(Locale.ROOT) + camelName.substring(1)}Plugin"
            // delay reading the file location until execution time by wrapping in a closure within a GString
            String file = "${-> new File(pluginsTmpDir, plugin.getValue().singleFile.getName()).toURI().toURL().toString()}"
            Object[] args = [new File(home, 'bin/plugin'), 'install', file]
            setup = configureExecTask(taskName, project, setup, cwd, args)
        }

        // extra setup commands
        for (Map.Entry<String, Object[]> command : config.setupCommands.entrySet()) {
            setup = configureExecTask("${task.name}#${command.getKey()}", project, setup, cwd, command.getValue())
        }

        Task start = configureStartTask("${task.name}#start", project, setup, cwd, config, clusterName, pidFile, home)
        task.dependsOn(start)

        if (config.daemonize) {
            // if we are running in the background, make sure to stop the server when the task completes
            Task stop = configureStopTask("${task.name}#stop", project, [], pidFile)
            task.finalizedBy(stop)
        }
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

        return project.tasks.create(name: name, type: Copy, dependsOn: setup) {
            into pluginsTmpDir
            from(config.plugins.values())
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
            'JAVA_HOME' : project.javaHome,
            'ES_GC_OPTS': config.jvmArgs // we pass these with the undocumented gc opts so the argline can set gc, etc
        ]
        List<String> esProps = config.systemProperties.collect { key, value -> "-D${key}=${value}" }
        for (Map.Entry<String, String> property : System.properties.entrySet()) {
            if (property.getKey().startsWith('es.')) {
                esProps.add("-D${property.getKey()}=${property.getValue()}")
            }
        }

        String executable
        // running with cmd on windows will look for this with the .bat extension
        String esScript = new File(home, 'bin/elasticsearch').toString()
        List<String> esArgs = []
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            executable = 'cmd'
            esArgs.add('/C')
            esArgs.add('call')
        } else {
            executable = 'sh'
        }

        File failedMarker = new File(cwd, 'run.failed')

        // this closure is converted into ant nodes by groovy's AntBuilder
        Closure antRunner = {
            // we must add debug options inside the closure so the config is read at execution time, as
            // gradle task options are not processed until the end of the configuration phase
            if (config.debug) {
                println 'Running elasticsearch in debug mode, suspending until connected on port 8000'
                esEnv['JAVA_OPTS'] = '-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=8000'
            }

            // Due to how ant exec works with the spawn option, we lose all stdout/stderr from the
            // process executed. To work around this, when spawning, we wrap the elasticsearch start
            // command inside another shell script, which simply internally redirects the output
            // of the real elasticsearch script. This allows ant to keep the streams open with the
            // dummy process, but us to have the output available if there is an error in the
            // elasticsearch start script
            if (config.daemonize) {
                String scriptName = 'run'
                String argsPasser = '"$@"'
                String exitMarker = '; if [ $? != 0 ]; then touch run.failed; fi'
                if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                    scriptName += '.bat'
                    argsPasser = '%*'
                    exitMarker = '\r\n if "%errorlevel%" neq "0" ( type nul >> run.failed )'
                }
                File wrapperScript = new File(cwd, scriptName)
                wrapperScript.setText("\"${esScript}\" ${argsPasser} > run.log 2>&1 ${exitMarker}", 'UTF-8')
                esScript = wrapperScript.toString()
            }

            exec(executable: executable, spawn: config.daemonize, dir: cwd, taskname: 'elasticsearch') {
                esEnv.each { key, value -> env(key: key, value: value) }
                arg(value: esScript)
                esProps.each { arg(value: it) }
            }
            waitfor(maxwait: '30', maxwaitunit: 'second', checkevery: '500', checkeveryunit: 'millisecond', timeoutproperty: "failed${name}") {
                or {
                    resourceexists {
                        file(file: failedMarker.toString())
                    }
                    and {
                        resourceexists {
                            file(file: pidFile.toString())
                        }
                        http(url: "http://localhost:${config.httpPort}")
                    }
                }
            }
        }

        // this closure is the actual code to run elasticsearch
        Closure elasticsearchRunner = {
            // Command as string for logging
            String esCommandString = "Elasticsearch command: ${esScript} "
            esCommandString += esProps.join(' ')
            if (esEnv.isEmpty() == false) {
                esCommandString += '\nenvironment:'
                esEnv.each { k, v -> esCommandString += "\n  ${k}: ${v}" }
            }
            logger.info(esCommandString)

            ByteArrayOutputStream buffer = new ByteArrayOutputStream()
            if (logger.isInfoEnabled() || config.daemonize == false) {
                // run with piping streams directly out (even stderr to stdout since gradle would capture it)
                runAntCommand(project, antRunner, System.out, System.err)
            } else {
                // buffer the output, we may not need to print it
                PrintStream captureStream = new PrintStream(buffer, true, "UTF-8")
                runAntCommand(project, antRunner, captureStream, captureStream)
            }

            if (ant.properties.containsKey("failed${name}".toString()) || failedMarker.exists()) {
                if (logger.isInfoEnabled() == false) {
                    // We already log the command at info level. No need to do it twice.
                    esCommandString.eachLine { line -> logger.error(line) }
                }
                // the waitfor failed, so dump any output we got (may be empty if info logging, but that is ok)
                buffer.toString('UTF-8').eachLine { line -> logger.error(line) }
                // also dump the log file for the startup script (which will include ES logging output to stdout)
                File startLog = new File(cwd, 'run.log')
                if (startLog.exists()) {
                    startLog.eachLine { line -> logger.error(line) }
                }
                throw new GradleException('Failed to start elasticsearch')
            }
        }

        Task start = project.tasks.create(name: name, type: DefaultTask, dependsOn: setup)
        start.doLast(elasticsearchRunner)
        return start
    }

    /** Adds a task to check if the process with the given pidfile is actually elasticsearch */
    static Task configureCheckPreviousTask(String name, Project project, Object depends, File pidFile) {
        return project.tasks.create(name: name, type: Exec, dependsOn: depends) {
            onlyIf { pidFile.exists() }
            // the pid file won't actually be read until execution time, since the read is wrapped within an inner closure of the GString
            ext.pid = "${ -> pidFile.getText('UTF-8').trim()}"
            File jps
            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                jps = getJpsExecutableByName(project, "jps.exe")
            } else {
                jps = getJpsExecutableByName(project, "jps")
            }
            if (!jps.exists()) {
                throw new GradleException("jps executable not found; ensure that you're running Gradle with the JDK rather than the JRE")
            }
            commandLine jps, '-l'
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

    private static File getJpsExecutableByName(Project project, String jpsExecutableName) {
        return Paths.get(project.javaHome.toString(), "bin/" + jpsExecutableName).toFile()
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
                path = "elasticsearch-${VersionProperties.elasticsearch}"
                break;
            default:
                throw new InvalidUserDataException("Unknown distribution: ${distro}")
        }
        return new File(baseDir, path)
    }

    static File pidFile(File dir) {
        return new File(dir, 'es.pid')
    }

    /** Runs an ant command, sending output to the given out and error streams */
    static void runAntCommand(Project project, Closure command, PrintStream outputStream, PrintStream errorStream) {
        DefaultLogger listener = new DefaultLogger(
                errorPrintStream: errorStream,
                outputPrintStream: outputStream,
                messageOutputLevel: org.apache.tools.ant.Project.MSG_INFO)

        project.ant.project.addBuildListener(listener)
        project.configure(project.ant, command)
        project.ant.project.removeBuildListener(listener)
    }
}
