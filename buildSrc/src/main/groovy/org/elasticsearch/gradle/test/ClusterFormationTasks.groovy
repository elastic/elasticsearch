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
import org.elasticsearch.gradle.LoggedExec
import org.elasticsearch.gradle.VersionProperties
import org.elasticsearch.gradle.plugin.PluginBuildPlugin
import org.gradle.api.AntBuilder
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.InvalidUserDataException
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.artifacts.Configuration
import org.gradle.api.file.FileCollection
import org.gradle.api.logging.Logger
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
     *
     * Returns a NodeInfo object for the first node in the cluster.
     */
    static NodeInfo setup(Project project, Task task, ClusterConfiguration config) {
        if (task.getEnabled() == false) {
            // no need to add cluster formation tasks if the task won't run!
            return
        }
        File sharedDir = new File(project.buildDir, "cluster/shared")
        // first we remove everything in the shared cluster directory to ensure there are no leftovers in repos or anything
        // in theory this should not be necessary but repositories are only deleted in the cluster-state and not on-disk
        // such that snapshots survive failures / test runs and there is no simple way today to fix that.
        Task cleanup = project.tasks.create(name: "${task.name}#prepareCluster.cleanShared", type: Delete, dependsOn: task.dependsOn.collect()) {
            delete sharedDir
            doLast {
                sharedDir.mkdirs()
            }
        }
        List<Task> startTasks = [cleanup]
        List<NodeInfo> nodes = []
        if (config.numNodes < config.numBwcNodes) {
            throw new GradleException("numNodes must be >= numBwcNodes [${config.numNodes} < ${config.numBwcNodes}]")
        }
        if (config.numBwcNodes > 0 && config.bwcVersion == null) {
            throw new GradleException("bwcVersion must not be null if numBwcNodes is > 0")
        }
        // this is our current version distribution configuration we use for all kinds of REST tests etc.
        project.configurations {
            elasticsearchDistro
        }
        configureDistributionDependency(project, config.distribution, project.configurations.elasticsearchDistro, VersionProperties.elasticsearch)
        if (config.bwcVersion != null && config.numBwcNodes > 0) {
            // if we have a cluster that has a BWC cluster we also need to configure a dependency on the BWC version
            // this version uses the same distribution etc. and only differs in the version we depend on.
            // from here on everything else works the same as if it's the current version, we fetch the BWC version
            // from mirrors using gradles built-in mechanism etc.
            project.configurations {
                elasticsearchBwcDistro
            }
            configureDistributionDependency(project, config.distribution, project.configurations.elasticsearchBwcDistro, config.bwcVersion)
        }

        for (int i = 0; i < config.numNodes; ++i) {
            // we start N nodes and out of these N nodes there might be M bwc nodes.
            // for each of those nodes we might have a different configuratioon
            String elasticsearchVersion = VersionProperties.elasticsearch
            Configuration configuration = project.configurations.elasticsearchDistro
            if (i < config.numBwcNodes) {
                elasticsearchVersion = config.bwcVersion
                configuration = project.configurations.elasticsearchBwcDistro
            }
            NodeInfo node = new NodeInfo(config, i, project, task, elasticsearchVersion, sharedDir)
            if (i == 0) {
                if (config.seedNodePortsFile != null) {
                    // we might allow this in the future to be set but for now we are the only authority to set this!
                    throw new GradleException("seedNodePortsFile has a non-null value but first node has not been intialized")
                }
                config.seedNodePortsFile = node.transportPortsFile;
            }
            nodes.add(node)
            startTasks.add(configureNode(project, task, cleanup, node, configuration))
        }

        Task wait = configureWaitTask("${task.name}#wait", project, nodes, startTasks)
        task.dependsOn(wait)

        // delay the resolution of the uri by wrapping in a closure, so it is not used until read for tests
        return nodes[0]
    }

    /** Adds a dependency on the given distribution */
    static void configureDistributionDependency(Project project, String distro, Configuration configuration, String elasticsearchVersion) {
        String packaging = distro
        if (distro == 'tar') {
            packaging = 'tar.gz'
        } else if (distro == 'integ-test-zip') {
            packaging = 'zip'
        }
        project.dependencies.add(configuration.name, "org.elasticsearch.distribution.${distro}:elasticsearch:${elasticsearchVersion}@${packaging}")
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
     *
     * @return a task which starts the node.
     */
    static Task configureNode(Project project, Task task, Object dependsOn, NodeInfo node, Configuration configuration) {

        // tasks are chained so their execution order is maintained
        Task setup = project.tasks.create(name: taskName(task, node, 'clean'), type: Delete, dependsOn: dependsOn) {
            delete node.homeDir
            delete node.cwd
            doLast {
                node.cwd.mkdirs()
            }
        }
        setup = configureCheckPreviousTask(taskName(task, node, 'checkPrevious'), project, setup, node)
        setup = configureStopTask(taskName(task, node, 'stopPrevious'), project, setup, node)
        setup = configureExtractTask(taskName(task, node, 'extract'), project, setup, node, configuration)
        setup = configureWriteConfigTask(taskName(task, node, 'configure'), project, setup, node)
        setup = configureExtraConfigFilesTask(taskName(task, node, 'extraConfig'), project, setup, node)
        setup = configureCopyPluginsTask(taskName(task, node, 'copyPlugins'), project, setup, node)

        // install modules
        for (Project module : node.config.modules) {
            String actionName = pluginTaskName('install', module.name, 'Module')
            setup = configureInstallModuleTask(taskName(task, node, actionName), project, setup, node, module)
        }

        // install plugins
        for (Map.Entry<String, Object> plugin : node.config.plugins.entrySet()) {
            String actionName = pluginTaskName('install', plugin.getKey(), 'Plugin')
            setup = configureInstallPluginTask(taskName(task, node, actionName), project, setup, node, plugin.getValue())
        }

        // extra setup commands
        for (Map.Entry<String, Object[]> command : node.config.setupCommands.entrySet()) {
            // the first argument is the actual script name, relative to home
            Object[] args = command.getValue().clone()
            args[0] = new File(node.homeDir, args[0].toString())
            setup = configureExecTask(taskName(task, node, command.getKey()), project, setup, node, args)
        }

        Task start = configureStartTask(taskName(task, node, 'start'), project, setup, node)

        if (node.config.daemonize) {
            // if we are running in the background, make sure to stop the server when the task completes
            Task stop = configureStopTask(taskName(task, node, 'stop'), project, [], node)
            task.finalizedBy(stop)
        }
        return start
    }

    /** Adds a task to extract the elasticsearch distribution */
    static Task configureExtractTask(String name, Project project, Task setup, NodeInfo node, Configuration configuration) {
        List extractDependsOn = [configuration, setup]
        /* configuration.singleFile will be an external artifact if this is being run by a plugin not living in the
          elasticsearch source tree. If this is a plugin built in the elasticsearch source tree or this is a distro in
          the elasticsearch source tree then this should be the version of elasticsearch built by the source tree.
          If it isn't then Bad Things(TM) will happen. */
        Task extract

        switch (node.config.distribution) {
            case 'integ-test-zip':
            case 'zip':
                extract = project.tasks.create(name: name, type: Copy, dependsOn: extractDependsOn) {
                    from {
                        project.zipTree(configuration.singleFile)
                    }
                    into node.baseDir
                }
                break;
            case 'tar':
                extract = project.tasks.create(name: name, type: Copy, dependsOn: extractDependsOn) {
                    from {
                        project.tarTree(project.resources.gzip(configuration.singleFile))
                    }
                    into node.baseDir
                }
                break;
            case 'rpm':
                File rpmDatabase = new File(node.baseDir, 'rpm-database')
                File rpmExtracted = new File(node.baseDir, 'rpm-extracted')
                /* Delay reading the location of the rpm file until task execution */
                Object rpm = "${ -> configuration.singleFile}"
                extract = project.tasks.create(name: name, type: LoggedExec, dependsOn: extractDependsOn) {
                    commandLine 'rpm', '--badreloc', '--nodeps', '--noscripts', '--notriggers',
                        '--dbpath', rpmDatabase,
                        '--relocate', "/=${rpmExtracted}",
                        '-i', rpm
                    doFirst {
                        rpmDatabase.deleteDir()
                        rpmExtracted.deleteDir()
                    }
                }
                break;
            case 'deb':
                /* Delay reading the location of the deb file until task execution */
                File debExtracted = new File(node.baseDir, 'deb-extracted')
                Object deb = "${ -> configuration.singleFile}"
                extract = project.tasks.create(name: name, type: LoggedExec, dependsOn: extractDependsOn) {
                    commandLine 'dpkg-deb', '-x', deb, debExtracted
                    doFirst {
                        debExtracted.deleteDir()
                    }
                }
                break;
            default:
                throw new InvalidUserDataException("Unknown distribution: ${node.config.distribution}")
        }
        return extract
    }

    /** Adds a task to write elasticsearch.yml for the given node configuration */
    static Task configureWriteConfigTask(String name, Project project, Task setup, NodeInfo node) {
        Map esConfig = [
                'cluster.name'                 : node.clusterName,
                'pidfile'                      : node.pidFile,
                'path.repo'                    : "${node.sharedDir}/repo",
                'path.shared_data'             : "${node.sharedDir}/",
                // Define a node attribute so we can test that it exists
                'node.attr.testattr'                : 'test',
                'repositories.url.allowed_urls': 'http://snapshot.test*'
        ]
        esConfig['http.port'] = node.config.httpPort
        esConfig['transport.tcp.port'] =  node.config.transportPort
        esConfig.putAll(node.config.settings)

        Task writeConfig = project.tasks.create(name: name, type: DefaultTask, dependsOn: setup)
        writeConfig.doFirst {
            if (node.nodeNum > 0) { // multi-node cluster case, we have to wait for the seed node to startup
                ant.waitfor(maxwait: '20', maxwaitunit: 'second', checkevery: '500', checkeveryunit: 'millisecond') {
                    resourceexists {
                        file(file: node.config.seedNodePortsFile.toString())
                    }
                }
                // the seed node is enough to form the cluster - all subsequent nodes will get the seed node as a unicast
                // host and join the cluster via that.
                esConfig['discovery.zen.ping.unicast.hosts'] = "\"${node.config.seedNodeTransportUri()}\""
            }
            File configFile = new File(node.confDir, 'elasticsearch.yml')
            logger.info("Configuring ${configFile}")
            configFile.setText(esConfig.collect { key, value -> "${key}: ${value}" }.join('\n'), 'UTF-8')
        }
    }

    static Task configureExtraConfigFilesTask(String name, Project project, Task setup, NodeInfo node) {
        if (node.config.extraConfigFiles.isEmpty()) {
            return setup
        }
        Copy copyConfig = project.tasks.create(name: name, type: Copy, dependsOn: setup)
        File configDir = new File(node.homeDir, 'config')
        copyConfig.into(configDir) // copy must always have a general dest dir, even though we don't use it
        for (Map.Entry<String,Object> extraConfigFile : node.config.extraConfigFiles.entrySet()) {
            Object extraConfigFileValue = extraConfigFile.getValue()
            copyConfig.doFirst {
                // make sure the copy won't be a no-op or act on a directory
                File srcConfigFile = project.file(extraConfigFileValue)
                if (srcConfigFile.isDirectory()) {
                    throw new GradleException("Source for extraConfigFile must be a file: ${srcConfigFile}")
                }
                if (srcConfigFile.exists() == false) {
                    throw new GradleException("Source file for extraConfigFile does not exist: ${srcConfigFile}")
                }
            }
            File destConfigFile = new File(node.homeDir, 'config/' + extraConfigFile.getKey())
            // wrap source file in closure to delay resolution to execution time
            copyConfig.from({ extraConfigFileValue }) {
                // this must be in a closure so it is only applied to the single file specified in from above
                into(configDir.toPath().relativize(destConfigFile.canonicalFile.parentFile.toPath()).toFile())
                rename { destConfigFile.name }
            }
        }
        return copyConfig
    }

    /**
     * Adds a task to copy plugins to a temp dir, which they will later be installed from.
     *
     * For each plugin, if the plugin has rest spec apis in its tests, those api files are also copied
     * to the test resources for this project.
     */
    static Task configureCopyPluginsTask(String name, Project project, Task setup, NodeInfo node) {
        if (node.config.plugins.isEmpty()) {
            return setup
        }
        Copy copyPlugins = project.tasks.create(name: name, type: Copy, dependsOn: setup)

        List<FileCollection> pluginFiles = []
        for (Map.Entry<String, Object> plugin : node.config.plugins.entrySet()) {
            FileCollection pluginZip
            if (plugin.getValue() instanceof Project) {
                Project pluginProject = plugin.getValue()
                if (pluginProject.plugins.hasPlugin(PluginBuildPlugin) == false) {
                    throw new GradleException("Task ${name} cannot project ${pluginProject.path} which is not an esplugin")
                }
                String configurationName = "_plugin_${pluginProject.path}"
                Configuration configuration = project.configurations.findByName(configurationName)
                if (configuration == null) {
                    configuration = project.configurations.create(configurationName)
                }
                project.dependencies.add(configurationName, pluginProject)
                setup.dependsOn(pluginProject.tasks.bundlePlugin)
                pluginZip = configuration

                // also allow rest tests to use the rest spec from the plugin
                Copy copyRestSpec = null
                for (File resourceDir : pluginProject.sourceSets.test.resources.srcDirs) {
                    File restApiDir = new File(resourceDir, 'rest-api-spec/api')
                    if (restApiDir.exists() == false) continue
                    if (copyRestSpec == null) {
                        copyRestSpec = project.tasks.create(name: pluginTaskName('copy', plugin.getKey(), 'PluginRestSpec'), type: Copy)
                        copyPlugins.dependsOn(copyRestSpec)
                        copyRestSpec.into(project.sourceSets.test.output.resourcesDir)
                    }
                    copyRestSpec.from(resourceDir).include('rest-api-spec/api/**')
                }
            } else {
                pluginZip = plugin.getValue()
            }
            pluginFiles.add(pluginZip)
        }

        copyPlugins.into(node.pluginsTmpDir)
        copyPlugins.from(pluginFiles)
        return copyPlugins
    }

    static Task configureInstallModuleTask(String name, Project project, Task setup, NodeInfo node, Project module) {
        if (node.config.distribution != 'integ-test-zip') {
            throw new GradleException("Module ${module.path} not allowed be installed distributions other than integ-test-zip because they should already have all modules bundled!")
        }
        if (module.plugins.hasPlugin(PluginBuildPlugin) == false) {
            throw new GradleException("Task ${name} cannot include module ${module.path} which is not an esplugin")
        }
        Copy installModule = project.tasks.create(name, Copy.class)
        installModule.dependsOn(setup)
        installModule.into(new File(node.homeDir, "modules/${module.name}"))
        installModule.from({ project.zipTree(module.tasks.bundlePlugin.outputs.files.singleFile) })
        return installModule
    }

    static Task configureInstallPluginTask(String name, Project project, Task setup, NodeInfo node, Object plugin) {
        FileCollection pluginZip
        if (plugin instanceof Project) {
            pluginZip = project.configurations.getByName("_plugin_${plugin.path}")
        } else {
            pluginZip = plugin
        }
        // delay reading the file location until execution time by wrapping in a closure within a GString
        String file = "${-> new File(node.pluginsTmpDir, pluginZip.singleFile.getName()).toURI().toURL().toString()}"
        Object[] args = [new File(node.homeDir, 'bin/elasticsearch-plugin'), 'install', file]
        return configureExecTask(name, project, setup, node, args)
    }

    /** Wrapper for command line argument: surrounds comma with double quotes **/
    private static class EscapeCommaWrapper {

        Object arg

        public String toString() {
            String s = arg.toString()

            /// Surround strings that contains a comma with double quotes
            if (s.indexOf(',') != -1) {
                return "\"${s}\""
            }
            return s
        }
    }

    /** Adds a task to execute a command to help setup the cluster */
    static Task configureExecTask(String name, Project project, Task setup, NodeInfo node, Object[] execArgs) {
        return project.tasks.create(name: name, type: LoggedExec, dependsOn: setup) {
            workingDir node.cwd
            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                executable 'cmd'
                args '/C', 'call'
                // On Windows the comma character is considered a parameter separator:
                // argument are wrapped in an ExecArgWrapper that escapes commas
                args execArgs.collect { a -> new EscapeCommaWrapper(arg: a) }
            } else {
                commandLine execArgs
            }
        }
    }

    /** Adds a task to start an elasticsearch node with the given configuration */
    static Task configureStartTask(String name, Project project, Task setup, NodeInfo node) {

        // this closure is converted into ant nodes by groovy's AntBuilder
        Closure antRunner = { AntBuilder ant ->
            ant.exec(executable: node.executable, spawn: node.config.daemonize, dir: node.cwd, taskname: 'elasticsearch') {
                node.env.each { key, value -> env(key: key, value: value) }
                node.args.each { arg(value: it) }
            }
        }

        // this closure is the actual code to run elasticsearch
        Closure elasticsearchRunner = {
            // Due to how ant exec works with the spawn option, we lose all stdout/stderr from the
            // process executed. To work around this, when spawning, we wrap the elasticsearch start
            // command inside another shell script, which simply internally redirects the output
            // of the real elasticsearch script. This allows ant to keep the streams open with the
            // dummy process, but us to have the output available if there is an error in the
            // elasticsearch start script
            if (node.config.daemonize) {
                node.writeWrapperScript()
            }

            // we must add debug options inside the closure so the config is read at execution time, as
            // gradle task options are not processed until the end of the configuration phase
            if (node.config.debug) {
                println 'Running elasticsearch in debug mode, suspending until connected on port 8000'
                node.env['ES_JAVA_OPTS'] = '-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=8000'
            }

            node.getCommandString().eachLine { line -> logger.info(line) }

            if (logger.isInfoEnabled() || node.config.daemonize == false) {
                runAntCommand(project, antRunner, System.out, System.err)
            } else {
                // buffer the output, we may not need to print it
                PrintStream captureStream = new PrintStream(node.buffer, true, "UTF-8")
                runAntCommand(project, antRunner, captureStream, captureStream)
            }
        }

        Task start = project.tasks.create(name: name, type: DefaultTask, dependsOn: setup)
        start.doLast(elasticsearchRunner)
        return start
    }

    static Task configureWaitTask(String name, Project project, List<NodeInfo> nodes, List<Task> startTasks) {
        Task wait = project.tasks.create(name: name, dependsOn: startTasks)
        wait.doLast {
            ant.waitfor(maxwait: '30', maxwaitunit: 'second', checkevery: '500', checkeveryunit: 'millisecond', timeoutproperty: "failed${name}") {
                or {
                    for (NodeInfo node : nodes) {
                        resourceexists {
                            file(file: node.failedMarker.toString())
                        }
                    }
                    and {
                        for (NodeInfo node : nodes) {
                            resourceexists {
                                file(file: node.pidFile.toString())
                            }
                            resourceexists {
                                file(file: node.httpPortsFile.toString())
                            }
                            resourceexists {
                                file(file: node.transportPortsFile.toString())
                            }
                        }
                    }
                }
            }
            boolean anyNodeFailed = false
            for (NodeInfo node : nodes) {
                anyNodeFailed |= node.failedMarker.exists()
            }
            if (ant.properties.containsKey("failed${name}".toString()) || anyNodeFailed) {
                waitFailed(nodes, logger, 'Failed to start elasticsearch')
            }

            // go through each node checking the wait condition
            for (NodeInfo node : nodes) {
                // first bind node info to the closure, then pass to the ant runner so we can get good logging
                Closure antRunner = node.config.waitCondition.curry(node)

                boolean success
                if (logger.isInfoEnabled()) {
                    success = runAntCommand(project, antRunner, System.out, System.err)
                } else {
                    PrintStream captureStream = new PrintStream(node.buffer, true, "UTF-8")
                    success = runAntCommand(project, antRunner, captureStream, captureStream)
                }

                if (success == false) {
                    waitFailed(nodes, logger, 'Elasticsearch cluster failed to pass wait condition')
                }
            }
        }
        return wait
    }

    static void waitFailed(List<NodeInfo> nodes, Logger logger, String msg) {
        for (NodeInfo node : nodes) {
            if (logger.isInfoEnabled() == false) {
                // We already log the command at info level. No need to do it twice.
                node.getCommandString().eachLine { line -> logger.error(line) }
            }
            logger.error("Node ${node.nodeNum} output:")
            logger.error("|-----------------------------------------")
            logger.error("|  failure marker exists: ${node.failedMarker.exists()}")
            logger.error("|  pid file exists: ${node.pidFile.exists()}")
            logger.error("|  http ports file exists: ${node.httpPortsFile.exists()}")
            logger.error("|  transport ports file exists: ${node.transportPortsFile.exists()}")
            // the waitfor failed, so dump any output we got (if info logging this goes directly to stdout)
            logger.error("|\n|  [ant output]")
            node.buffer.toString('UTF-8').eachLine { line -> logger.error("|    ${line}") }
            // also dump the log file for the startup script (which will include ES logging output to stdout)
            if (node.startLog.exists()) {
                logger.error("|\n|  [log]")
                node.startLog.eachLine { line -> logger.error("|    ${line}") }
            }
            logger.error("|-----------------------------------------")
        }
        throw new GradleException(msg)
    }

    /** Adds a task to check if the process with the given pidfile is actually elasticsearch */
    static Task configureCheckPreviousTask(String name, Project project, Object depends, NodeInfo node) {
        return project.tasks.create(name: name, type: Exec, dependsOn: depends) {
            onlyIf { node.pidFile.exists() }
            // the pid file won't actually be read until execution time, since the read is wrapped within an inner closure of the GString
            ext.pid = "${ -> node.pidFile.getText('UTF-8').trim()}"
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
    static Task configureStopTask(String name, Project project, Object depends, NodeInfo node) {
        return project.tasks.create(name: name, type: LoggedExec, dependsOn: depends) {
            onlyIf { node.pidFile.exists() }
            // the pid file won't actually be read until execution time, since the read is wrapped within an inner closure of the GString
            ext.pid = "${ -> node.pidFile.getText('UTF-8').trim()}"
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
                project.delete(node.pidFile)
            }
        }
    }

    /** Returns a unique task name for this task and node configuration */
    static String taskName(Task parentTask, NodeInfo node, String action) {
        if (node.config.numNodes > 1) {
            return "${parentTask.name}#node${node.nodeNum}.${action}"
        } else {
            return "${parentTask.name}#${action}"
        }
    }

    public static String pluginTaskName(String action, String name, String suffix) {
        // replace every dash followed by a character with just the uppercase character
        String camelName = name.replaceAll(/-(\w)/) { _, c -> c.toUpperCase(Locale.ROOT) }
        return action + camelName[0].toUpperCase(Locale.ROOT) + camelName.substring(1) + suffix
    }

    /** Runs an ant command, sending output to the given out and error streams */
    static Object runAntCommand(Project project, Closure command, PrintStream outputStream, PrintStream errorStream) {
        DefaultLogger listener = new DefaultLogger(
                errorPrintStream: errorStream,
                outputPrintStream: outputStream,
                messageOutputLevel: org.apache.tools.ant.Project.MSG_INFO)

        project.ant.project.addBuildListener(listener)
        Object retVal = command(project.ant)
        project.ant.project.removeBuildListener(listener)
        return retVal
    }
}
