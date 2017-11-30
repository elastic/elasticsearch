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
import org.elasticsearch.gradle.Version
import org.elasticsearch.gradle.VersionProperties
import org.elasticsearch.gradle.plugin.PluginBuildPlugin
import org.elasticsearch.gradle.plugin.PluginPropertiesExtension
import org.gradle.api.AntBuilder
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.InvalidUserDataException
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.Dependency
import org.gradle.api.file.FileCollection
import org.gradle.api.logging.Logger
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.Delete
import org.gradle.api.tasks.Exec

import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.util.concurrent.TimeUnit

/**
 * A helper for creating tasks to build a cluster that is used by a task, and tear down the cluster when the task is finished.
 */
class ClusterFormationTasks {

    /**
     * Adds dependent tasks to the given task to start and stop a cluster with the given configuration.
     *
     * Returns a list of NodeInfo objects for each node in the cluster.
     */
    static List<NodeInfo> setup(Project project, String prefix, Task runner, ClusterConfiguration config) {
        File sharedDir = new File(project.buildDir, "cluster/shared")
        Object startDependencies = config.dependencies
        /* First, if we want a clean environment, we remove everything in the
         * shared cluster directory to ensure there are no leftovers in repos
         * or anything in theory this should not be necessary but repositories
         * are only deleted in the cluster-state and not on-disk such that
         * snapshots survive failures / test runs and there is no simple way
         * today to fix that. */
        if (config.cleanShared) {
          Task cleanup = project.tasks.create(
            name: "${prefix}#prepareCluster.cleanShared",
            type: Delete,
            dependsOn: startDependencies) {
              delete sharedDir
              doLast {
                  sharedDir.mkdirs()
              }
          }
          startDependencies = cleanup
        }
        List<Task> startTasks = []
        List<NodeInfo> nodes = []
        if (config.numNodes < config.numBwcNodes) {
            throw new GradleException("numNodes must be >= numBwcNodes [${config.numNodes} < ${config.numBwcNodes}]")
        }
        if (config.numBwcNodes > 0 && config.bwcVersion == null) {
            throw new GradleException("bwcVersion must not be null if numBwcNodes is > 0")
        }
        // this is our current version distribution configuration we use for all kinds of REST tests etc.
        Configuration currentDistro = project.configurations.create("${prefix}_elasticsearchDistro")
        Configuration bwcDistro = project.configurations.create("${prefix}_elasticsearchBwcDistro")
        Configuration bwcPlugins = project.configurations.create("${prefix}_elasticsearchBwcPlugins")
        configureDistributionDependency(project, config.distribution, currentDistro, VersionProperties.elasticsearch)
        if (config.numBwcNodes > 0) {
            if (config.bwcVersion == null) {
                throw new IllegalArgumentException("Must specify bwcVersion when numBwcNodes > 0")
            }
            // if we have a cluster that has a BWC cluster we also need to configure a dependency on the BWC version
            // this version uses the same distribution etc. and only differs in the version we depend on.
            // from here on everything else works the same as if it's the current version, we fetch the BWC version
            // from mirrors using gradles built-in mechanism etc.

            configureDistributionDependency(project, config.distribution, bwcDistro, config.bwcVersion)
            for (Map.Entry<String, Project> entry : config.plugins.entrySet()) {
                configureBwcPluginDependency("${prefix}_elasticsearchBwcPlugins", project, entry.getValue(), bwcPlugins, config.bwcVersion)
            }
            bwcDistro.resolutionStrategy.cacheChangingModulesFor(0, TimeUnit.SECONDS)
            bwcPlugins.resolutionStrategy.cacheChangingModulesFor(0, TimeUnit.SECONDS)
        }
        for (int i = 0; i < config.numNodes; i++) {
            // we start N nodes and out of these N nodes there might be M bwc nodes.
            // for each of those nodes we might have a different configuration
            String elasticsearchVersion = VersionProperties.elasticsearch
            Configuration distro = currentDistro
            if (i < config.numBwcNodes) {
                elasticsearchVersion = config.bwcVersion
                distro = bwcDistro
            }
            NodeInfo node = new NodeInfo(config, i, project, prefix, elasticsearchVersion, sharedDir)
            nodes.add(node)
            Object dependsOn = startTasks.empty ? startDependencies : startTasks.get(0)
            startTasks.add(configureNode(project, prefix, runner, dependsOn, node, config, distro, nodes.get(0)))
        }

        Task wait = configureWaitTask("${prefix}#wait", project, nodes, startTasks)
        runner.dependsOn(wait)

        return nodes
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

    /** Adds a dependency on a different version of the given plugin, which will be retrieved using gradle's dependency resolution */
    static void configureBwcPluginDependency(String name, Project project, Project pluginProject, Configuration configuration, String elasticsearchVersion) {
        verifyProjectHasBuildPlugin(name, elasticsearchVersion, project, pluginProject)
        PluginPropertiesExtension extension = pluginProject.extensions.findByName('esplugin');
        project.dependencies.add(configuration.name, "org.elasticsearch.plugin:${extension.name}:${elasticsearchVersion}@zip")
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
    static Task configureNode(Project project, String prefix, Task runner, Object dependsOn, NodeInfo node, ClusterConfiguration config,
                              Configuration distribution, NodeInfo seedNode) {

        // tasks are chained so their execution order is maintained
        Task setup = project.tasks.create(name: taskName(prefix, node, 'clean'), type: Delete, dependsOn: dependsOn) {
            delete node.homeDir
            delete node.cwd
        }
        setup = project.tasks.create(name: taskName(prefix, node, 'createCwd'), type: DefaultTask, dependsOn: setup) {
            doLast {
                node.cwd.mkdirs()
            }
            outputs.dir node.cwd
        }
        setup = configureCheckPreviousTask(taskName(prefix, node, 'checkPrevious'), project, setup, node)
        setup = configureStopTask(taskName(prefix, node, 'stopPrevious'), project, setup, node)
        setup = configureExtractTask(taskName(prefix, node, 'extract'), project, setup, node, distribution)
        setup = configureWriteConfigTask(taskName(prefix, node, 'configure'), project, setup, node, seedNode)
        setup = configureCreateKeystoreTask(taskName(prefix, node, 'createKeystore'), project, setup, node)
        setup = configureAddKeystoreSettingTasks(prefix, project, setup, node)

        if (node.config.plugins.isEmpty() == false) {
            if (node.nodeVersion == VersionProperties.elasticsearch) {
                setup = configureCopyPluginsTask(taskName(prefix, node, 'copyPlugins'), project, setup, node, prefix)
            } else {
                setup = configureCopyBwcPluginsTask(taskName(prefix, node, 'copyBwcPlugins'), project, setup, node, prefix)
            }
        }

        // install modules
        for (Project module : node.config.modules) {
            String actionName = pluginTaskName('install', module.name, 'Module')
            setup = configureInstallModuleTask(taskName(prefix, node, actionName), project, setup, node, module)
        }

        // install plugins
        for (Map.Entry<String, Project> plugin : node.config.plugins.entrySet()) {
            String actionName = pluginTaskName('install', plugin.getKey(), 'Plugin')
            setup = configureInstallPluginTask(taskName(prefix, node, actionName), project, setup, node, plugin.getValue(), prefix)
        }

        // sets up any extra config files that need to be copied over to the ES instance;
        // its run after plugins have been installed, as the extra config files may belong to plugins
        setup = configureExtraConfigFilesTask(taskName(prefix, node, 'extraConfig'), project, setup, node)

        // extra setup commands
        for (Map.Entry<String, Object[]> command : node.config.setupCommands.entrySet()) {
            // the first argument is the actual script name, relative to home
            Object[] args = command.getValue().clone()
            final Object commandPath
            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                /*
                 * We have to delay building the string as the path will not exist during configuration which will fail on Windows due to
                 * getting the short name requiring the path to already exist. Note that we have to capture the value of arg[0] now
                 * otherwise we would stack overflow later since arg[0] is replaced below.
                 */
                String argsZero = args[0]
                commandPath = "${-> Paths.get(NodeInfo.getShortPathName(node.homeDir.toString())).resolve(argsZero.toString()).toString()}"
            } else {
                commandPath = node.homeDir.toPath().resolve(args[0].toString()).toString()
            }
            args[0] = commandPath
            setup = configureExecTask(taskName(prefix, node, command.getKey()), project, setup, node, args)
        }

        Task start = configureStartTask(taskName(prefix, node, 'start'), project, setup, node)

        if (node.config.daemonize) {
            Task stop = configureStopTask(taskName(prefix, node, 'stop'), project, [], node)
            // if we are running in the background, make sure to stop the server when the task completes
            runner.finalizedBy(stop)
            start.finalizedBy(stop)
            for (Object dependency : config.dependencies) {
                if (dependency instanceof Fixture) {
                    def depStop = ((Fixture)dependency).stopTask
                    runner.finalizedBy(depStop)
                    start.finalizedBy(depStop)
                }
            }
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
                    outputs.dir rpmExtracted
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
                    outputs.dir debExtracted
                }
                break;
            default:
                throw new InvalidUserDataException("Unknown distribution: ${node.config.distribution}")
        }
        return extract
    }

    /** Adds a task to write elasticsearch.yml for the given node configuration */
    static Task configureWriteConfigTask(String name, Project project, Task setup, NodeInfo node, NodeInfo seedNode) {
        Map esConfig = [
                'cluster.name'                 : node.clusterName,
                'node.name'                    : "node-" + node.nodeNum,
                'pidfile'                      : node.pidFile,
                'path.repo'                    : "${node.sharedDir}/repo",
                'path.shared_data'             : "${node.sharedDir}/",
                // Define a node attribute so we can test that it exists
                'node.attr.testattr'           : 'test'
        ]
        int minimumMasterNodes = node.config.minimumMasterNodes.call()
        if (minimumMasterNodes > 0) {
            esConfig['discovery.zen.minimum_master_nodes'] = minimumMasterNodes
        }
        if (node.config.numNodes > 1) {
            // don't wait for state.. just start up quickly
            // this will also allow new and old nodes in the BWC case to become the master
            esConfig['discovery.initial_state_timeout'] = '0s'
        }
        esConfig['node.max_local_storage_nodes'] = node.config.numNodes
        esConfig['http.port'] = node.config.httpPort
        esConfig['transport.tcp.port'] =  node.config.transportPort
        // Default the watermarks to absurdly low to prevent the tests from failing on nodes without enough disk space
        esConfig['cluster.routing.allocation.disk.watermark.low'] = '1b'
        esConfig['cluster.routing.allocation.disk.watermark.high'] = '1b'
        if (Version.fromString(node.nodeVersion).major >= 6) {
            esConfig['cluster.routing.allocation.disk.watermark.flood_stage'] = '1b'
        }
        // increase script compilation limit since tests can rapid-fire script compilations
        if (Version.fromString(node.nodeVersion).major >= 6) {
          esConfig['script.max_compilations_rate'] = '2048/1m'
        } else {
          esConfig['script.max_compilations_per_minute'] = 2048
        }
        esConfig.putAll(node.config.settings)

        Task writeConfig = project.tasks.create(name: name, type: DefaultTask, dependsOn: setup)
        writeConfig.doFirst {
            String unicastTransportUri = node.config.unicastTransportUri(seedNode, node, project.ant)
            if (unicastTransportUri != null) {
                esConfig['discovery.zen.ping.unicast.hosts'] = "\"${unicastTransportUri}\""
            }
            File configFile = new File(node.pathConf, 'elasticsearch.yml')
            logger.info("Configuring ${configFile}")
            configFile.setText(esConfig.collect { key, value -> "${key}: ${value}" }.join('\n'), 'UTF-8')
        }
    }

    /** Adds a task to create keystore */
    static Task configureCreateKeystoreTask(String name, Project project, Task setup, NodeInfo node) {
        if (node.config.keystoreSettings.isEmpty()) {
            return setup
        } else {
            /*
             * We have to delay building the string as the path will not exist during configuration which will fail on Windows due to
             * getting the short name requiring the path to already exist.
             */
            final Object esKeystoreUtil = "${-> node.binPath().resolve('elasticsearch-keystore').toString()}"
            return configureExecTask(name, project, setup, node, esKeystoreUtil, 'create')
        }
    }

    /** Adds tasks to add settings to the keystore */
    static Task configureAddKeystoreSettingTasks(String parent, Project project, Task setup, NodeInfo node) {
        Map kvs = node.config.keystoreSettings
        Task parentTask = setup
        /*
         * We have to delay building the string as the path will not exist during configuration which will fail on Windows due to getting
         * the short name requiring the path to already exist.
         */
        final Object esKeystoreUtil = "${-> node.binPath().resolve('elasticsearch-keystore').toString()}"
        for (Map.Entry<String, String> entry in kvs) {
            String key = entry.getKey()
            String name = taskName(parent, node, 'addToKeystore#' + key)
            Task t = configureExecTask(name, project, parentTask, node, esKeystoreUtil, 'add', key, '-x')
            String settingsValue = entry.getValue() // eval this early otherwise it will not use the right value
            t.doFirst {
                standardInput = new ByteArrayInputStream(settingsValue.getBytes(StandardCharsets.UTF_8))
            }
            parentTask = t
        }
        return parentTask
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
    static Task configureCopyPluginsTask(String name, Project project, Task setup, NodeInfo node, String prefix) {
        Copy copyPlugins = project.tasks.create(name: name, type: Copy, dependsOn: setup)

        List<FileCollection> pluginFiles = []
        for (Map.Entry<String, Project> plugin : node.config.plugins.entrySet()) {

            Project pluginProject = plugin.getValue()
            verifyProjectHasBuildPlugin(name, node.nodeVersion, project, pluginProject)
            String configurationName = pluginConfigurationName(prefix, pluginProject)
            Configuration configuration = project.configurations.findByName(configurationName)
            if (configuration == null) {
                configuration = project.configurations.create(configurationName)
            }
            project.dependencies.add(configurationName, project.dependencies.project(path: pluginProject.path, configuration: 'zip'))
            setup.dependsOn(pluginProject.tasks.bundlePlugin)

            // also allow rest tests to use the rest spec from the plugin
            String copyRestSpecTaskName = pluginTaskName('copy', plugin.getKey(), 'PluginRestSpec')
            Copy copyRestSpec = project.tasks.findByName(copyRestSpecTaskName)
            for (File resourceDir : pluginProject.sourceSets.test.resources.srcDirs) {
                File restApiDir = new File(resourceDir, 'rest-api-spec/api')
                if (restApiDir.exists() == false) continue
                if (copyRestSpec == null) {
                    copyRestSpec = project.tasks.create(name: copyRestSpecTaskName, type: Copy)
                    copyPlugins.dependsOn(copyRestSpec)
                    copyRestSpec.into(project.sourceSets.test.output.resourcesDir)
                }
                copyRestSpec.from(resourceDir).include('rest-api-spec/api/**')
            }
            pluginFiles.add(configuration)
        }

        copyPlugins.into(node.pluginsTmpDir)
        copyPlugins.from(pluginFiles)
        return copyPlugins
    }

    private static String pluginConfigurationName(final String prefix, final Project project) {
        return "_plugin_${prefix}_${project.path}".replace(':', '_')
    }

    private static String pluginBwcConfigurationName(final String prefix, final Project project) {
        return "_plugin_bwc_${prefix}_${project.path}".replace(':', '_')
    }

    /** Configures task to copy a plugin based on a zip file resolved using dependencies for an older version */
    static Task configureCopyBwcPluginsTask(String name, Project project, Task setup, NodeInfo node, String prefix) {
        Configuration bwcPlugins = project.configurations.getByName("${prefix}_elasticsearchBwcPlugins")
        for (Map.Entry<String, Project> plugin : node.config.plugins.entrySet()) {
            Project pluginProject = plugin.getValue()
            verifyProjectHasBuildPlugin(name, node.nodeVersion, project, pluginProject)
            String configurationName = pluginBwcConfigurationName(prefix, pluginProject)
            Configuration configuration = project.configurations.findByName(configurationName)
            if (configuration == null) {
                configuration = project.configurations.create(configurationName)
            }

            final String depName = pluginProject.extensions.findByName('esplugin').name

            Dependency dep = bwcPlugins.dependencies.find {
                it.name == depName
            }
            configuration.dependencies.add(dep)
        }

        Copy copyPlugins = project.tasks.create(name: name, type: Copy, dependsOn: setup) {
            from bwcPlugins
            into node.pluginsTmpDir
        }
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
        installModule.dependsOn(module.tasks.bundlePlugin)
        installModule.into(new File(node.homeDir, "modules/${module.name}"))
        installModule.from({ project.zipTree(module.tasks.bundlePlugin.outputs.files.singleFile) })
        return installModule
    }

    static Task configureInstallPluginTask(String name, Project project, Task setup, NodeInfo node, Project plugin, String prefix) {
        final FileCollection pluginZip;
        if (node.nodeVersion != VersionProperties.elasticsearch) {
            pluginZip = project.configurations.getByName(pluginBwcConfigurationName(prefix, plugin))
        } else {
            pluginZip = project.configurations.getByName(pluginConfigurationName(prefix, plugin))
        }
        // delay reading the file location until execution time by wrapping in a closure within a GString
        final Object file = "${-> new File(node.pluginsTmpDir, pluginZip.singleFile.getName()).toURI().toURL().toString()}"
        /*
         * We have to delay building the string as the path will not exist during configuration which will fail on Windows due to getting
         * the short name requiring the path to already exist.
         */
        final Object esPluginUtil = "${-> node.binPath().resolve('elasticsearch-plugin').toString()}"
        final Object[] args = [esPluginUtil, 'install', file]
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
                waitFailed(project, nodes, logger, 'Failed to start elasticsearch')
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
                    waitFailed(project, nodes, logger, 'Elasticsearch cluster failed to pass wait condition')
                }
            }
        }
        return wait
    }

    static void waitFailed(Project project, List<NodeInfo> nodes, Logger logger, String msg) {
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
            if (node.pidFile.exists() && node.failedMarker.exists() == false &&
                (node.httpPortsFile.exists() == false || node.transportPortsFile.exists() == false)) {
                logger.error("|\n|  [jstack]")
                String pid = node.pidFile.getText('UTF-8')
                ByteArrayOutputStream output = new ByteArrayOutputStream()
                project.exec {
                    commandLine = ["${project.javaHome}/bin/jstack", pid]
                    standardOutput = output
                }
                output.toString('UTF-8').eachLine { line -> logger.error("|    ${line}") }
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
                if (out.contains("${ext.pid} org.elasticsearch.bootstrap.Elasticsearch") == false) {
                    logger.error('jps -l')
                    logger.error(out)
                    logger.error("pid file: ${node.pidFile}")
                    logger.error("pid: ${ext.pid}")
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
    static String taskName(String prefix, NodeInfo node, String action) {
        if (node.config.numNodes > 1) {
            return "${prefix}#node${node.nodeNum}.${action}"
        } else {
            return "${prefix}#${action}"
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

    static void verifyProjectHasBuildPlugin(String name, String version, Project project, Project pluginProject) {
        if (pluginProject.plugins.hasPlugin(PluginBuildPlugin) == false) {
            throw new GradleException("Task [${name}] cannot add plugin [${pluginProject.path}] with version [${version}] to project's " +
                    "[${project.path}] dependencies: the plugin is not an esplugin")
        }
    }
}
