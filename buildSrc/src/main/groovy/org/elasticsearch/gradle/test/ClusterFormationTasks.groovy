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
import org.elasticsearch.gradle.BuildPlugin
import org.elasticsearch.gradle.BwcVersions
import org.elasticsearch.gradle.LoggedExec
import org.elasticsearch.gradle.Version
import org.elasticsearch.gradle.VersionProperties
import org.elasticsearch.gradle.plugin.PluginBuildPlugin
import org.elasticsearch.gradle.plugin.PluginPropertiesExtension
import org.gradle.api.AntBuilder
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.Dependency
import org.gradle.api.file.FileCollection
import org.gradle.api.logging.Logger
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.Delete
import org.gradle.api.tasks.Exec
import org.gradle.internal.jvm.Jvm

import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors

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
        if (System.getProperty('tests.distribution', 'oss') == 'integ-test-zip') {
            throw new Exception("tests.distribution=integ-test-zip is not supported")
        }
        configureDistributionDependency(project, config.distribution, currentDistro, VersionProperties.elasticsearch)
        boolean hasBwcNodes = config.numBwcNodes > 0
        if (hasBwcNodes) {
            if (config.bwcVersion == null) {
                throw new IllegalArgumentException("Must specify bwcVersion when numBwcNodes > 0")
            }
            // if we have a cluster that has a BWC cluster we also need to configure a dependency on the BWC version
            // this version uses the same distribution etc. and only differs in the version we depend on.
            // from here on everything else works the same as if it's the current version, we fetch the BWC version
            // from mirrors using gradles built-in mechanism etc.

            configureDistributionDependency(project, config.distribution, bwcDistro, config.bwcVersion.toString())
            for (Map.Entry<String, Object> entry : config.plugins.entrySet()) {
                configureBwcPluginDependency(project, entry.getValue(), bwcPlugins, config.bwcVersion)
            }
            bwcDistro.resolutionStrategy.cacheChangingModulesFor(0, TimeUnit.SECONDS)
            bwcPlugins.resolutionStrategy.cacheChangingModulesFor(0, TimeUnit.SECONDS)
        }
        for (int i = 0; i < config.numNodes; i++) {
            // we start N nodes and out of these N nodes there might be M bwc nodes.
            // for each of those nodes we might have a different configuration
            Configuration distro
            String elasticsearchVersion
            if (i < config.numBwcNodes) {
                elasticsearchVersion = config.bwcVersion.toString()
                if (project.bwcVersions.unreleased.contains(config.bwcVersion) && 
                    (project.version != elasticsearchVersion)) {
                    elasticsearchVersion += "-SNAPSHOT"
                }
                distro = bwcDistro
            } else {
                elasticsearchVersion = VersionProperties.elasticsearch
                distro = currentDistro
            }
            NodeInfo node = new NodeInfo(config, i, project, prefix, elasticsearchVersion, sharedDir)
            nodes.add(node)
            Closure<Map> writeConfigSetup
            Object dependsOn
            writeConfigSetup = { Map esConfig ->
                if (config.getAutoSetHostsProvider()) {
                    if (esConfig.containsKey("discovery.seed_providers") == false) {
                        esConfig["discovery.seed_providers"] = 'file'
                    }
                    esConfig["discovery.seed_hosts"] = []
                }
                if (esConfig['discovery.type'] == null && config.getAutoSetInitialMasterNodes()) {
                    esConfig['cluster.initial_master_nodes'] = nodes.stream().map({ n ->
                        if (n.config.settings['node.name'] == null) {
                            return "node-" + n.nodeNum
                        } else {
                            return n.config.settings['node.name']
                        }
                    }).collect(Collectors.toList())
                }
                esConfig
            }
            dependsOn = startDependencies
            startTasks.add(configureNode(project, prefix, runner, dependsOn, node, config, distro, writeConfigSetup))
        }

        Task wait = configureWaitTask("${prefix}#wait", project, nodes, startTasks, config.nodeStartupWaitSeconds)
        runner.dependsOn(wait)

        return nodes
    }

    /** Adds a dependency on the given distribution */
    static void configureDistributionDependency(Project project, String distro, Configuration configuration, String elasticsearchVersion) {
        boolean internalBuild = project.hasProperty('bwcVersions')
        if (distro.equals("integ-test-zip")) {
            // short circuit integ test so it doesn't complicate the rest of the distribution setup below
            if (internalBuild) {
                project.dependencies.add(
                        configuration.name,
                        project.dependencies.project(path: ":distribution", configuration: 'integ-test-zip')
                )
            } else {
                project.dependencies.add(
                        configuration.name,
                        "org.elasticsearch.distribution.integ-test-zip:elasticsearch:${elasticsearchVersion}@zip"
                )
            }
            return
        }
        // TEMP HACK
        // The oss docs CI build overrides the distro on the command line. This hack handles backcompat until CI is updated.
        if (distro.equals('oss-zip')) {
            distro = 'oss'
        }
        if (distro.equals('zip')) {
            distro = 'default'
        }
        // END TEMP HACK
        if (['oss', 'default'].contains(distro) == false) {
            throw new GradleException("Unknown distribution: ${distro} in project ${project.path}")
        }
        Version version = Version.fromString(elasticsearchVersion)
        String os = getOs()
        String classifier = "${os}-x86_64"
        String packaging = os.equals('windows') ? 'zip' : 'tar.gz'
        String artifactName = 'elasticsearch'
        if (distro.equals('oss') && Version.fromString(elasticsearchVersion).onOrAfter('6.3.0')) {
            artifactName += '-oss'
        }
        Object dependency
        String snapshotProject = "${os}-${os.equals('windows') ? 'zip' : 'tar'}"
        if (version.before("7.0.0")) {
            snapshotProject = "zip"
        }
        if (distro.equals("oss")) {
            snapshotProject = "oss-" + snapshotProject
        }
        
        BwcVersions.UnreleasedVersionInfo unreleasedInfo = null

        if (project.hasProperty('bwcVersions')) {
            // NOTE: leniency is needed for external plugin authors using build-tools. maybe build the version compat info into build-tools?
            unreleasedInfo = project.bwcVersions.unreleasedInfo(version)
        }
        if (unreleasedInfo != null) {
            dependency = project.dependencies.project(
                    path: unreleasedInfo.gradleProjectPath, configuration: snapshotProject
            )
        } else if (internalBuild && elasticsearchVersion.equals(VersionProperties.elasticsearch)) {
            dependency = project.dependencies.project(path: ":distribution:archives:${snapshotProject}")
        } else {
            if (version.before('7.0.0')) {
                classifier = "" // for bwc, before we had classifiers
            }
            // group does not matter as it is not used when we pull from the ivy repo that points to the download service
            dependency = "dnm:${artifactName}:${elasticsearchVersion}-${classifier}@${packaging}"
        }
        project.dependencies.add(configuration.name, dependency)
    }

    /** Adds a dependency on a different version of the given plugin, which will be retrieved using gradle's dependency resolution */
    static void configureBwcPluginDependency(Project project, Object plugin, Configuration configuration, Version elasticsearchVersion) {
        if (plugin instanceof Project) {
            Project pluginProject = (Project)plugin
            verifyProjectHasBuildPlugin(configuration.name, elasticsearchVersion, project, pluginProject)
            final String pluginName = findPluginName(pluginProject)
            project.dependencies.add(configuration.name, "org.elasticsearch.plugin:${pluginName}:${elasticsearchVersion}@zip")
        } else {
            project.dependencies.add(configuration.name, "${plugin}@zip")
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
     *
     * @return a task which starts the node.
     */
    static Task configureNode(Project project, String prefix, Task runner, Object dependsOn, NodeInfo node, ClusterConfiguration config,
                              Configuration distribution, Closure<Map> writeConfig) {

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
        setup = configureExtractTask(taskName(prefix, node, 'extract'), project, setup, node, distribution, config.distribution)
        setup = configureWriteConfigTask(taskName(prefix, node, 'configure'), project, setup, node, writeConfig)
        setup = configureCreateKeystoreTask(taskName(prefix, node, 'createKeystore'), project, setup, node)
        setup = configureAddKeystoreSettingTasks(prefix, project, setup, node)
        setup = configureAddKeystoreFileTasks(prefix, project, setup, node)

        if (node.config.plugins.isEmpty() == false) {
            if (node.nodeVersion == Version.fromString(VersionProperties.elasticsearch)) {
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
        for (String pluginName : node.config.plugins.keySet()) {
            String actionName = pluginTaskName('install', pluginName, 'Plugin')
            setup = configureInstallPluginTask(taskName(prefix, node, actionName), project, setup, node, pluginName, prefix)
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
    static Task configureExtractTask(String name, Project project, Task setup, NodeInfo node,
                                     Configuration configuration, String distribution) {
        List extractDependsOn = [configuration, setup]
        /* configuration.singleFile will be an external artifact if this is being run by a plugin not living in the
          elasticsearch source tree. If this is a plugin built in the elasticsearch source tree or this is a distro in
          the elasticsearch source tree then this should be the version of elasticsearch built by the source tree.
          If it isn't then Bad Things(TM) will happen. */
        Task extract = project.tasks.create(name: name, type: Copy, dependsOn: extractDependsOn) {
            if (getOs().equals("windows") || distribution.equals("integ-test-zip")) {
                from {
                    project.zipTree(configuration.singleFile)
                }
            } else {
                // macos and linux use tar
                from {
                    project.tarTree(project.resources.gzip(configuration.singleFile))
                }
            }
            into node.baseDir
        }

        return extract
    }

    /** Adds a task to write elasticsearch.yml for the given node configuration */
    static Task configureWriteConfigTask(String name, Project project, Task setup, NodeInfo node, Closure<Map> configFilter) {
        Map esConfig = [
                'cluster.name'                 : node.clusterName,
                'node.name'                    : "node-" + node.nodeNum,
                (node.nodeVersion.onOrAfter('7.4.0') ? 'node.pidfile' : 'pidfile') : node.pidFile,
                'path.repo'                    : "${node.sharedDir}/repo",
                'path.shared_data'             : "${node.sharedDir}/",
                // Define a node attribute so we can test that it exists
                'node.attr.testattr'           : 'test',
                // Don't wait for state, just start up quickly. This will also allow new and old nodes in the BWC case to become the master
                'discovery.initial_state_timeout' : '0s'
        ]
        esConfig['http.port'] = node.config.httpPort
        if (node.nodeVersion.onOrAfter('6.7.0')) {
            esConfig['transport.port'] =  node.config.transportPort
        } else {
            esConfig['transport.tcp.port'] =  node.config.transportPort
        }
        // Default the watermarks to absurdly low to prevent the tests from failing on nodes without enough disk space
        esConfig['cluster.routing.allocation.disk.watermark.low'] = '1b'
        esConfig['cluster.routing.allocation.disk.watermark.high'] = '1b'
        if (node.nodeVersion.major >= 6) {
            esConfig['cluster.routing.allocation.disk.watermark.flood_stage'] = '1b'
        }
        // increase script compilation limit since tests can rapid-fire script compilations
        esConfig['script.max_compilations_rate'] = '2048/1m'
        // Temporarily disable the real memory usage circuit breaker. It depends on real memory usage which we have no full control
        // over and the REST client will not retry on circuit breaking exceptions yet (see #31986 for details). Once the REST client
        // can retry on circuit breaking exceptions, we can revert again to the default configuration.
        if (node.nodeVersion.major >= 7) {
            esConfig['indices.breaker.total.use_real_memory'] = false
        }

        Task writeConfig = project.tasks.create(name: name, type: DefaultTask, dependsOn: setup)
        writeConfig.doFirst {
            for (Map.Entry<String, Object> setting : node.config.settings) {
                if (setting.value == null) {
                    esConfig.remove(setting.key)
                } else {
                    esConfig.put(setting.key, setting.value)
                }
            }

            esConfig = configFilter.call(esConfig)
            File configFile = new File(node.pathConf, 'elasticsearch.yml')
            logger.info("Configuring ${configFile}")
            configFile.setText(esConfig.collect { key, value -> "${key}: ${value}" }.join('\n'), 'UTF-8')
        }
    }

    /** Adds a task to create keystore */
    static Task configureCreateKeystoreTask(String name, Project project, Task setup, NodeInfo node) {
        if (node.config.keystoreSettings.isEmpty() && node.config.keystoreFiles.isEmpty()) {
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

    /** Adds tasks to add files to the keystore */
    static Task configureAddKeystoreFileTasks(String parent, Project project, Task setup, NodeInfo node) {
        Map<String, Object> kvs = node.config.keystoreFiles
        if (kvs.isEmpty()) {
            return setup
        }
        Task parentTask = setup
        /*
         * We have to delay building the string as the path will not exist during configuration which will fail on Windows due to getting
         * the short name requiring the path to already exist.
         */
        final Object esKeystoreUtil = "${-> node.binPath().resolve('elasticsearch-keystore').toString()}"
        for (Map.Entry<String, Object> entry in kvs) {
            String key = entry.getKey()
            String name = taskName(parent, node, 'addToKeystore#' + key)
            String srcFileName = entry.getValue()
            Task t = configureExecTask(name, project, parentTask, node, esKeystoreUtil, 'add-file', key, srcFileName)
            t.doFirst {
                File srcFile = project.file(srcFileName)
                if (srcFile.isDirectory()) {
                    throw new GradleException("Source for keystoreFile must be a file: ${srcFile}")
                }
                if (srcFile.exists() == false) {
                    throw new GradleException("Source file for keystoreFile does not exist: ${srcFile}")
                }
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
        for (Map.Entry<String, Object> plugin : node.config.plugins.entrySet()) {

            String configurationName = pluginConfigurationName(prefix, plugin.key)
            Configuration configuration = project.configurations.findByName(configurationName)
            if (configuration == null) {
                configuration = project.configurations.create(configurationName)
            }

            if (plugin.getValue() instanceof Project) {
                Project pluginProject = plugin.getValue()
                verifyProjectHasBuildPlugin(name, node.nodeVersion, project, pluginProject)

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
            } else {
                project.dependencies.add(configurationName, "${plugin.getValue()}@zip")
            }



            pluginFiles.add(configuration)
        }

        copyPlugins.into(node.pluginsTmpDir)
        copyPlugins.from(pluginFiles)
        return copyPlugins
    }

    private static String pluginConfigurationName(final String prefix, final String name) {
        return "_plugin_${prefix}_${name}".replace(':', '_')
    }

    private static String pluginBwcConfigurationName(final String prefix, final String name) {
        return "_plugin_bwc_${prefix}_${name}".replace(':', '_')
    }

    /** Configures task to copy a plugin based on a zip file resolved using dependencies for an older version */
    static Task configureCopyBwcPluginsTask(String name, Project project, Task setup, NodeInfo node, String prefix) {
        Configuration bwcPlugins = project.configurations.getByName("${prefix}_elasticsearchBwcPlugins")
        for (Map.Entry<String, Object> plugin : node.config.plugins.entrySet()) {
            String configurationName = pluginBwcConfigurationName(prefix, plugin.key)
            Configuration configuration = project.configurations.findByName(configurationName)
            if (configuration == null) {
                configuration = project.configurations.create(configurationName)
            }

            if (plugin.getValue() instanceof Project) {
                Project pluginProject = plugin.getValue()
                verifyProjectHasBuildPlugin(name, node.nodeVersion, project, pluginProject)

                final String depName = findPluginName(pluginProject)

                Dependency dep = bwcPlugins.dependencies.find {
                    it.name == depName
                }
                configuration.dependencies.add(dep)
            } else {
                project.dependencies.add(configurationName, "${plugin.getValue()}@zip")
            }
        }

        Copy copyPlugins = project.tasks.create(name: name, type: Copy, dependsOn: setup) {
            from bwcPlugins
            into node.pluginsTmpDir
        }
        return copyPlugins
    }

    static Task configureInstallModuleTask(String name, Project project, Task setup, NodeInfo node, Project module) {
        if (node.config.distribution != 'integ-test-zip') {
            project.logger.info("Not installing modules for $name, ${node.config.distribution} already has them")
            return setup
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

    static Task configureInstallPluginTask(String name, Project project, Task setup, NodeInfo node, String pluginName, String prefix) {
        FileCollection pluginZip;
        if (node.nodeVersion != Version.fromString(VersionProperties.elasticsearch)) {
            pluginZip = project.configurations.getByName(pluginBwcConfigurationName(prefix, pluginName))
        } else {
            pluginZip = project.configurations.getByName(pluginConfigurationName(prefix, pluginName))
        }
        // delay reading the file location until execution time by wrapping in a closure within a GString
        final Object file = "${-> new File(node.pluginsTmpDir, pluginZip.singleFile.getName()).toURI().toURL().toString()}"
        /*
         * We have to delay building the string as the path will not exist during configuration which will fail on Windows due to getting
         * the short name requiring the path to already exist.
         */
        final Object esPluginUtil = "${-> node.binPath().resolve('elasticsearch-plugin').toString()}"
        final Object[] args = [esPluginUtil, 'install', '--batch', file]
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
        return project.tasks.create(name: name, type: LoggedExec, dependsOn: setup) { Exec exec ->
            exec.workingDir node.cwd
            if ((project.isRuntimeJavaHomeSet && node.isBwcNode == false) // runtime Java might not be compatible with old nodes
                    || node.config.distribution == 'integ-test-zip') {
                exec.environment.put('JAVA_HOME', project.runtimeJavaHome)
            } else {
                // force JAVA_HOME to *not* be set
                exec.environment.remove('JAVA_HOME')
            }
            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                exec.executable 'cmd'
                exec.args '/C', 'call'
                // On Windows the comma character is considered a parameter separator:
                // argument are wrapped in an ExecArgWrapper that escapes commas
                exec.args execArgs.collect { a -> new EscapeCommaWrapper(arg: a) }
            } else {
                exec.commandLine execArgs
            }
        }
    }

    /** Adds a task to start an elasticsearch node with the given configuration */
    static Task configureStartTask(String name, Project project, Task setup, NodeInfo node) {
        // this closure is converted into ant nodes by groovy's AntBuilder
        Closure antRunner = { AntBuilder ant ->
            ant.exec(executable: node.executable, spawn: node.config.daemonize, newenvironment: true,
                     dir: node.cwd, taskname: 'elasticsearch') {
                node.env.each { key, value -> env(key: key, value: value) }
                if ((project.isRuntimeJavaHomeSet && node.isBwcNode == false) // runtime Java might not be compatible with old nodes
                        || node.config.distribution == 'integ-test-zip') {
                    env(key: 'JAVA_HOME', value: project.runtimeJavaHome)
                }
                node.args.each { arg(value: it) }
                if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                    // Having no TMP on Windows defaults to C:\Windows and permission errors
                    // Since we configure ant to run with a new  environment above, we need to explicitly pass this
                    String tmp = System.getenv("TMP")
                    assert tmp != null
                    env(key: "TMP", value: tmp)
                }
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
        if (node.javaVersion != null) {
            BuildPlugin.requireJavaHome(start, node.javaVersion)
        }
        start.doLast(elasticsearchRunner)
        start.doFirst {
            // If the node runs in a FIPS 140-2 JVM, the BCFKS default keystore will be password protected
            if (project.inFipsJvm){
                node.config.systemProperties.put('javax.net.ssl.trustStorePassword', 'password')
                node.config.systemProperties.put('javax.net.ssl.keyStorePassword', 'password')
            }

            // Configure ES JAVA OPTS - adds system properties, assertion flags, remote debug etc
            List<String> esJavaOpts = [node.env.get('ES_JAVA_OPTS', '')]
            String collectedSystemProperties = node.config.systemProperties.collect { key, value -> "-D${key}=${value}" }.join(" ")
            esJavaOpts.add(collectedSystemProperties)
            esJavaOpts.add(node.config.jvmArgs)
            if (Boolean.parseBoolean(System.getProperty('tests.asserts', 'true'))) {
                // put the enable assertions options before other options to allow
                // flexibility to disable assertions for specific packages or classes
                // in the cluster-specific options
                esJavaOpts.add("-ea")
                esJavaOpts.add("-esa")
            }
            // we must add debug options inside the closure so the config is read at execution time, as
            // gradle task options are not processed until the end of the configuration phase
            if (node.config.debug) {
                println 'Running elasticsearch in debug mode, suspending until connected on port 8000'
                esJavaOpts.add('-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=8000')
            }
            node.env['ES_JAVA_OPTS'] = esJavaOpts.join(" ")

            //
            project.logger.info("Starting node in ${node.clusterName} distribution: ${node.config.distribution}")
        }
        return start
    }

    static Task configureWaitTask(String name, Project project, List<NodeInfo> nodes, List<Task> startTasks, int waitSeconds) {
        Task wait = project.tasks.create(name: name, dependsOn: startTasks)
        wait.doLast {

            Collection<String> unicastHosts = new HashSet<>()
            nodes.forEach { node ->
                unicastHosts.addAll(node.config.otherUnicastHostAddresses.call())
                String unicastHost = node.config.unicastTransportUri(node, null, project.createAntBuilder())
                if (unicastHost != null) {
                    unicastHosts.add(unicastHost)
                }
            }
            String unicastHostsTxt = String.join("\n", unicastHosts)
            nodes.forEach { node ->
                node.pathConf.toPath().resolve("unicast_hosts.txt").setText(unicastHostsTxt)
            }

            ant.waitfor(maxwait: "${waitSeconds}", maxwaitunit: 'second', checkevery: '500', checkeveryunit: 'millisecond', timeoutproperty: "failed${name}") {
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
            if (ant.properties.containsKey("failed${name}".toString())) {
                waitFailed(project, nodes, logger, "Failed to start elasticsearch: timed out after ${waitSeconds} seconds")
            }

            boolean anyNodeFailed = false
            for (NodeInfo node : nodes) {
                if (node.failedMarker.exists()) {
                    logger.error("Failed to start elasticsearch: ${node.failedMarker.toString()} exists")
                    anyNodeFailed = true
                }
            }
            if (anyNodeFailed) {
                waitFailed(project, nodes, logger, 'Failed to start elasticsearch')
            }

            // make sure all files exist otherwise we haven't fully started up
            boolean missingFile = false
            for (NodeInfo node : nodes) {
                missingFile |= node.pidFile.exists() == false
                missingFile |= node.httpPortsFile.exists() == false
                missingFile |= node.transportPortsFile.exists() == false
            }
            if (missingFile) {
                waitFailed(project, nodes, logger, 'Elasticsearch did not complete startup in time allotted')
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
                    commandLine = ["${project.runtimeJavaHome}/bin/jstack", pid]
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
            final File jps = Jvm.forHome(project.runtimeJavaHome).getExecutable('jps')
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
                // Large tests can exhaust disk space, clean up jdk from the distribution to save some space
                project.delete(new File(node.homeDir, "jdk"))
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

        AntBuilder ant = project.createAntBuilder()
        ant.project.addBuildListener(listener)
        Object retVal = command(ant)
        ant.project.removeBuildListener(listener)
        return retVal
    }

    static void verifyProjectHasBuildPlugin(String name, Version version, Project project, Project pluginProject) {
        if (pluginProject.plugins.hasPlugin(PluginBuildPlugin) == false) {
            throw new GradleException("Task [${name}] cannot add plugin [${pluginProject.path}] with version [${version}] to project's " +
                    "[${project.path}] dependencies: the plugin is not an esplugin")
        }
    }

    /** Find the plugin name in the given project. */
    static String findPluginName(Project pluginProject) {
        PluginPropertiesExtension extension = pluginProject.extensions.findByName('esplugin')
        return extension.name
    }

    /** Find the current OS */
    static String getOs() {
        String os = "linux"
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            os = "windows"
        } else if (Os.isFamily(Os.FAMILY_MAC)) {
            os = "darwin"
        }
        return os
    }
}
