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
import org.elasticsearch.gradle.LoggedExec
import org.elasticsearch.gradle.VersionProperties
import org.gradle.api.AntBuilder
import org.gradle.api.DefaultTask
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.artifacts.Configuration
import org.gradle.api.plugins.JavaBasePlugin
import org.gradle.api.tasks.Delete
import org.gradle.api.tasks.Input
import org.gradle.util.ConfigureUtil

/**
 * Starts nodes with different command line options (-p pid, --help, ...) and checks that all is working OK.
 */
public class CommandLineTestTask extends DefaultTask {

    ClusterConfiguration clusterConfig = new ClusterConfiguration()


    public CommandLineTestTask() {

        description = 'Checks that command line options like -V, -p pid and so on work.'
        group = JavaBasePlugin.VERIFICATION_GROUP

        // this is our current version distribution configuration we use for all kinds of REST tests etc.
        project.configurations {
            elasticsearchDistro
        }
        // directory needed for installing and testing
        File sharedDir = new File(project.buildDir, "cluster/shared")
        clusterConfig.daemonize = false
        clusterConfig.distribution = 'zip'
        NodeInfo node = new NodeInfo(clusterConfig, 0, project, this, VersionProperties.elasticsearch, sharedDir)
        Configuration configuration = project.configurations.elasticsearchDistro

        // cleanup task
        Task clean = project.tasks.create(name: ClusterFormationTasks.taskName(this, node, 'clean'), type: Delete) {
            delete node.homeDir
            delete node.cwd
            doLast {
                node.cwd.mkdirs()
            }
        }

        // prepares extraction and configuration setup so we can start the node
        Task setup = ClusterFormationTasks.configureExtractTask(ClusterFormationTasks.taskName(this, node, 'extract'), project, clean,
                node, configuration)
        ClusterFormationTasks.configureDistributionDependency(project, clusterConfig.distribution, project.configurations.elasticsearchDistro,
                VersionProperties.elasticsearch)
        setup = ClusterFormationTasks.configureWriteConfigTask(ClusterFormationTasks.taskName(this, node, 'configure'), project, setup,
                node)

        // check that --help works
        setup = configureInfoTask(ClusterFormationTasks.taskName(this, node, 'test-help-param'), project, setup, node, "-E " +
                "<KeyValuePair>  Configure an Elasticsearch setting", "--help");

        // check that --version works
        boolean isSnapshot = VersionProperties.elasticsearch.endsWith("-SNAPSHOT");
        String version = VersionProperties.elasticsearch;
        if (isSnapshot) {
            version = version.substring(0, version.length() - 9)
        }
        setup = configureInfoTask(ClusterFormationTasks.taskName(this, node, 'test-version-param'), project, setup, node,
                "Version: " + version, "--version");

        // check that -p works
        File pidFile = new File(sharedDir, "pidForTest-p")
        setup = configurePidParamTask(ClusterFormationTasks.taskName(this, node, 'test-pid-param-start'), project, setup, node, pidFile
                .absolutePath)
        setup = configurePidCheckTask(ClusterFormationTasks.taskName(this, node, 'test-pid-param-check'), project, setup, pidFile)
        setup = configureStopTask(ClusterFormationTasks.taskName(this, node, 'test-pid-param-stop'), project, setup, pidFile)
        this.dependsOn(setup)

    }

    /** Adds a task to start an elasticsearch with a command line option that will not actually start the node (--help or --version) */
    static Task configureInfoTask(String name, Project project, Task setup, NodeInfo node, String matchString, String
            commandLineParameter) {

        // this closure is converted into ant nodes by groovy's AntBuilder
        Closure antRunner = { AntBuilder ant ->
            ant.exec(executable: node.executable, spawn: false, dir: node.cwd, taskname: 'elasticsearch') {
                node.env.each { key, value -> env(key: key, value: value) }
                node.args.each { arg(value: it) }
                arg(value: commandLineParameter) // add the command line param we want to test
            }
        }

        // this closure is the actual code to run elasticsearch and check the output
        Closure elasticsearchRunner = {
            // capture output so we can check it later
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            PrintStream captureStream = new PrintStream(outputStream, true, "UTF-8")
            ClusterFormationTasks.runAntCommand(project, antRunner, captureStream, captureStream)
            String output = new String(outputStream.toByteArray());

            if (output.contains(matchString) == false || output.contains("ERROR")) {
                logger.error("Start elasticsearch with " + commandLineParameter + " failed.")
                logger.error("Here is the message:")
                logger.error(output)
                logger.error("in which we were looking for this string: " + "\"" + matchString + "\"")
                logger.error("Started elasticsearch with this command: " + node.executable + " " + node.args.join(' ') + " " +
                        commandLineParameter)
                throw new Exception("Test command line options failed.")
            }
        }

        Task start = project.tasks.create(name: name, type: DefaultTask, dependsOn: setup)
        start.doLast(elasticsearchRunner)
        return start
    }

    /** Adds a task to start an elasticsearch node with -p pid option */
    static Task configurePidParamTask(String name, Project project, Task setup, NodeInfo node, String pathToPID) {

        // this closure is converted into ant nodes by groovy's AntBuilder
        Closure antRunner = { AntBuilder ant ->
            ant.exec(executable: node.executable, spawn: true, dir: node.cwd, taskname: 'elasticsearch') {
                node.env.each { key, value -> env(key: key, value: value) }
                node.args.each { arg(value: it) }
                arg(value: '-p') // add -p
                arg(value: pathToPID) // add the path to the pid
            }
        }

        // start the node
        Closure elasticsearchRunner = {
            ClusterFormationTasks.runAntCommand(project, antRunner, System.out, System.err)
        }

        Task start = project.tasks.create(name: name, type: DefaultTask, dependsOn: setup)
        start.doLast(elasticsearchRunner)
        return start
    }

    /** Adds a task to check that the pid was actually written */
    static Task configurePidCheckTask(String name, Project project, Task depends, File pidFile) {

        Task wait = project.tasks.create(name: name, dependsOn: depends)
        wait.doLast {
            ant.waitfor(maxwait: '30', maxwaitunit: 'second', checkevery: '5000', checkeveryunit: 'millisecond', timeoutproperty:
                    "failed${name}") {
                resourceexists {
                    file(file: pidFile.toString())
                }
            }
            if (pidFile.exists() == false) {
                throw new Exception("Pid file " + pidFile + " was not found! Beware, there might be a rogue elasticsearch instance " +
                        "running and we cannot shut it down because we don't know which pid it has.")
            }
        }
        return wait
    }

    /** Adds a task to kill an elasticsearch node with the given pidfile */
    // TODO: Maybe this code can be shared with ClusterFormationTasks.configureStopTask ?
    static Task configureStopTask(String name, Project project, Task depends, File pidFile) {
        return project.tasks.create(name: name, type: LoggedExec, dependsOn: depends) {
            onlyIf { pidFile.exists() }
            // the pid file won't actually be read until execution time, since the read is wrapped within an inner closure of the GString
            ext.pid = "${-> pidFile.getText('UTF-8').trim()}"
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

    @Input
    public void cluster(Closure closure) {
        ConfigureUtil.configure(closure, clusterConfig)
    }

    public ClusterConfiguration getCluster() {
        return clusterConfig
    }

    @Override
    public Task dependsOn(Object... dependencies) {
        super.dependsOn(dependencies)
        for (Object dependency : dependencies) {
            if (dependency instanceof Fixture) {
                finalizedBy(((Fixture) dependency).stopTask)
            }
        }
        return this
    }

    @Override
    public void setDependsOn(Iterable<?> dependencies) {
        super.setDependsOn(dependencies)
        for (Object dependency : dependencies) {
            if (dependency instanceof Fixture) {
                finalizedBy(((Fixture) dependency).stopTask)
            }
        }
    }
}
