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
import org.gradle.api.InvalidUserDataException
import org.gradle.api.Project
import org.gradle.api.Task

/**
 * A container for the files and configuration associated with a single node in a test cluster.
 */
class NodeInfo {
    /** common configuration for all nodes, including this one */
    ClusterConfiguration config

    /** node number within the cluster, for creating unique names and paths */
    int nodeNum

    /** name of the cluster this node is part of */
    String clusterName

    /** root directory all node files and operations happen under */
    File baseDir

    /** the pid file the node will use */
    File pidFile

    /** elasticsearch home dir */
    File homeDir

    /** working directory for the node process */
    File cwd

    /** file that if it exists, indicates the node failed to start */
    File failedMarker

    /** stdout/stderr log of the elasticsearch process for this node */
    File startLog

    /** directory to install plugins from */
    File pluginsTmpDir

    /** environment variables to start the node with */
    Map<String, String> env

    /** arguments to start the node with */
    List<String> args

    /** Path to the elasticsearch start script */
    String esScript

    /** buffer for ant output when starting this node */
    ByteArrayOutputStream buffer = new ByteArrayOutputStream()

    /** Creates a node to run as part of a cluster for the given task */
    NodeInfo(ClusterConfiguration config, int nodeNum, Project project, Task task) {
        this.config = config
        this.nodeNum = nodeNum
        clusterName = "${task.path.replace(':', '_').substring(1)}"
        baseDir = new File(project.buildDir, "cluster/${task.name} node${nodeNum}")
        pidFile = new File(baseDir, 'es.pid')
        homeDir = homeDir(baseDir, config.distribution)
        cwd = new File(baseDir, "cwd")
        failedMarker = new File(cwd, 'run.failed')
        startLog = new File(cwd, 'run.log')
        pluginsTmpDir = new File(baseDir, "plugins tmp")

        env = [
            'JAVA_HOME' : project.javaHome,
            'ES_GC_OPTS': config.jvmArgs // we pass these with the undocumented gc opts so the argline can set gc, etc
        ]
        args = config.systemProperties.collect { key, value -> "-D${key}=${value}" }
        for (Map.Entry<String, String> property : System.properties.entrySet()) {
            if (property.getKey().startsWith('es.')) {
                args.add("-D${property.getKey()}=${property.getValue()}")
            }
        }
        // running with cmd on windows will look for this with the .bat extension
        esScript = new File(homeDir, 'bin/elasticsearch').toString()
    }

    /** Returns debug string for the command that started this node. */
    String getCommandString() {
        String esCommandString = "Elasticsearch node ${nodeNum} command: ${esScript} "
        esCommandString += args.join(' ')
        esCommandString += '\nenvironment:'
        env.each { k, v -> esCommandString += "\n  ${k}: ${v}" }
        return esCommandString
    }

    /** Returns the http port for this node */
    int httpPort() {
        return config.baseHttpPort + nodeNum
    }

    /** Returns the transport port for this node */
    int transportPort() {
        return config.baseTransportPort + nodeNum
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
}
