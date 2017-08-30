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

import com.sun.jna.Native
import com.sun.jna.WString
import org.apache.tools.ant.taskdefs.condition.Os
import org.elasticsearch.gradle.Version
import org.gradle.api.InvalidUserDataException
import org.gradle.api.Project

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

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

    /** shared data directory all nodes share */
    File sharedDir

    /** the pid file the node will use */
    File pidFile

    /** a file written by elasticsearch containing the ports of each bound address for http */
    File httpPortsFile

    /** a file written by elasticsearch containing the ports of each bound address for transport */
    File transportPortsFile

    /** elasticsearch home dir */
    File homeDir

    /** config directory */
    File pathConf

    /** data directory (as an Object, to allow lazy evaluation) */
    Object dataDir

    /** THE config file */
    File configFile

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

    /** Executable to run the bin/elasticsearch with, either cmd or sh */
    String executable

    /** Path to the elasticsearch start script */
    private Object esScript

    /** script to run when running in the background */
    private File wrapperScript

    /** buffer for ant output when starting this node */
    ByteArrayOutputStream buffer = new ByteArrayOutputStream()

    /** the version of elasticsearch that this node runs */
    String nodeVersion

    /** Holds node configuration for part of a test cluster. */
    NodeInfo(ClusterConfiguration config, int nodeNum, Project project, String prefix, String nodeVersion, File sharedDir) {
        this.config = config
        this.nodeNum = nodeNum
        this.sharedDir = sharedDir
        if (config.clusterName != null) {
            clusterName = config.clusterName
        } else {
            clusterName = project.path.replace(':', '_').substring(1) + '_' + prefix
        }
        baseDir = new File(project.buildDir, "cluster/${prefix} node${nodeNum}")
        pidFile = new File(baseDir, 'es.pid')
        this.nodeVersion = nodeVersion
        homeDir = homeDir(baseDir, config.distribution, nodeVersion)
        pathConf = pathConf(baseDir, config.distribution, nodeVersion)
        if (config.dataDir != null) {
            dataDir = "${config.dataDir(nodeNum)}"
        } else {
            dataDir = new File(homeDir, "data")
        }
        configFile = new File(pathConf, 'elasticsearch.yml')
        // even for rpm/deb, the logs are under home because we dont start with real services
        File logsDir = new File(homeDir, 'logs')
        httpPortsFile = new File(logsDir, 'http.ports')
        transportPortsFile = new File(logsDir, 'transport.ports')
        cwd = new File(baseDir, "cwd")
        failedMarker = new File(cwd, 'run.failed')
        startLog = new File(cwd, 'run.log')
        pluginsTmpDir = new File(baseDir, "plugins tmp")

        args = []
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            executable = 'cmd'
            args.add('/C')
            args.add('"') // quote the entire command
            wrapperScript = new File(cwd, "run.bat")
            /*
             * We have to delay building the string as the path will not exist during configuration which will fail on Windows due to
             * getting the short name requiring the path to already exist.
             */
            esScript = "${-> binPath().resolve('elasticsearch.bat').toString()}"
        } else {
            executable = 'bash'
            wrapperScript = new File(cwd, "run")
            esScript = binPath().resolve('elasticsearch')
        }
        if (config.daemonize) {
            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                /*
                 * We have to delay building the string as the path will not exist during configuration which will fail on Windows due to
                 * getting the short name requiring the path to already exist.
                 */
                args.add("${-> getShortPathName(wrapperScript.toString())}")
            } else {
                args.add("${wrapperScript}")
            }
        } else {
            args.add("${esScript}")
        }

        env = ['JAVA_HOME': project.javaHome]
        args.addAll("-E", "node.portsfile=true")
        String collectedSystemProperties = config.systemProperties.collect { key, value -> "-D${key}=${value}" }.join(" ")
        String esJavaOpts = config.jvmArgs.isEmpty() ? collectedSystemProperties : collectedSystemProperties + " " + config.jvmArgs
        if (Boolean.parseBoolean(System.getProperty('tests.asserts', 'true'))) {
            esJavaOpts += " -ea -esa"
        }
        env.put('ES_JAVA_OPTS', esJavaOpts)
        for (Map.Entry<String, String> property : System.properties.entrySet()) {
            if (property.key.startsWith('tests.es.')) {
                args.add("-E")
                args.add("${property.key.substring('tests.es.'.size())}=${property.value}")
            }
        }
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            /*
             * We have to delay building the string as the path will not exist during configuration which will fail on Windows due to
             * getting the short name requiring the path to already exist.
             */
            env.put('ES_PATH_CONF', "${-> getShortPathName(pathConf.toString())}")
        }
        else {
            env.put('ES_PATH_CONF', pathConf)
        }
        if (Version.fromString(nodeVersion).major == 5) {
            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                /*
                 * We have to delay building the string as the path will not exist during configuration which will fail on Windows due to
                 * getting the short name requiring the path to already exist.
                 */
                args.addAll("-E", "path.conf=${-> getShortPathName(pathConf.toString())}")
            } else {
                args.addAll("-E", "path.conf=${pathConf}")
            }
        }
        if (!System.properties.containsKey("tests.es.path.data")) {
            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                /*
                 * We have to delay building the string as the path will not exist during configuration which will fail on Windows due to
                 * getting the short name requiring the path to already exist. This one is extra tricky because usually we rely on the node
                 * creating its data directory on startup but we simply can not do that here because getting the short path name requires
                 * the directory to already exist. Therefore, we create this directory immediately before getting the short name.
                 */
                args.addAll("-E", "path.data=${-> Files.createDirectories(Paths.get(dataDir.toString())); getShortPathName(dataDir.toString())}")
            } else {
                args.addAll("-E", "path.data=${-> dataDir.toString()}")
            }
        }
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            args.add('"') // end the entire command, quoted
        }
    }

    Path binPath() {
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            return Paths.get(getShortPathName(new File(homeDir, 'bin').toString()))
        } else {
            return Paths.get(new File(homeDir, 'bin').toURI())
        }
    }

    static String getShortPathName(String path) {
        assert Os.isFamily(Os.FAMILY_WINDOWS)
        final WString longPath = new WString("\\\\?\\" + path)
        // first we get the length of the buffer needed
        final int length = JNAKernel32Library.getInstance().GetShortPathNameW(longPath, null, 0)
        if (length == 0) {
            throw new IllegalStateException("path [" + path + "] encountered error [" + Native.getLastError() + "]")
        }
        final char[] shortPath = new char[length]
        // knowing the length of the buffer, now we get the short name
        if (JNAKernel32Library.getInstance().GetShortPathNameW(longPath, shortPath, length) == 0) {
            throw new IllegalStateException("path [" + path + "] encountered error [" + Native.getLastError() + "]")
        }
        // we have to strip the \\?\ away from the path for cmd.exe
        return Native.toString(shortPath).substring(4)
    }

    /** Returns debug string for the command that started this node. */
    String getCommandString() {
        String esCommandString = "\nNode ${nodeNum} configuration:\n"
        esCommandString += "|-----------------------------------------\n"
        esCommandString += "|  cwd: ${cwd}\n"
        esCommandString += "|  command: ${executable} ${args.join(' ')}\n"
        esCommandString += '|  environment:\n'
        env.each { k, v -> esCommandString += "|    ${k}: ${v}\n" }
        if (config.daemonize) {
            esCommandString += "|\n|  [${wrapperScript.name}]\n"
            wrapperScript.eachLine('UTF-8', { line -> esCommandString += "    ${line}\n"})
        }
        esCommandString += '|\n|  [elasticsearch.yml]\n'
        configFile.eachLine('UTF-8', { line -> esCommandString += "|    ${line}\n" })
        esCommandString += "|-----------------------------------------"
        return esCommandString
    }

    void writeWrapperScript() {
        String argsPasser = '"$@"'
        String exitMarker = "; if [ \$? != 0 ]; then touch run.failed; fi"
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            argsPasser = '%*'
            exitMarker = "\r\n if \"%errorlevel%\" neq \"0\" ( type nul >> run.failed )"
        }
        wrapperScript.setText("\"${esScript}\" ${argsPasser} > run.log 2>&1 ${exitMarker}", 'UTF-8')
    }

    /** Returns an address and port suitable for a uri to connect to this node over http */
    String httpUri() {
        return httpPortsFile.readLines("UTF-8").get(0)
    }

    /** Returns an address and port suitable for a uri to connect to this node over transport protocol */
    String transportUri() {
        return transportPortsFile.readLines("UTF-8").get(0)
    }

    /** Returns the file which contains the transport protocol ports for this node */
    File getTransportPortsFile() {
        return transportPortsFile
    }

    /** Returns the data directory for this node */
    File getDataDir() {
        if (!(dataDir instanceof File)) {
            return new File(dataDir)
        }
        return dataDir
    }

    /** Returns the directory elasticsearch home is contained in for the given distribution */
    static File homeDir(File baseDir, String distro, String nodeVersion) {
        String path
        switch (distro) {
            case 'integ-test-zip':
            case 'zip':
            case 'tar':
                path = "elasticsearch-${nodeVersion}"
                break
            case 'rpm':
            case 'deb':
                path = "${distro}-extracted/usr/share/elasticsearch"
                break
            default:
                throw new InvalidUserDataException("Unknown distribution: ${distro}")
        }
        return new File(baseDir, path)
    }

    static File pathConf(File baseDir, String distro, String nodeVersion) {
        switch (distro) {
            case 'integ-test-zip':
            case 'zip':
            case 'tar':
                return new File(homeDir(baseDir, distro, nodeVersion), 'config')
            case 'rpm':
            case 'deb':
                return new File(baseDir, "${distro}-extracted/etc/elasticsearch")
            default:
                throw new InvalidUserDataException("Unkown distribution: ${distro}")
        }
    }
}
