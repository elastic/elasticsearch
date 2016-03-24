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

import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.file.FileCollection
import org.gradle.api.tasks.Input

/** Configuration for an elasticsearch cluster, used for integration tests. */
class ClusterConfiguration {

    @Input
    String distribution = 'integ-test-zip'

    @Input
    int numNodes = 1

    @Input
    int numBwcNodes = 0

    @Input
    String bwcVersion = null

    @Input
    int httpPort = 0

    @Input
    int transportPort = 0

    @Input
    boolean daemonize = true

    @Input
    boolean debug = false

    @Input
    String jvmArgs = System.getProperty('tests.jvm.argline', '')

    /**
     * The seed nodes port file. In the case the cluster has more than one node we use a seed node
     * to form the cluster. The file is null if there is no seed node yet available.
     *
     * Note: this can only be null if the cluster has only one node or if the first node is not yet
     * configured. All nodes but the first node should see a non null value.
     */
    File seedNodePortsFile

    /**
     * A closure to call before the cluster is considered ready. The closure is passed the node info,
     * as well as a groovy AntBuilder, to enable running ant condition checks. The default wait
     * condition is for http on the http port.
     */
    @Input
    Closure waitCondition = { NodeInfo node, AntBuilder ant ->
        File tmpFile = new File(node.cwd, 'wait.success')
        ant.get(src: "http://${node.httpUri()}/_cluster/health?wait_for_nodes=${numNodes}",
                dest: tmpFile.toString(),
                ignoreerrors: true, // do not fail on error, so logging buffers can be flushed by the wait task
                retries: 10)
        return tmpFile.exists()
    }

    Map<String, String> systemProperties = new HashMap<>()

    Map<String, String> settings = new HashMap<>()

    // map from destination path, to source file
    Map<String, Object> extraConfigFiles = new HashMap<>()

    LinkedHashMap<String, Object> plugins = new LinkedHashMap<>()

    List<Project> modules = new ArrayList<>()

    LinkedHashMap<String, Object[]> setupCommands = new LinkedHashMap<>()

    @Input
    void systemProperty(String property, String value) {
        systemProperties.put(property, value)
    }

    @Input
    void setting(String name, String value) {
        settings.put(name, value)
    }

    @Input
    void plugin(String name, FileCollection file) {
        plugins.put(name, file)
    }

    @Input
    void plugin(String name, Project pluginProject) {
        plugins.put(name, pluginProject)
    }

    /** Add a module to the cluster. The project must be an esplugin and have a single zip default artifact. */
    @Input
    void module(Project moduleProject) {
        modules.add(moduleProject)
    }

    @Input
    void setupCommand(String name, Object... args) {
        setupCommands.put(name, args)
    }

    /**
     * Add an extra configuration file. The path is relative to the config dir, and the sourceFile
     * is anything accepted by project.file()
     */
    @Input
    void extraConfigFile(String path, Object sourceFile) {
        if (path == 'elasticsearch.yml') {
            throw new GradleException('Overwriting elasticsearch.yml is not allowed, add additional settings using cluster { setting "foo", "bar" }')
        }
        extraConfigFiles.put(path, sourceFile)
    }

    /** Returns an address and port suitable for a uri to connect to this clusters seed node over transport protocol*/
    String seedNodeTransportUri() {
        if (seedNodePortsFile != null) {
            return seedNodePortsFile.readLines("UTF-8").get(0)
        }
        return null;
    }
}
