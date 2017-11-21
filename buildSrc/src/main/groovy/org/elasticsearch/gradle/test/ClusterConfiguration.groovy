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
import org.gradle.api.tasks.Input

/** Configuration for an elasticsearch cluster, used for integration tests. */
class ClusterConfiguration {

    private final Project project

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

    /**
     * An override of the data directory. Input is the node number and output
     * is the override data directory.
     */
    @Input
    Closure<String> dataDir = null

    /** Optional override of the cluster name. */
    @Input
    String clusterName = null

    @Input
    boolean daemonize = true

    @Input
    boolean debug = false

    /**
     * Configuration of the setting <tt>discovery.zen.minimum_master_nodes</tt> on the nodes.
     * In case of more than one node, this defaults to the number of nodes
     */
    @Input
    Closure<Integer> minimumMasterNodes = { getNumNodes() > 1 ? getNumNodes() : -1 }

    @Input
    String jvmArgs = "-Xms" + System.getProperty('tests.heap.size', '512m') +
        " " + "-Xmx" + System.getProperty('tests.heap.size', '512m') +
        " " + System.getProperty('tests.jvm.argline', '')

    /**
     * Should the shared environment be cleaned on cluster startup? Defaults
     * to {@code true} so we run with a clean cluster but some tests wish to
     * preserve snapshots between clusters so they set this to true.
     */
    @Input
    boolean cleanShared = true

    /**
     * A closure to call which returns the unicast host to connect to for cluster formation.
     *
     * This allows multi node clusters, or a new cluster to connect to an existing cluster.
     * The closure takes two arguments, the NodeInfo for the first node in the cluster, and
     * an AntBuilder which may be used to wait on conditions before returning.
     */
    @Input
    Closure unicastTransportUri = { NodeInfo seedNode, NodeInfo node, AntBuilder ant ->
        if (seedNode == node) {
            return null
        }
        ant.waitfor(maxwait: '40', maxwaitunit: 'second', checkevery: '500', checkeveryunit: 'millisecond') {
            resourceexists {
                file(file: seedNode.transportPortsFile.toString())
            }
        }
        return seedNode.transportUri()
    }

    /**
     * A closure to call before the cluster is considered ready. The closure is passed the node info,
     * as well as a groovy AntBuilder, to enable running ant condition checks. The default wait
     * condition is for http on the http port.
     */
    @Input
    Closure waitCondition = { NodeInfo node, AntBuilder ant ->
        File tmpFile = new File(node.cwd, 'wait.success')
        String waitUrl = "http://${node.httpUri()}/_cluster/health?wait_for_nodes=>=${numNodes}&wait_for_status=yellow"
        ant.echo(message: "==> [${new Date()}] checking health: ${waitUrl}",
                 level: 'info')
        // checking here for wait_for_nodes to be >= the number of nodes because its possible
        // this cluster is attempting to connect to nodes created by another task (same cluster name),
        // so there will be more nodes in that case in the cluster state
        ant.get(src: waitUrl,
                dest: tmpFile.toString(),
                ignoreerrors: true, // do not fail on error, so logging buffers can be flushed by the wait task
                retries: 10)
        return tmpFile.exists()
    }

    public ClusterConfiguration(Project project) {
        this.project = project
    }

    Map<String, String> systemProperties = new HashMap<>()

    Map<String, Object> settings = new HashMap<>()

    Map<String, String> keystoreSettings = new HashMap<>()

    // map from destination path, to source file
    Map<String, Object> extraConfigFiles = new HashMap<>()

    LinkedHashMap<String, Project> plugins = new LinkedHashMap<>()

    List<Project> modules = new ArrayList<>()

    LinkedHashMap<String, Object[]> setupCommands = new LinkedHashMap<>()

    List<Object> dependencies = new ArrayList<>()

    @Input
    void systemProperty(String property, String value) {
        systemProperties.put(property, value)
    }

    @Input
    void setting(String name, Object value) {
        settings.put(name, value)
    }

    @Input
    void keystoreSetting(String name, String value) {
        keystoreSettings.put(name, value)
    }

    @Input
    void plugin(String path) {
        Project pluginProject = project.project(path)
        plugins.put(pluginProject.name, pluginProject)
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

    /** Add dependencies that must be run before the first task setting up the cluster. */
    @Input
    void dependsOn(Object... deps) {
        dependencies.addAll(deps)
    }
}
