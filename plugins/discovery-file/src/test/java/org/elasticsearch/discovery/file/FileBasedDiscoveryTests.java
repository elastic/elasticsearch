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

package org.elasticsearch.discovery.file;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportSettings;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;

/**
 * Integration tests to ensure the file based discovery plugin works
 * properly in a cluster.
 */
@ESIntegTestCase.SuppressLocalMode
@ESIntegTestCase.ClusterScope(supportsDedicatedMasters = false, numDataNodes = 2, numClientNodes = 0)
public class FileBasedDiscoveryTests extends ESIntegTestCase {

    private static final int BASE_PORT = 14723; // pick a port unlikely to be used by any other running process
    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int NUM_NODES = 2;
    private static Path CONFIG_DIR;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(FileBasedDiscoveryPlugin.class);
    }

    @BeforeClass
    public static void initUnicastHostsFile() throws Exception {
        CONFIG_DIR = createTempDir().resolve("config");
        // write the unicast_hosts.txt file, only write it once as all nodes share the same config dir
        writeUnicastHostsFileForNodes(CONFIG_DIR);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        // set the network host to a known binding so we can configure unicast_hosts.txt accordingly
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
                   .put(Environment.PATH_CONF_SETTING.getKey(), CONFIG_DIR)
                   .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), "zen")
                   .put(TransportSettings.HOST.getKey(), DEFAULT_HOST)
                   .put(TransportSettings.PORT.getKey(), Integer.toString(BASE_PORT + nodeOrdinal))
                   .build();
    }

    private static List<String> getHostAddresses() {
        final List<String> entries = new ArrayList<>();
        for (int i = 0; i < NUM_NODES; i++) {
            entries.add(DEFAULT_HOST + ":" + Integer.toString(BASE_PORT + i));
        }
        return entries;
    }

    private static void writeUnicastHostsFileForNodes(final Path configDir) throws IOException {
        final List<String> entries = getHostAddresses();
        final byte[] fileContents = String.join("\n", entries).getBytes(StandardCharsets.UTF_8);
        // write the unicast_hosts.txt file for each node
        final Path discoFileDir = configDir.resolve("discovery-file");
        Files.createDirectories(discoFileDir);
        Files.write(discoFileDir.resolve("unicast_hosts.txt"), fileContents);
    }

    public void testNodesJoinCluster() throws Exception {
        //  wait for the cluster to form with the specified number of nodes
        assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(NUM_NODES)).get());
        // check that the discovery nodes in the cluster state have the expected transport addresses
        final ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
        final DiscoveryNodes discoveryNodes = clusterService.state().getNodes();
        assertEquals(NUM_NODES, discoveryNodes.getSize());
        final Iterator<DiscoveryNode> iter = discoveryNodes.getNodes().valuesIt();
        while (iter.hasNext()) {
            final DiscoveryNode node = iter.next();
            assertEquals(DEFAULT_HOST, node.getAddress().getHost());
            final int port = node.getAddress().getPort();
            boolean portFound = false;
            for (int i = 0; i < NUM_NODES; i++) {
                portFound |= (port == BASE_PORT + i);
            }
            assertTrue(portFound);
        }
    }

}
