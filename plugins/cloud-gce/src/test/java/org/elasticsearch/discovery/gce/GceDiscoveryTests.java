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

package org.elasticsearch.discovery.gce;

import org.elasticsearch.Version;
import org.elasticsearch.cloud.gce.GceComputeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.local.LocalTransport;
import org.junit.*;

import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * This test class uses a GCE HTTP Mock system which allows to simulate JSON Responses.
 *
 * To implement a new test you'll need to create an `instances.json` file which contains expected response
 * for a given project-id and zone under the src/test/resources/org/elasticsearch/discovery/gce with dir name:
 *
 * compute/v1/projects/[project-id]/zones/[zone]
 *
 * By default, project-id is the test method name, lowercase.
 *
 * For example, if you create a test `myNewAwesomeTest` with following settings:
 *
 * Settings nodeSettings = Settings.builder()
 *  .put(GceComputeService.Fields.PROJECT, projectName)
 *  .put(GceComputeService.Fields.ZONE, "europe-west1-b")
 *  .build();
 *
 *  You need to create a file under `src/test/resources/org/elasticsearch/discovery/gce/` named:
 *
 *  compute/v1/projects/mynewawesometest/zones/europe-west1-b/instances.json
 *
 */
public class GceDiscoveryTests extends ESTestCase {

    protected static ThreadPool threadPool;
    protected MockTransportService transportService;
    protected GceComputeService mock;
    protected String projectName;

    @BeforeClass
    public static void createThreadPool() {
        threadPool = new ThreadPool(GceDiscoveryTests.class.getName());
    }

    @AfterClass
    public static void stopThreadPool() {
        if (threadPool !=null) {
            threadPool.shutdownNow();
            threadPool = null;
        }
    }

    @Before
    public void setProjectName() {
        projectName = getTestName().toLowerCase(Locale.ROOT);
    }

    @Before
    public void createTransportService() {
        transportService = new MockTransportService(
                Settings.EMPTY,
                new LocalTransport(Settings.EMPTY, threadPool, Version.CURRENT, new NamedWriteableRegistry()), threadPool);
    }

    @After
    public void stopGceComputeService() {
        if (mock != null) {
            mock.stop();
        }
    }

    protected List<DiscoveryNode> buildDynamicNodes(GceComputeService gceComputeService, Settings nodeSettings) {
        GceUnicastHostsProvider provider = new GceUnicastHostsProvider(nodeSettings, gceComputeService,
                transportService, new NetworkService(Settings.EMPTY), Version.CURRENT);

        List<DiscoveryNode> discoveryNodes = provider.buildDynamicNodes();
        logger.info("--> nodes found: {}", discoveryNodes);
        return discoveryNodes;
    }

    @Test
    public void nodesWithDifferentTagsAndNoTagSet() {
        Settings nodeSettings = Settings.builder()
                .put(GceComputeService.Fields.PROJECT, projectName)
                .put(GceComputeService.Fields.ZONE, "europe-west1-b")
                .build();
        mock = new GceComputeServiceMock(nodeSettings);
        List<DiscoveryNode> discoveryNodes = buildDynamicNodes(mock, nodeSettings);
        assertThat(discoveryNodes, hasSize(2));
    }

    @Test
    public void nodesWithDifferentTagsAndOneTagSet() {
        Settings nodeSettings = Settings.builder()
                .put(GceComputeService.Fields.PROJECT, projectName)
                .put(GceComputeService.Fields.ZONE, "europe-west1-b")
                .putArray(GceComputeService.Fields.TAGS, "elasticsearch")
                .build();
        mock = new GceComputeServiceMock(nodeSettings);
        List<DiscoveryNode> discoveryNodes = buildDynamicNodes(mock, nodeSettings);
        assertThat(discoveryNodes, hasSize(1));
        assertThat(discoveryNodes.get(0).getId(), is("#cloud-test2-0"));
    }

    @Test
    public void nodesWithDifferentTagsAndTwoTagSet() {
        Settings nodeSettings = Settings.builder()
                .put(GceComputeService.Fields.PROJECT, projectName)
                .put(GceComputeService.Fields.ZONE, "europe-west1-b")
                .putArray(GceComputeService.Fields.TAGS, "elasticsearch", "dev")
                .build();
        mock = new GceComputeServiceMock(nodeSettings);
        List<DiscoveryNode> discoveryNodes = buildDynamicNodes(mock, nodeSettings);
        assertThat(discoveryNodes, hasSize(1));
        assertThat(discoveryNodes.get(0).getId(), is("#cloud-test2-0"));
    }

    @Test
    public void nodesWithSameTagsAndNoTagSet() {
        Settings nodeSettings = Settings.builder()
                .put(GceComputeService.Fields.PROJECT, projectName)
                .put(GceComputeService.Fields.ZONE, "europe-west1-b")
                .build();
        mock = new GceComputeServiceMock(nodeSettings);
        List<DiscoveryNode> discoveryNodes = buildDynamicNodes(mock, nodeSettings);
        assertThat(discoveryNodes, hasSize(2));
    }

    @Test
    public void nodesWithSameTagsAndOneTagSet() {
        Settings nodeSettings = Settings.builder()
                .put(GceComputeService.Fields.PROJECT, projectName)
                .put(GceComputeService.Fields.ZONE, "europe-west1-b")
                .putArray(GceComputeService.Fields.TAGS, "elasticsearch")
                .build();
        mock = new GceComputeServiceMock(nodeSettings);
        List<DiscoveryNode> discoveryNodes = buildDynamicNodes(mock, nodeSettings);
        assertThat(discoveryNodes, hasSize(2));
    }

    @Test
    public void nodesWithSameTagsAndTwoTagsSet() {
        Settings nodeSettings = Settings.builder()
                .put(GceComputeService.Fields.PROJECT, projectName)
                .put(GceComputeService.Fields.ZONE, "europe-west1-b")
                .putArray(GceComputeService.Fields.TAGS, "elasticsearch", "dev")
                .build();
        mock = new GceComputeServiceMock(nodeSettings);
        List<DiscoveryNode> discoveryNodes = buildDynamicNodes(mock, nodeSettings);
        assertThat(discoveryNodes, hasSize(2));
    }

    @Test
    public void multipleZonesAndTwoNodesInSameZone() {
        Settings nodeSettings = Settings.builder()
                .put(GceComputeService.Fields.PROJECT, projectName)
                .putArray(GceComputeService.Fields.ZONE, "us-central1-a", "europe-west1-b")
                .build();
        mock = new GceComputeServiceMock(nodeSettings);
        List<DiscoveryNode> discoveryNodes = buildDynamicNodes(mock, nodeSettings);
        assertThat(discoveryNodes, hasSize(2));
    }

    @Test
    public void multipleZonesAndTwoNodesInDifferentZones() {
        Settings nodeSettings = Settings.builder()
                .put(GceComputeService.Fields.PROJECT, projectName)
                .putArray(GceComputeService.Fields.ZONE, "us-central1-a", "europe-west1-b")
                .build();
        mock = new GceComputeServiceMock(nodeSettings);
        List<DiscoveryNode> discoveryNodes = buildDynamicNodes(mock, nodeSettings);
        assertThat(discoveryNodes, hasSize(2));
    }

    /**
     * For issue https://github.com/elastic/elasticsearch-cloud-gce/issues/43
     */
    @Test
    public void zeroNode43() {
        Settings nodeSettings = Settings.builder()
                .put(GceComputeService.Fields.PROJECT, projectName)
                .putArray(GceComputeService.Fields.ZONE, "us-central1-a", "us-central1-b")
                .build();
        mock = new GceComputeServiceMock(nodeSettings);
        List<DiscoveryNode> discoveryNodes = buildDynamicNodes(mock, nodeSettings);
        assertThat(discoveryNodes, hasSize(0));
    }
}
