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
import org.elasticsearch.cloud.gce.GceInstancesServiceImpl;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
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
 * By default, project-id is the test method name, lowercase and missing the "test" prefix.
 *
 * For example, if you create a test `myNewAwesomeTest` with following settings:
 *
 * Settings nodeSettings = Settings.builder()
 *  .put(GceComputeService.PROJECT, projectName)
 *  .put(GceComputeService.ZONE, "europe-west1-b")
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
    protected GceInstancesServiceMock mock;
    protected String projectName;

    @BeforeClass
    public static void createThreadPool() {
        threadPool = new TestThreadPool(GceDiscoveryTests.class.getName());
    }

    @AfterClass
    public static void stopThreadPool() {
        if (threadPool != null) {
            threadPool.shutdownNow();
            threadPool = null;
        }
    }

    @Before
    public void setProjectName() {
        projectName = getTestName().toLowerCase(Locale.ROOT);
        // Slice off the "test" part of the method names so the project names
        if (projectName.startsWith("test")) {
            projectName = projectName.substring("test".length());
        }
    }

    @Before
    public void createTransportService() {
        transportService = MockTransportService.local(Settings.EMPTY, Version.CURRENT, threadPool);
    }

    @After
    public void stopGceComputeService() {
        if (mock != null) {
            mock.stop();
        }
    }

    protected List<DiscoveryNode> buildDynamicNodes(GceInstancesServiceImpl gceInstancesService, Settings nodeSettings) {
        GceUnicastHostsProvider provider = new GceUnicastHostsProvider(nodeSettings, gceInstancesService,
            transportService, new NetworkService(Settings.EMPTY, Collections.emptyList()));

        List<DiscoveryNode> discoveryNodes = provider.buildDynamicNodes();
        logger.info("--> nodes found: {}", discoveryNodes);
        return discoveryNodes;
    }

    public void testNodesWithDifferentTagsAndNoTagSet() {
        Settings nodeSettings = Settings.builder()
                .put(GceInstancesServiceImpl.PROJECT_SETTING.getKey(), projectName)
                .put(GceInstancesServiceImpl.ZONE_SETTING.getKey(), "europe-west1-b")
                .build();
        mock = new GceInstancesServiceMock(nodeSettings);
        List<DiscoveryNode> discoveryNodes = buildDynamicNodes(mock, nodeSettings);
        assertThat(discoveryNodes, hasSize(2));
    }

    public void testNodesWithDifferentTagsAndOneTagSet() {
        Settings nodeSettings = Settings.builder()
                .put(GceInstancesServiceImpl.PROJECT_SETTING.getKey(), projectName)
                .put(GceInstancesServiceImpl.ZONE_SETTING.getKey(), "europe-west1-b")
                .putArray(GceUnicastHostsProvider.TAGS_SETTING.getKey(), "elasticsearch")
                .build();
        mock = new GceInstancesServiceMock(nodeSettings);
        List<DiscoveryNode> discoveryNodes = buildDynamicNodes(mock, nodeSettings);
        assertThat(discoveryNodes, hasSize(1));
        assertThat(discoveryNodes.get(0).getId(), is("#cloud-test2-0"));
    }

    public void testNodesWithDifferentTagsAndTwoTagSet() {
        Settings nodeSettings = Settings.builder()
                .put(GceInstancesServiceImpl.PROJECT_SETTING.getKey(), projectName)
                .put(GceInstancesServiceImpl.ZONE_SETTING.getKey(), "europe-west1-b")
                .putArray(GceUnicastHostsProvider.TAGS_SETTING.getKey(), "elasticsearch", "dev")
                .build();
        mock = new GceInstancesServiceMock(nodeSettings);
        List<DiscoveryNode> discoveryNodes = buildDynamicNodes(mock, nodeSettings);
        assertThat(discoveryNodes, hasSize(1));
        assertThat(discoveryNodes.get(0).getId(), is("#cloud-test2-0"));
    }

    public void testNodesWithSameTagsAndNoTagSet() {
        Settings nodeSettings = Settings.builder()
                .put(GceInstancesServiceImpl.PROJECT_SETTING.getKey(), projectName)
                .put(GceInstancesServiceImpl.ZONE_SETTING.getKey(), "europe-west1-b")
                .build();
        mock = new GceInstancesServiceMock(nodeSettings);
        List<DiscoveryNode> discoveryNodes = buildDynamicNodes(mock, nodeSettings);
        assertThat(discoveryNodes, hasSize(2));
    }

    public void testNodesWithSameTagsAndOneTagSet() {
        Settings nodeSettings = Settings.builder()
                .put(GceInstancesServiceImpl.PROJECT_SETTING.getKey(), projectName)
                .put(GceInstancesServiceImpl.ZONE_SETTING.getKey(), "europe-west1-b")
                .putArray(GceUnicastHostsProvider.TAGS_SETTING.getKey(), "elasticsearch")
                .build();
        mock = new GceInstancesServiceMock(nodeSettings);
        List<DiscoveryNode> discoveryNodes = buildDynamicNodes(mock, nodeSettings);
        assertThat(discoveryNodes, hasSize(2));
    }

    public void testNodesWithSameTagsAndTwoTagsSet() {
        Settings nodeSettings = Settings.builder()
                .put(GceInstancesServiceImpl.PROJECT_SETTING.getKey(), projectName)
                .put(GceInstancesServiceImpl.ZONE_SETTING.getKey(), "europe-west1-b")
                .putArray(GceUnicastHostsProvider.TAGS_SETTING.getKey(), "elasticsearch", "dev")
                .build();
        mock = new GceInstancesServiceMock(nodeSettings);
        List<DiscoveryNode> discoveryNodes = buildDynamicNodes(mock, nodeSettings);
        assertThat(discoveryNodes, hasSize(2));
    }

    public void testMultipleZonesAndTwoNodesInSameZone() {
        Settings nodeSettings = Settings.builder()
                .put(GceInstancesServiceImpl.PROJECT_SETTING.getKey(), projectName)
                .putArray(GceInstancesServiceImpl.ZONE_SETTING.getKey(), "us-central1-a", "europe-west1-b")
                .build();
        mock = new GceInstancesServiceMock(nodeSettings);
        List<DiscoveryNode> discoveryNodes = buildDynamicNodes(mock, nodeSettings);
        assertThat(discoveryNodes, hasSize(2));
    }

    public void testMultipleZonesAndTwoNodesInDifferentZones() {
        Settings nodeSettings = Settings.builder()
                .put(GceInstancesServiceImpl.PROJECT_SETTING.getKey(), projectName)
                .putArray(GceInstancesServiceImpl.ZONE_SETTING.getKey(), "us-central1-a", "europe-west1-b")
                .build();
        mock = new GceInstancesServiceMock(nodeSettings);
        List<DiscoveryNode> discoveryNodes = buildDynamicNodes(mock, nodeSettings);
        assertThat(discoveryNodes, hasSize(2));
    }

    /**
     * For issue https://github.com/elastic/elasticsearch-cloud-gce/issues/43
     */
    public void testZeroNode43() {
        Settings nodeSettings = Settings.builder()
                .put(GceInstancesServiceImpl.PROJECT_SETTING.getKey(), projectName)
                .putArray(GceInstancesServiceImpl.ZONE_SETTING.getKey(), "us-central1-a", "us-central1-b")
                .build();
        mock = new GceInstancesServiceMock(nodeSettings);
        List<DiscoveryNode> discoveryNodes = buildDynamicNodes(mock, nodeSettings);
        assertThat(discoveryNodes, hasSize(0));
    }

    public void testIllegalSettingsMissingAllRequired() {
        Settings nodeSettings = Settings.EMPTY;
        mock = new GceInstancesServiceMock(nodeSettings);
        try {
            buildDynamicNodes(mock, nodeSettings);
            fail("We expect an IllegalArgumentException for incomplete settings");
        } catch (IllegalArgumentException expected) {
            assertThat(expected.getMessage(), containsString("one or more gce discovery settings are missing."));
        }
    }

    public void testIllegalSettingsMissingProject() {
        Settings nodeSettings = Settings.builder()
            .putArray(GceInstancesServiceImpl.ZONE_SETTING.getKey(), "us-central1-a", "us-central1-b")
            .build();
        mock = new GceInstancesServiceMock(nodeSettings);
        try {
            buildDynamicNodes(mock, nodeSettings);
            fail("We expect an IllegalArgumentException for incomplete settings");
        } catch (IllegalArgumentException expected) {
            assertThat(expected.getMessage(), containsString("one or more gce discovery settings are missing."));
        }
    }

    public void testIllegalSettingsMissingZone() {
        Settings nodeSettings = Settings.builder()
            .put(GceInstancesServiceImpl.PROJECT_SETTING.getKey(), projectName)
            .build();
        mock = new GceInstancesServiceMock(nodeSettings);
        try {
            buildDynamicNodes(mock, nodeSettings);
            fail("We expect an IllegalArgumentException for incomplete settings");
        } catch (IllegalArgumentException expected) {
            assertThat(expected.getMessage(), containsString("one or more gce discovery settings are missing."));
        }
    }

    /**
     * For issue https://github.com/elastic/elasticsearch/issues/16967:
     * When using multiple regions and one of them has no instance at all, this
     * was producing a NPE as a result.
     */
    public void testNoRegionReturnsEmptyList() {
        Settings nodeSettings = Settings.builder()
            .put(GceInstancesServiceImpl.PROJECT_SETTING.getKey(), projectName)
            .putArray(GceInstancesServiceImpl.ZONE_SETTING.getKey(), "europe-west1-b", "us-central1-a")
            .build();
        mock = new GceInstancesServiceMock(nodeSettings);
        List<DiscoveryNode> discoveryNodes = buildDynamicNodes(mock, nodeSettings);
        assertThat(discoveryNodes, hasSize(1));
    }
}
