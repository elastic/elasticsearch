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

package org.elasticsearch.discovery.ec2;

import com.amazonaws.services.ec2.model.Tag;
import org.elasticsearch.Version;
import org.elasticsearch.cloud.aws.AwsEc2Service;
import org.elasticsearch.cloud.aws.AwsEc2Service.DISCOVERY_EC2;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.local.LocalTransport;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class Ec2DiscoveryTest extends ESTestCase {

    protected static ThreadPool threadPool;
    protected MockTransportService transportService;

    @BeforeClass
    public static void createThreadPool() {
        threadPool = new ThreadPool(Ec2DiscoveryTest.class.getName());
    }

    @AfterClass
    public static void stopThreadPool() throws InterruptedException {
        if (threadPool !=null) {
            terminate(threadPool);
            threadPool = null;
        }
    }

    @Before
    public void createTransportService() {
        transportService = new MockTransportService(
                Settings.EMPTY,
                new LocalTransport(Settings.EMPTY, threadPool, Version.CURRENT, new NamedWriteableRegistry()), threadPool);
    }

    protected List<DiscoveryNode> buildDynamicNodes(Settings nodeSettings, int nodes) {
        return buildDynamicNodes(nodeSettings, nodes, null);
    }

    protected List<DiscoveryNode> buildDynamicNodes(Settings nodeSettings, int nodes, List<List<Tag>> tagsList) {
        AwsEc2Service awsEc2Service = new AwsEc2ServiceMock(nodeSettings, nodes, tagsList);

        AwsEc2UnicastHostsProvider provider = new AwsEc2UnicastHostsProvider(nodeSettings, transportService,
                awsEc2Service, Version.CURRENT);

        List<DiscoveryNode> discoveryNodes = provider.buildDynamicNodes();
        logger.debug("--> nodes found: {}", discoveryNodes);
        return discoveryNodes;
    }

    @Test
    public void defaultSettings() throws InterruptedException {
        int nodes = randomInt(10);
        Settings nodeSettings = Settings.builder()
                .build();
        List<DiscoveryNode> discoveryNodes = buildDynamicNodes(nodeSettings, nodes);
        assertThat(discoveryNodes, hasSize(nodes));
    }

    @Test
    public void privateIp() throws InterruptedException {
        int nodes = randomInt(10);
        Settings nodeSettings = Settings.builder()
                .put(DISCOVERY_EC2.HOST_TYPE, "private_ip")
                .build();
        List<DiscoveryNode> discoveryNodes = buildDynamicNodes(nodeSettings, nodes);
        assertThat(discoveryNodes, hasSize(nodes));
        // We check that we are using here expected address
        int node = 1;
        for (DiscoveryNode discoveryNode : discoveryNodes) {
            TransportAddress address = discoveryNode.getAddress();
            TransportAddress expected = new LocalTransportAddress(AmazonEC2Mock.PREFIX_PRIVATE_IP + node++);
            assertThat(address.sameHost(expected), is(true));
        }
    }

    @Test
    public void publicIp() throws InterruptedException {
        int nodes = randomInt(10);
        Settings nodeSettings = Settings.builder()
                .put(DISCOVERY_EC2.HOST_TYPE, "public_ip")
                .build();
        List<DiscoveryNode> discoveryNodes = buildDynamicNodes(nodeSettings, nodes);
        assertThat(discoveryNodes, hasSize(nodes));
        // We check that we are using here expected address
        int node = 1;
        for (DiscoveryNode discoveryNode : discoveryNodes) {
            TransportAddress address = discoveryNode.getAddress();
            TransportAddress expected = new LocalTransportAddress(AmazonEC2Mock.PREFIX_PUBLIC_IP + node++);
            assertThat(address.sameHost(expected), is(true));
        }
    }

    @Test
    public void privateDns() throws InterruptedException {
        int nodes = randomInt(10);
        Settings nodeSettings = Settings.builder()
                .put(DISCOVERY_EC2.HOST_TYPE, "private_dns")
                .build();
        List<DiscoveryNode> discoveryNodes = buildDynamicNodes(nodeSettings, nodes);
        assertThat(discoveryNodes, hasSize(nodes));
        // We check that we are using here expected address
        int node = 1;
        for (DiscoveryNode discoveryNode : discoveryNodes) {
            String instanceId = "node" + node++;
            TransportAddress address = discoveryNode.getAddress();
            TransportAddress expected = new LocalTransportAddress(
                    AmazonEC2Mock.PREFIX_PRIVATE_DNS + instanceId + AmazonEC2Mock.SUFFIX_PRIVATE_DNS);
            assertThat(address.sameHost(expected), is(true));
        }
    }

    @Test
    public void publicDns() throws InterruptedException {
        int nodes = randomInt(10);
        Settings nodeSettings = Settings.builder()
                .put(DISCOVERY_EC2.HOST_TYPE, "public_dns")
                .build();
        List<DiscoveryNode> discoveryNodes = buildDynamicNodes(nodeSettings, nodes);
        assertThat(discoveryNodes, hasSize(nodes));
        // We check that we are using here expected address
        int node = 1;
        for (DiscoveryNode discoveryNode : discoveryNodes) {
            String instanceId = "node" + node++;
            TransportAddress address = discoveryNode.getAddress();
            TransportAddress expected = new LocalTransportAddress(
                    AmazonEC2Mock.PREFIX_PUBLIC_DNS + instanceId + AmazonEC2Mock.SUFFIX_PUBLIC_DNS);
            assertThat(address.sameHost(expected), is(true));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidHostType() throws InterruptedException {
        Settings nodeSettings = Settings.builder()
                .put(DISCOVERY_EC2.HOST_TYPE, "does_not_exist")
                .build();
        buildDynamicNodes(nodeSettings, 1);
    }

    @Test
    public void filterByTags() throws InterruptedException {
        int nodes = randomIntBetween(5, 10);
        Settings nodeSettings = Settings.builder()
                .put(DISCOVERY_EC2.TAG_PREFIX + "stage", "prod")
                .build();

        int prodInstances = 0;
        List<List<Tag>> tagsList = new ArrayList<>();

        for (int node = 0; node < nodes; node++) {
            List<Tag> tags = new ArrayList<>();
            if (randomBoolean()) {
                tags.add(new Tag("stage", "prod"));
                prodInstances++;
            } else {
                tags.add(new Tag("stage", "dev"));
            }
            tagsList.add(tags);
        }

        logger.info("started [{}] instances with [{}] stage=prod tag");
        List<DiscoveryNode> discoveryNodes = buildDynamicNodes(nodeSettings, nodes, tagsList);
        assertThat(discoveryNodes, hasSize(prodInstances));
    }

    @Test
    public void filterByMultipleTags() throws InterruptedException {
        int nodes = randomIntBetween(5, 10);
        Settings nodeSettings = Settings.builder()
                .putArray(DISCOVERY_EC2.TAG_PREFIX + "stage", "prod", "preprod")
                .build();

        int prodInstances = 0;
        List<List<Tag>> tagsList = new ArrayList<>();

        for (int node = 0; node < nodes; node++) {
            List<Tag> tags = new ArrayList<>();
            if (randomBoolean()) {
                tags.add(new Tag("stage", "prod"));
                if (randomBoolean()) {
                    tags.add(new Tag("stage", "preprod"));
                    prodInstances++;
                }
            } else {
                tags.add(new Tag("stage", "dev"));
                if (randomBoolean()) {
                    tags.add(new Tag("stage", "preprod"));
                }
            }
            tagsList.add(tags);
        }

        logger.info("started [{}] instances with [{}] stage=prod tag");
        List<DiscoveryNode> discoveryNodes = buildDynamicNodes(nodeSettings, nodes, tagsList);
        assertThat(discoveryNodes, hasSize(prodInstances));
    }

}
