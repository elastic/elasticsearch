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
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.nio.MockNioTransport;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class Ec2DiscoveryTests extends ESTestCase {

    protected static ThreadPool threadPool;
    protected MockTransportService transportService;
    private Map<String, TransportAddress> poorMansDNS = new ConcurrentHashMap<>();

    @BeforeClass
    public static void createThreadPool() {
        threadPool = new TestThreadPool(Ec2DiscoveryTests.class.getName());
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
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        final Transport transport = new MockNioTransport(Settings.EMPTY, Version.CURRENT, threadPool,
            new NetworkService(Collections.emptyList()), PageCacheRecycler.NON_RECYCLING_INSTANCE, namedWriteableRegistry,
            new NoneCircuitBreakerService()) {
            @Override
            public TransportAddress[] addressesFromString(String address) throws UnknownHostException {
                // we just need to ensure we don't resolve DNS here
                return new TransportAddress[] {poorMansDNS.getOrDefault(address, buildNewFakeTransportAddress())};
            }
        };
        transportService = new MockTransportService(Settings.EMPTY, transport, threadPool, TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                null);
    }

    protected List<TransportAddress> buildDynamicHosts(Settings nodeSettings, int nodes) {
        return buildDynamicHosts(nodeSettings, nodes, null);
    }

    protected List<TransportAddress> buildDynamicHosts(Settings nodeSettings, int nodes, List<List<Tag>> tagsList) {
        try (Ec2DiscoveryPluginMock plugin = new Ec2DiscoveryPluginMock(Settings.EMPTY, nodes, tagsList)) {
            AwsEc2SeedHostsProvider provider = new AwsEc2SeedHostsProvider(nodeSettings, transportService, plugin.ec2Service);
            List<TransportAddress> dynamicHosts = provider.getSeedAddresses(null);
            logger.debug("--> addresses found: {}", dynamicHosts);
            return dynamicHosts;
        } catch (IOException e) {
            fail("Unexpected IOException");
            return null;
        }
    }

    public void testDefaultSettings() throws InterruptedException {
        int nodes = randomInt(10);
        Settings nodeSettings = Settings.builder()
                .build();
        List<TransportAddress> discoveryNodes = buildDynamicHosts(nodeSettings, nodes);
        assertThat(discoveryNodes, hasSize(nodes));
    }

    public void testPrivateIp() throws InterruptedException {
        int nodes = randomInt(10);
        for (int i = 0; i < nodes; i++) {
            poorMansDNS.put(AmazonEC2Mock.PREFIX_PRIVATE_IP + (i+1), buildNewFakeTransportAddress());
        }
        Settings nodeSettings = Settings.builder()
                .put(AwsEc2Service.HOST_TYPE_SETTING.getKey(), "private_ip")
                .build();
        List<TransportAddress> transportAddresses = buildDynamicHosts(nodeSettings, nodes);
        assertThat(transportAddresses, hasSize(nodes));
        // We check that we are using here expected address
        int node = 1;
        for (TransportAddress address : transportAddresses) {
            TransportAddress expected = poorMansDNS.get(AmazonEC2Mock.PREFIX_PRIVATE_IP + node++);
            assertEquals(address, expected);
        }
    }

    public void testPublicIp() throws InterruptedException {
        int nodes = randomInt(10);
        for (int i = 0; i < nodes; i++) {
            poorMansDNS.put(AmazonEC2Mock.PREFIX_PUBLIC_IP + (i+1), buildNewFakeTransportAddress());
        }
        Settings nodeSettings = Settings.builder()
                .put(AwsEc2Service.HOST_TYPE_SETTING.getKey(), "public_ip")
                .build();
        List<TransportAddress> dynamicHosts = buildDynamicHosts(nodeSettings, nodes);
        assertThat(dynamicHosts, hasSize(nodes));
        // We check that we are using here expected address
        int node = 1;
        for (TransportAddress address : dynamicHosts) {
            TransportAddress expected = poorMansDNS.get(AmazonEC2Mock.PREFIX_PUBLIC_IP + node++);
            assertEquals(address, expected);
        }
    }

    public void testPrivateDns() throws InterruptedException {
        int nodes = randomInt(10);
        for (int i = 0; i < nodes; i++) {
            String instanceId = "node" + (i+1);
            poorMansDNS.put(AmazonEC2Mock.PREFIX_PRIVATE_DNS + instanceId +
                AmazonEC2Mock.SUFFIX_PRIVATE_DNS, buildNewFakeTransportAddress());
        }
        Settings nodeSettings = Settings.builder()
                .put(AwsEc2Service.HOST_TYPE_SETTING.getKey(), "private_dns")
                .build();
        List<TransportAddress> dynamicHosts = buildDynamicHosts(nodeSettings, nodes);
        assertThat(dynamicHosts, hasSize(nodes));
        // We check that we are using here expected address
        int node = 1;
        for (TransportAddress address : dynamicHosts) {
            String instanceId = "node" + node++;
            TransportAddress expected = poorMansDNS.get(
                    AmazonEC2Mock.PREFIX_PRIVATE_DNS + instanceId + AmazonEC2Mock.SUFFIX_PRIVATE_DNS);
            assertEquals(address, expected);
        }
    }

    public void testPublicDns() throws InterruptedException {
        int nodes = randomInt(10);
        for (int i = 0; i < nodes; i++) {
            String instanceId = "node" + (i+1);
            poorMansDNS.put(AmazonEC2Mock.PREFIX_PUBLIC_DNS + instanceId
                + AmazonEC2Mock.SUFFIX_PUBLIC_DNS, buildNewFakeTransportAddress());
        }
        Settings nodeSettings = Settings.builder()
                .put(AwsEc2Service.HOST_TYPE_SETTING.getKey(), "public_dns")
                .build();
        List<TransportAddress> dynamicHosts = buildDynamicHosts(nodeSettings, nodes);
        assertThat(dynamicHosts, hasSize(nodes));
        // We check that we are using here expected address
        int node = 1;
        for (TransportAddress address : dynamicHosts) {
            String instanceId = "node" + node++;
            TransportAddress expected = poorMansDNS.get(
                    AmazonEC2Mock.PREFIX_PUBLIC_DNS + instanceId + AmazonEC2Mock.SUFFIX_PUBLIC_DNS);
            assertEquals(address, expected);
        }
    }

    public void testInvalidHostType() throws InterruptedException {
        Settings nodeSettings = Settings.builder()
                .put(AwsEc2Service.HOST_TYPE_SETTING.getKey(), "does_not_exist")
                .build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            buildDynamicHosts(nodeSettings, 1);
        });
        assertThat(exception.getMessage(), containsString("does_not_exist is unknown for discovery.ec2.host_type"));
    }

    public void testFilterByTags() throws InterruptedException {
        int nodes = randomIntBetween(5, 10);
        Settings nodeSettings = Settings.builder()
                .put(AwsEc2Service.TAG_SETTING.getKey() + "stage", "prod")
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

        logger.info("started [{}] instances with [{}] stage=prod tag", nodes, prodInstances);
        List<TransportAddress> dynamicHosts = buildDynamicHosts(nodeSettings, nodes, tagsList);
        assertThat(dynamicHosts, hasSize(prodInstances));
    }

    public void testFilterByMultipleTags() throws InterruptedException {
        int nodes = randomIntBetween(5, 10);
        Settings nodeSettings = Settings.builder()
                .putList(AwsEc2Service.TAG_SETTING.getKey() + "stage", "prod", "preprod")
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

        logger.info("started [{}] instances with [{}] stage=prod tag", nodes, prodInstances);
        List<TransportAddress> dynamicHosts = buildDynamicHosts(nodeSettings, nodes, tagsList);
        assertThat(dynamicHosts, hasSize(prodInstances));
    }

    public void testReadHostFromTag() throws UnknownHostException {
        int nodes = randomIntBetween(5, 10);

        String[] addresses = new String[nodes];

        for (int node = 0; node < nodes; node++) {
            addresses[node] = "192.168.0." + (node + 1);
            poorMansDNS.put("node" + (node + 1), new TransportAddress(InetAddress.getByName(addresses[node]), 9300));
        }

        Settings nodeSettings = Settings.builder()
            .put(AwsEc2Service.HOST_TYPE_SETTING.getKey(), "tag:foo")
            .build();

        List<List<Tag>> tagsList = new ArrayList<>();

        for (int node = 0; node < nodes; node++) {
            List<Tag> tags = new ArrayList<>();
            tags.add(new Tag("foo", "node" + (node + 1)));
            tagsList.add(tags);
        }

        logger.info("started [{}] instances", nodes);
        List<TransportAddress> dynamicHosts = buildDynamicHosts(nodeSettings, nodes, tagsList);
        assertThat(dynamicHosts, hasSize(nodes));
        int node = 1;
        for (TransportAddress address : dynamicHosts) {
            TransportAddress expected = poorMansDNS.get("node" + node++);
            assertEquals(address, expected);
        }
    }


    abstract class DummyEc2SeedHostsProvider extends AwsEc2SeedHostsProvider {
        public int fetchCount = 0;
        DummyEc2SeedHostsProvider(Settings settings, TransportService transportService, AwsEc2Service service) {
            super(settings, transportService, service);
        }
    }

    public void testGetNodeListEmptyCache() {
        AwsEc2Service awsEc2Service = new AwsEc2ServiceMock(1, null);
        DummyEc2SeedHostsProvider provider = new DummyEc2SeedHostsProvider(Settings.EMPTY, transportService, awsEc2Service) {
            @Override
            protected List<TransportAddress> fetchDynamicNodes() {
                fetchCount++;
                return new ArrayList<>();
            }
        };
        for (int i=0; i<3; i++) {
            provider.getSeedAddresses(null);
        }
        assertThat(provider.fetchCount, is(3));
    }

    public void testGetNodeListCached() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put(AwsEc2Service.NODE_CACHE_TIME_SETTING.getKey(), "500ms");
        try (Ec2DiscoveryPluginMock plugin = new Ec2DiscoveryPluginMock(Settings.EMPTY)) {
            DummyEc2SeedHostsProvider provider = new DummyEc2SeedHostsProvider(builder.build(), transportService, plugin.ec2Service) {
                @Override
                protected List<TransportAddress> fetchDynamicNodes() {
                    fetchCount++;
                    return Ec2DiscoveryTests.this.buildDynamicHosts(Settings.EMPTY, 1);
                }
            };
            for (int i=0; i<3; i++) {
                provider.getSeedAddresses(null);
            }
            assertThat(provider.fetchCount, is(1));
            Thread.sleep(1_000L); // wait for cache to expire
            for (int i=0; i<3; i++) {
                provider.getSeedAddresses(null);
            }
            assertThat(provider.fetchCount, is(2));
        }
    }
}
