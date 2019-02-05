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

package org.elasticsearch.discovery.azure.arm;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.discovery.azure.arm.AzureVirtualMachine.PowerState;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.nio.MockNioTransport;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

public class AzureUnicastHostsProviderTests extends ESTestCase {

    private static ThreadPool threadPool;
    private MockTransportService transportService;
    private Map<String, TransportAddress> poorMansDNS;
    private List<AzureVirtualMachine> mockVms;
    private static final AzureVirtualMachine VM_PRIVATE_1 = new AzureVirtualMachine()
        .withName("azure-mock-private-1")
        .withPowerState(PowerState.RUNNING)
        .withPrivateIp("10.0.0.238")
        .withGroupName("azure-group")
        .withRegion("westeurope");
    private static final AzureVirtualMachine VM_1 = new AzureVirtualMachine()
        .withName("azure-mock-1")
        .withPowerState(PowerState.RUNNING)
        .withPrivateIp("10.0.0.238")
        .withPublicIp("8.8.8.8")
        .withGroupName("azure-group")
        .withRegion("westeurope");
    private static final AzureVirtualMachine VM_2 = new AzureVirtualMachine()
        .withName("azure-mock-2")
        .withPowerState(PowerState.RUNNING)
        .withPrivateIp("10.0.0.239")
        .withPublicIp("8.8.8.9")
        .withGroupName("azure-group")
        .withRegion("westeurope");
    private static final AzureVirtualMachine VM_STARTING = new AzureVirtualMachine()
        .withName("azure-mock-starting-1")
        .withPowerState(PowerState.STARTING)
        .withPrivateIp("10.0.0.100")
        .withGroupName("azure-group")
        .withRegion("westeurope");
    private static final AzureVirtualMachine VM_STOPPED = new AzureVirtualMachine()
        .withName("azure-mock-stopped-1")
        .withPowerState(PowerState.STOPPED)
        .withPrivateIp("10.0.0.101")
        .withGroupName("azure-group")
        .withRegion("westeurope");
    private static final AzureVirtualMachine VM_DEALLOCATED = new AzureVirtualMachine()
        .withName("azure-mock-deallocated-1")
        .withPowerState(PowerState.DEALLOCATED)
        .withPrivateIp("10.0.0.102")
        .withGroupName("azure-group")
        .withRegion("westeurope");
    private static final AzureVirtualMachine VM_GROUP_1_1 = new AzureVirtualMachine()
        .withName("azure-mock-group1-1")
        .withPowerState(PowerState.RUNNING)
        .withPrivateIp("10.0.0.150")
        .withGroupName("azure-group1")
        .withRegion("westeurope");
    private static final AzureVirtualMachine VM_GROUP_1_2 = new AzureVirtualMachine()
        .withName("azure-mock-group1-2")
        .withPowerState(PowerState.RUNNING)
        .withPrivateIp("10.0.0.151")
        .withGroupName("azure-group1")
        .withRegion("westeurope");
    private static final AzureVirtualMachine VM_GROUP_2_1 = new AzureVirtualMachine()
        .withName("azure-mock-group2-1")
        .withPowerState(PowerState.RUNNING)
        .withPrivateIp("10.0.0.160")
        .withGroupName("azure-group2")
        .withRegion("westeurope");
    private static final AzureVirtualMachine VM_GROUP_2_2 = new AzureVirtualMachine()
        .withName("azure-mock-group2-2")
        .withPowerState(PowerState.RUNNING)
        .withPrivateIp("10.0.0.161")
        .withGroupName("azure-group2")
        .withRegion("westeurope");
    /**
     * This is a weird instance which is running but has absolutely no IP address associated.
     * This can not happen in real life, but let's be paranoid and test that.
     */
    private static final AzureVirtualMachine VM_WEIRD = new AzureVirtualMachine()
        .withName("azure-mock-weird")
        .withPowerState(PowerState.RUNNING)
        .withGroupName("azure-group")
        .withRegion("westeurope");
    private static final AzureVirtualMachine VM_REGION_EASTUS = new AzureVirtualMachine()
        .withName("azure-mock-eastus")
        .withPowerState(PowerState.RUNNING)
        .withPrivateIp("10.0.0.50")
        .withGroupName("azure-group")
        .withRegion("eastus");
    private static final AzureVirtualMachine VM_REGION_WESTUS = new AzureVirtualMachine()
        .withName("azure-mock-westus")
        .withPowerState(PowerState.RUNNING)
        .withPrivateIp("10.0.1.50")
        .withGroupName("azure-group")
        .withRegion("westus");
    private static final AzureVirtualMachine VM_REGION_WESTEUROPE = new AzureVirtualMachine()
        .withName("azure-mock-westeurope")
        .withPowerState(PowerState.RUNNING)
        .withPrivateIp("10.0.2.50")
        .withGroupName("azure-group")
        .withRegion("westeurope");

    @BeforeClass
    public static void createThreadPool() {
        threadPool = new TestThreadPool(AzureUnicastHostsProviderTests.class.getName());
    }

    @AfterClass
    public static void stopThreadPool() throws InterruptedException {
        if (threadPool !=null) {
            terminate(threadPool);
            threadPool = null;
        }
    }

    @Before
    public void reinitDnsAndVirtualMachineMock() {
        poorMansDNS = new ConcurrentHashMap<>();
        mockVms = new ArrayList<>();
    }

    @Before
    public void createTransportService() {
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        final Transport transport = new MockNioTransport(Settings.EMPTY, Version.CURRENT, threadPool,
            new NetworkService(Collections.emptyList()), PageCacheRecycler.NON_RECYCLING_INSTANCE, namedWriteableRegistry,
            new NoneCircuitBreakerService()) {
            @Override
            public TransportAddress[] addressesFromString(String address, int perAddressLimit) {
                // we just need to ensure we don't resolve DNS here
                return new TransportAddress[] {
                    poorMansDNS.getOrDefault(address, buildNewFakeTransportAddress())
                };
            }
        };
        transportService = new MockTransportService(Settings.EMPTY, transport, threadPool, TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            null);
    }

    public void testDefaultSettings() {
        List<TransportAddress> addresses = runDiscoveryTest(Settings.EMPTY, VM_1);
        assertThat(addresses, hasSize(1));
        TransportAddress address = addresses.get(0);
        assertThat(address.getAddress(), is(VM_1.getPrivateIp()));
        assertThat(address.getPort(), is(9300));
    }

    public void testPrivateIpSettings() {
        List<TransportAddress> addresses = runDiscoveryTest(Settings.builder()
                .put(AzureClientSettings.HOST_TYPE_SETTING.getKey(), AzureClientSettings.HostType.PRIVATE_IP.name())
                .build(),
            VM_1);
        assertThat(addresses, hasSize(1));
        TransportAddress address = addresses.get(0);
        assertThat(address.getAddress(), is(VM_1.getPrivateIp()));
        assertThat(address.getPort(), is(9300));
    }

    public void testPublicIpSettings() {
        List<TransportAddress> addresses = runDiscoveryTest(Settings.builder()
                .put(AzureClientSettings.HOST_TYPE_SETTING.getKey(), AzureClientSettings.HostType.PUBLIC_IP.name())
                .build(),
            VM_1);
        assertThat(addresses, hasSize(1));
        TransportAddress address = addresses.get(0);
        assertThat(address.getAddress(), is(VM_1.getPublicIp()));
        assertThat(address.getPort(), is(9300));
    }

    public void testPublicIpNotBoundedSettings() {
        List<TransportAddress> addresses = runDiscoveryTest(Settings.builder()
                .put(AzureClientSettings.HOST_TYPE_SETTING.getKey(), AzureClientSettings.HostType.PUBLIC_IP.name())
                .build(),
            VM_PRIVATE_1);
        assertThat(addresses, hasSize(0));
    }

    public void testMultipleNodes() {
        List<TransportAddress> addresses = runDiscoveryTest(Settings.EMPTY, VM_1, VM_2);
        assertThat(addresses, hasSize(2));
        assertThat(addresses,
            containsInAnyOrder(Arrays.asList(
                isNode(VM_1.getPrivateIp(), 9300),
                isNode(VM_2.getPrivateIp(), 9300)
            )));
    }

    public void testPowerState() {
        List<TransportAddress> addresses = runDiscoveryTest(Settings.EMPTY, VM_PRIVATE_1, VM_DEALLOCATED, VM_STARTING, VM_STOPPED);
        assertThat(addresses, hasSize(2));
        assertThat(addresses,
            containsInAnyOrder(Arrays.asList(
                isNode(VM_PRIVATE_1.getPrivateIp(), 9300),
                isNode(VM_STARTING.getPrivateIp(), 9300)
            )));
    }

    public void testNoGroup() {
        List<TransportAddress> addresses = runDiscoveryTest(Settings.EMPTY, VM_GROUP_1_1, VM_GROUP_1_2, VM_GROUP_2_1, VM_GROUP_2_2);
        assertThat(addresses, hasSize(4));
        assertThat(addresses,
            containsInAnyOrder(Arrays.asList(
                isNode(VM_GROUP_1_1.getPrivateIp(), 9300),
                isNode(VM_GROUP_1_2.getPrivateIp(), 9300),
                isNode(VM_GROUP_2_1.getPrivateIp(), 9300),
                isNode(VM_GROUP_2_2.getPrivateIp(), 9300)
            )));
    }

    public void testGroup1() {
        List<TransportAddress> addresses = runDiscoveryTest(
            Settings.builder().put(AzureClientSettings.HOST_RESOURCE_GROUP_SETTING.getKey(), VM_GROUP_1_1.getGroupName()).build(),
            VM_GROUP_1_1, VM_GROUP_1_2, VM_GROUP_2_1, VM_GROUP_2_2);
        assertThat(addresses, hasSize(2));
        assertThat(addresses,
            containsInAnyOrder(Arrays.asList(
                isNode(VM_GROUP_1_1.getPrivateIp(), 9300),
                isNode(VM_GROUP_1_2.getPrivateIp(), 9300)
            )));
    }

    public void testGroupWithWildcard() {
        List<TransportAddress> addresses = runDiscoveryTest(
            Settings.builder().put(AzureClientSettings.HOST_RESOURCE_GROUP_SETTING.getKey(), "azure-group*").build(),
            VM_GROUP_1_1, VM_GROUP_1_2, VM_GROUP_2_1, VM_GROUP_2_2);
        assertThat(addresses, hasSize(4));
        assertThat(addresses,
            containsInAnyOrder(Arrays.asList(
                isNode(VM_GROUP_1_1.getPrivateIp(), 9300),
                isNode(VM_GROUP_1_2.getPrivateIp(), 9300),
                isNode(VM_GROUP_2_1.getPrivateIp(), 9300),
                isNode(VM_GROUP_2_2.getPrivateIp(), 9300)
            )));
    }

    public void testName() {
        List<TransportAddress> addresses = runDiscoveryTest(
            Settings.builder().put(AzureClientSettings.HOST_NAME_SETTING.getKey(), VM_GROUP_1_1.getName()).build(),
            VM_GROUP_1_1, VM_GROUP_1_2, VM_GROUP_2_1, VM_GROUP_2_2);
        assertThat(addresses, hasSize(1));
        assertThat(addresses, hasItem(isNode(VM_GROUP_1_1.getPrivateIp(), 9300)));
    }

    public void testNameWithWildcard1() {
        List<TransportAddress> addresses = runDiscoveryTest(
            Settings.builder().put(AzureClientSettings.HOST_NAME_SETTING.getKey(), "azure-mock-group1-*").build(),
            VM_GROUP_1_1, VM_GROUP_1_2, VM_GROUP_2_1, VM_GROUP_2_2);
        assertThat(addresses, hasSize(2));
        assertThat(addresses,
            containsInAnyOrder(Arrays.asList(
                isNode(VM_GROUP_1_1.getPrivateIp(), 9300),
                isNode(VM_GROUP_1_2.getPrivateIp(), 9300)
            )));
    }

    public void testNameWithWildcard2() {
        List<TransportAddress> addresses = runDiscoveryTest(
            Settings.builder().put(AzureClientSettings.HOST_NAME_SETTING.getKey(), "azure-mock-group*-1").build(),
            VM_GROUP_1_1, VM_GROUP_1_2, VM_GROUP_2_1, VM_GROUP_2_2);
        assertThat(addresses, hasSize(2));
        assertThat(addresses,
            containsInAnyOrder(Arrays.asList(
                isNode(VM_GROUP_1_1.getPrivateIp(), 9300),
                isNode(VM_GROUP_2_1.getPrivateIp(), 9300)
            )));
    }

    public void testWeird() {
        List<TransportAddress> addresses = runDiscoveryTest(Settings.EMPTY, VM_WEIRD);
        assertThat(addresses, hasSize(0));
    }

    public void testNoRegion() {
        List<TransportAddress> addresses = runDiscoveryTest(Settings.EMPTY,
            VM_REGION_EASTUS, VM_REGION_WESTUS, VM_REGION_WESTEUROPE);
        assertThat(addresses, hasSize(3));
        assertThat(addresses, containsInAnyOrder(Arrays.asList(
            isNode(VM_REGION_EASTUS.getPrivateIp(), 9300),
            isNode(VM_REGION_WESTUS.getPrivateIp(), 9300),
            isNode(VM_REGION_WESTEUROPE.getPrivateIp(), 9300)
        )));
    }

    public void testRegion() {
        List<TransportAddress> addresses = runDiscoveryTest(
            Settings.builder().put(AzureClientSettings.REGION_SETTING.getKey(), VM_REGION_EASTUS.getRegion()).build(),
            VM_REGION_EASTUS, VM_REGION_WESTUS, VM_REGION_WESTEUROPE);
        assertThat(addresses, hasSize(1));
        assertThat(addresses, hasItem(isNode(VM_REGION_EASTUS.getPrivateIp(), 9300)));
    }


    private List<TransportAddress> runDiscoveryTest(Settings settings, AzureVirtualMachine... vms) {
        for (AzureVirtualMachine vm : vms) {
            // We add the vm addresses to the Dns so it can be "resolved"
            addMachineToDns(vm.getPrivateIp());
            if (vm.getPublicIp() != null) {
                addMachineToDns(vm.getPublicIp());
            }
            addVirtualMachineToMock(vm);
        }

        return new AzureArmUnicastHostsProvider(settings, groupName -> mockVms, transportService).buildDynamicHosts(null);
    }

    private void addMachineToDns(String ip) {
        try {
            if (ip != null) {
                poorMansDNS.put(ip, new TransportAddress(InetAddress.getByName(ip), 9300));
            }
        } catch(UnknownHostException ignored) {
        }
    }

    private void addVirtualMachineToMock(AzureVirtualMachine vm) {
        mockVms.add(vm);
    }

    private Matcher<TransportAddress> isNode(final String address, final int port) {
        return new TypeSafeMatcher<TransportAddress>() {
            @Override
            protected boolean matchesSafely(TransportAddress item) {
                return item.getAddress().equals(address) &&
                    item.getPort() == port;
            }

            @Override
            public void describeTo(final Description description) {
                description.appendText("node with address ").appendValue(address).appendText(":").appendValue(port);
            }
        };
    }
}
