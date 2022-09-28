/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.azure.arm;

import com.azure.resourcemanager.compute.models.PowerState;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class AzureUnicastHostsProviderTests extends ESTestCase {

    private static ThreadPool threadPool;

    private static final AzureVirtualMachine VM_PRIVATE_1 = AzureVirtualMachine.builder()
        .setName("azure-mock-private-1")
        .setPowerState(PowerState.RUNNING)
        .setPrivateIp("10.0.0.238")
        .setGroupName("azure-group")
        .setRegion("westeurope")
        .build();
    private static final AzureVirtualMachine VM_1 = AzureVirtualMachine.builder()
        .setName("azure-mock-1")
        .setPowerState(PowerState.RUNNING)
        .setPrivateIp("10.0.0.238")
        .setPublicIp("8.8.8.8")
        .setGroupName("azure-group")
        .setRegion("westeurope")
        .build();
    private static final AzureVirtualMachine VM_2 = AzureVirtualMachine.builder()
        .setName("azure-mock-2")
        .setPowerState(PowerState.RUNNING)
        .setPrivateIp("10.0.0.239")
        .setPublicIp("8.8.8.9")
        .setGroupName("azure-group")
        .setRegion("westeurope")
        .build();
    private static final AzureVirtualMachine VM_STARTING = AzureVirtualMachine.builder()
        .setName("azure-mock-starting-1")
        .setPowerState(PowerState.STARTING)
        .setPrivateIp("10.0.0.100")
        .setGroupName("azure-group")
        .setRegion("westeurope")
        .build();
    private static final AzureVirtualMachine VM_STOPPED = AzureVirtualMachine.builder()
        .setName("azure-mock-stopped-1")
        .setPowerState(PowerState.STOPPED)
        .setPrivateIp("10.0.0.101")
        .setGroupName("azure-group")
        .setRegion("westeurope")
        .build();
    private static final AzureVirtualMachine VM_DEALLOCATED = AzureVirtualMachine.builder()
        .setName("azure-mock-deallocated-1")
        .setPowerState(PowerState.DEALLOCATED)
        .setPrivateIp("10.0.0.102")
        .setGroupName("azure-group")
        .setRegion("westeurope")
        .build();
    private static final AzureVirtualMachine VM_GROUP_1_1 = AzureVirtualMachine.builder()
        .setName("azure-mock-group1-1")
        .setPowerState(PowerState.RUNNING)
        .setPrivateIp("10.0.0.150")
        .setGroupName("azure-group1")
        .setRegion("westeurope")
        .build();
    private static final AzureVirtualMachine VM_GROUP_1_2 = AzureVirtualMachine.builder()
        .setName("azure-mock-group1-2")
        .setPowerState(PowerState.RUNNING)
        .setPrivateIp("10.0.0.151")
        .setGroupName("azure-group1")
        .setRegion("westeurope")
        .build();
    private static final AzureVirtualMachine VM_GROUP_2_1 = AzureVirtualMachine.builder()
        .setName("azure-mock-group2-1")
        .setPowerState(PowerState.RUNNING)
        .setPrivateIp("10.0.0.160")
        .setGroupName("azure-group2")
        .setRegion("westeurope")
        .build();
    private static final AzureVirtualMachine VM_GROUP_2_2 = AzureVirtualMachine.builder()
        .setName("azure-mock-group2-2")
        .setPowerState(PowerState.RUNNING)
        .setPrivateIp("10.0.0.161")
        .setGroupName("azure-group2")
        .setRegion("westeurope")
        .build();
    /**
     * This is a weird instance which is running but has absolutely no IP address associated.
     * This can not happen in real life, but let's be paranoid and test that.
     */
    private static final AzureVirtualMachine VM_WEIRD = AzureVirtualMachine.builder()
        .setName("azure-mock-weird")
        .setPowerState(PowerState.RUNNING)
        .setGroupName("azure-group")
        .setRegion("westeurope")
        .build();
    private static final AzureVirtualMachine VM_REGION_EASTUS = AzureVirtualMachine.builder()
        .setName("azure-mock-eastus")
        .setPowerState(PowerState.RUNNING)
        .setPrivateIp("10.0.0.50")
        .setGroupName("azure-group")
        .setRegion("eastus")
        .build();
    private static final AzureVirtualMachine VM_REGION_WESTUS = AzureVirtualMachine.builder()
        .setName("azure-mock-westus")
        .setPowerState(PowerState.RUNNING)
        .setPrivateIp("10.0.1.50")
        .setGroupName("azure-group")
        .setRegion("westus")
        .build();
    private static final AzureVirtualMachine VM_REGION_WESTEUROPE = AzureVirtualMachine.builder()
        .setName("azure-mock-westeurope")
        .setPowerState(PowerState.RUNNING)
        .setPrivateIp("10.0.2.50")
        .setGroupName("azure-group")
        .setRegion("westeurope")
        .build();

    private final AzureManagementService azureManagementService = Mockito.mock(AzureManagementService.class);
    private final TransportService transportService = Mockito.mock(TransportService.class);

    @Before
    public void setUpTransportService() throws Exception {
        when(transportService.addressesFromString(anyString())).thenAnswer(new Answer<TransportAddress[]>() {
            @Override
            public TransportAddress[] answer(InvocationOnMock invocation) throws Throwable {
                return new TransportAddress[] { new TransportAddress(InetAddress.getByName(invocation.getArgument(0)), 9300) };
            }
        });
    }

    @BeforeClass
    public static void createThreadPool() {
        threadPool = new TestThreadPool(AzureUnicastHostsProviderTests.class.getName());
    }

    @AfterClass
    public static void stopThreadPool() {
        terminate(threadPool);
    }

    private AzureArmSeedHostsProvider azureArmSeedHostsProvider(Settings settings) {
        return new AzureArmSeedHostsProvider(settings, azureManagementService, transportService);
    }

    public void testDefaultSettings() throws UnknownHostException {
        when(azureManagementService.getVirtualMachines("")).thenReturn(List.of(VM_1));

        List<TransportAddress> addresses = getSeedAddresses(Settings.EMPTY);

        assertThat(addresses, hasSize(1));
        assertThat(addresses.get(0).getAddress(), is(VM_1.privateIp()));
        assertThat(addresses.get(0).getPort(), is(9300));
    }

    public void testPrivateIpSettings() throws UnknownHostException {
        when(azureManagementService.getVirtualMachines("")).thenReturn(List.of(VM_1));

        List<TransportAddress> addresses = getSeedAddresses(
            Settings.builder().put(AzureClientSettings.HOST_TYPE_SETTING.getKey(), AzureClientSettings.HostType.PRIVATE_IP.name()).build()
        );

        assertThat(addresses, hasSize(1));
        assertThat(addresses.get(0).getAddress(), is(VM_1.privateIp()));
        assertThat(addresses.get(0).getPort(), is(9300));
    }

    public void testPublicIpSettings() throws UnknownHostException {
        when(azureManagementService.getVirtualMachines("")).thenReturn(List.of(VM_1));

        List<TransportAddress> addresses = getSeedAddresses(
            Settings.builder().put(AzureClientSettings.HOST_TYPE_SETTING.getKey(), AzureClientSettings.HostType.PUBLIC_IP.name()).build()
        );

        assertThat(addresses, hasSize(1));
        assertThat(addresses.get(0).getAddress(), is(VM_1.publicIp()));
        assertThat(addresses.get(0).getPort(), is(9300));
    }

    public void testPublicIpNotBoundedSettings() {
        when(azureManagementService.getVirtualMachines("")).thenReturn(List.of(VM_PRIVATE_1));

        List<TransportAddress> addresses = getSeedAddresses(
            Settings.builder().put(AzureClientSettings.HOST_TYPE_SETTING.getKey(), AzureClientSettings.HostType.PUBLIC_IP.name()).build()
        );
        assertThat(addresses, hasSize(0));
    }

    public void testMultipleNodes() throws UnknownHostException {
        when(azureManagementService.getVirtualMachines("")).thenReturn(List.of(VM_1, VM_2));

        List<TransportAddress> addresses = getSeedAddresses(Settings.EMPTY);
        assertThat(addresses, hasSize(2));
        assertThat(addresses, containsInAnyOrder(Arrays.asList(isNode(VM_1.privateIp(), 9300), isNode(VM_2.privateIp(), 9300))));
    }

    public void testPowerState() throws UnknownHostException {
        when(azureManagementService.getVirtualMachines("")).thenReturn(List.of(VM_PRIVATE_1, VM_DEALLOCATED, VM_STARTING, VM_STOPPED));

        List<TransportAddress> addresses = getSeedAddresses(Settings.EMPTY);

        assertThat(addresses, hasSize(2));
        assertThat(
            addresses,
            containsInAnyOrder(Arrays.asList(isNode(VM_PRIVATE_1.privateIp(), 9300), isNode(VM_STARTING.privateIp(), 9300)))
        );
    }

    public void testNoGroup() {
        when(azureManagementService.getVirtualMachines("")).thenReturn(List.of(VM_GROUP_1_1, VM_GROUP_1_2, VM_GROUP_2_1, VM_GROUP_2_2));

        List<TransportAddress> addresses = getSeedAddresses(Settings.EMPTY);

        assertThat(addresses, hasSize(4));
        assertThat(
            addresses,
            containsInAnyOrder(
                Arrays.asList(
                    isNode(VM_GROUP_1_1.privateIp(), 9300),
                    isNode(VM_GROUP_1_2.privateIp(), 9300),
                    isNode(VM_GROUP_2_1.privateIp(), 9300),
                    isNode(VM_GROUP_2_2.privateIp(), 9300)
                )
            )
        );
    }

    public void testGroup1() {
        when(azureManagementService.getVirtualMachines(VM_GROUP_1_1.groupName())).thenReturn(List.of(VM_GROUP_1_1, VM_GROUP_1_2));

        List<TransportAddress> addresses = getSeedAddresses(
            Settings.builder().put(AzureClientSettings.HOST_RESOURCE_GROUP_SETTING.getKey(), VM_GROUP_1_1.groupName()).build()
        );

        assertThat(addresses, hasSize(2));
        assertThat(addresses, containsInAnyOrder(List.of(isNode(VM_GROUP_1_1.privateIp(), 9300), isNode(VM_GROUP_1_2.privateIp(), 9300))));
    }

    public void testGroupsetWildcard() {
        when(azureManagementService.getVirtualMachines("")).thenReturn(List.of(VM_GROUP_1_1, VM_GROUP_1_2, VM_GROUP_2_1, VM_GROUP_2_2));

        List<TransportAddress> addresses = getSeedAddresses(
            Settings.builder().put(AzureClientSettings.HOST_RESOURCE_GROUP_SETTING.getKey(), "azure-group*").build()
        );

        assertThat(addresses, hasSize(4));
        assertThat(
            addresses,
            containsInAnyOrder(
                Arrays.asList(
                    isNode(VM_GROUP_1_1.privateIp(), 9300),
                    isNode(VM_GROUP_1_2.privateIp(), 9300),
                    isNode(VM_GROUP_2_1.privateIp(), 9300),
                    isNode(VM_GROUP_2_2.privateIp(), 9300)
                )
            )
        );
    }

    public void testName() {
        when(azureManagementService.getVirtualMachines("")).thenReturn(List.of(VM_GROUP_1_1, VM_GROUP_1_2, VM_GROUP_2_1, VM_GROUP_2_2));

        List<TransportAddress> addresses = getSeedAddresses(
            Settings.builder().put(AzureClientSettings.HOST_NAME_SETTING.getKey(), VM_GROUP_1_1.name()).build()
        );

        assertThat(addresses, hasSize(1));
        assertThat(addresses, hasItem(isNode(VM_GROUP_1_1.privateIp(), 9300)));
    }

    public void testNamesetWildcard1() {
        when(azureManagementService.getVirtualMachines("")).thenReturn(List.of(VM_GROUP_1_1, VM_GROUP_1_2, VM_GROUP_2_1, VM_GROUP_2_2));

        List<TransportAddress> addresses = getSeedAddresses(
            Settings.builder().put(AzureClientSettings.HOST_NAME_SETTING.getKey(), "azure-mock-group1-*").build()
        );

        assertThat(addresses, hasSize(2));
        assertThat(
            addresses,
            containsInAnyOrder(Arrays.asList(isNode(VM_GROUP_1_1.privateIp(), 9300), isNode(VM_GROUP_1_2.privateIp(), 9300)))
        );
    }

    public void testNamesetWildcard2() {
        when(azureManagementService.getVirtualMachines("")).thenReturn(List.of(VM_GROUP_1_1, VM_GROUP_1_2, VM_GROUP_2_1, VM_GROUP_2_2));

        List<TransportAddress> addresses = getSeedAddresses(
            Settings.builder().put(AzureClientSettings.HOST_NAME_SETTING.getKey(), "azure-mock-group*-1").build()
        );
        assertThat(addresses, hasSize(2));
        assertThat(
            addresses,
            containsInAnyOrder(Arrays.asList(isNode(VM_GROUP_1_1.privateIp(), 9300), isNode(VM_GROUP_2_1.privateIp(), 9300)))
        );
    }

    public void testWeird() {
        when(azureManagementService.getVirtualMachines("")).thenReturn(List.of(VM_WEIRD));

        List<TransportAddress> addresses = getSeedAddresses(Settings.EMPTY);

        assertThat(addresses, hasSize(0));
    }

    public void testNoRegion() {
        when(azureManagementService.getVirtualMachines("")).thenReturn(List.of(VM_REGION_EASTUS, VM_REGION_WESTUS, VM_REGION_WESTEUROPE));

        List<TransportAddress> addresses = getSeedAddresses(Settings.EMPTY);

        assertThat(addresses, hasSize(3));
        assertThat(
            addresses,
            containsInAnyOrder(
                Arrays.asList(
                    isNode(VM_REGION_EASTUS.privateIp(), 9300),
                    isNode(VM_REGION_WESTUS.privateIp(), 9300),
                    isNode(VM_REGION_WESTEUROPE.privateIp(), 9300)
                )
            )
        );
    }

    public void testRegion() {
        when(azureManagementService.getVirtualMachines("")).thenReturn(List.of(VM_REGION_EASTUS, VM_REGION_WESTUS, VM_REGION_WESTEUROPE));

        List<TransportAddress> addresses = getSeedAddresses(
            Settings.builder().put(AzureClientSettings.REGION_SETTING.getKey(), VM_REGION_EASTUS.region()).build()
        );
        assertThat(addresses, hasSize(1));
        assertThat(addresses, hasItem(isNode(VM_REGION_EASTUS.privateIp(), 9300)));
    }

    private List<TransportAddress> getSeedAddresses(Settings settings) {
        return new AzureArmSeedHostsProvider(settings, azureManagementService, transportService).getSeedAddresses(null);
    }

    private Matcher<TransportAddress> isNode(String address, int port) {
        return new TypeSafeMatcher<>() {
            @Override
            protected boolean matchesSafely(TransportAddress item) {
                return item.getAddress().equals(address) && item.getPort() == port;
            }

            @Override
            public void describeTo(final Description description) {
                description.appendText("node set address ").appendValue(address).appendText(":").appendValue(port);
            }
        };
    }
}
