/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.network;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.hamcrest.OptionalMatchers;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.common.network.NetworkUtils.getInterfaces;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for network utils. Please avoid using any methods that cause DNS lookups!
 */
public class NetworkUtilsTests extends ESTestCase {

    /**
     * test sort key order respects PREFER_IPV4
     */
    public void testSortKey() throws Exception {
        InetAddress localhostv4 = InetAddress.getByName("127.0.0.1");
        InetAddress localhostv6 = InetAddress.getByName("::1");
        assertTrue(NetworkUtils.sortKey(localhostv4, false) < NetworkUtils.sortKey(localhostv6, false));
        assertTrue(NetworkUtils.sortKey(localhostv6, true) < NetworkUtils.sortKey(localhostv4, true));
    }

    /**
     * test ordinary addresses sort before private addresses
     */
    public void testSortKeySiteLocal() throws Exception {
        InetAddress siteLocal = InetAddress.getByName("172.16.0.1");
        assert siteLocal.isSiteLocalAddress();
        InetAddress ordinary = InetAddress.getByName("192.192.192.192");
        assertTrue(NetworkUtils.sortKey(ordinary, true) < NetworkUtils.sortKey(siteLocal, true));
        assertTrue(NetworkUtils.sortKey(ordinary, false) < NetworkUtils.sortKey(siteLocal, false));

        InetAddress siteLocal6 = InetAddress.getByName("fec0::1");
        assert siteLocal6.isSiteLocalAddress();
        InetAddress ordinary6 = InetAddress.getByName("fddd::1");
        assertTrue(NetworkUtils.sortKey(ordinary6, true) < NetworkUtils.sortKey(siteLocal6, true));
        assertTrue(NetworkUtils.sortKey(ordinary6, false) < NetworkUtils.sortKey(siteLocal6, false));
    }

    /**
     * test private addresses sort before link local addresses
     */
    public void testSortKeyLinkLocal() throws Exception {
        InetAddress linkLocal = InetAddress.getByName("fe80::1");
        assert linkLocal.isLinkLocalAddress();
        InetAddress ordinary = InetAddress.getByName("fddd::1");
        assertTrue(NetworkUtils.sortKey(ordinary, true) < NetworkUtils.sortKey(linkLocal, true));
        assertTrue(NetworkUtils.sortKey(ordinary, false) < NetworkUtils.sortKey(linkLocal, false));
    }

    /**
     * Test filtering out ipv4/ipv6 addresses
     */
    public void testFilter() throws Exception {
        InetAddress addresses[] = { InetAddress.getByName("::1"), InetAddress.getByName("127.0.0.1") };
        assertArrayEquals(new InetAddress[] { InetAddress.getByName("127.0.0.1") }, NetworkUtils.filterIPV4(addresses));
        assertArrayEquals(new InetAddress[] { InetAddress.getByName("::1") }, NetworkUtils.filterIPV6(addresses));
    }

    // test that selecting by name is possible
    public void testMaybeGetInterfaceByName() throws Exception {
        final List<NetworkInterface> networkInterfaces = getInterfaces();
        for (NetworkInterface netIf : networkInterfaces) {
            final Optional<NetworkInterface> maybeNetworkInterface =
                NetworkUtils.maybeGetInterfaceByName(networkInterfaces, netIf.getName());
            assertThat(maybeNetworkInterface, OptionalMatchers.isPresent());
            assertThat(maybeNetworkInterface.get().getName(), equalTo(netIf.getName()));
        }
    }

    public void testNonExistingInterface() throws Exception {
        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> NetworkUtils.getAddressesForInterface("settingValue", ":suffix", "non-existing"));
        assertThat(exception.getMessage(), containsString("setting [settingValue] matched no network interfaces; valid values include"));
        final boolean atLeastOneInterfaceIsPresentInExceptionMessage = getInterfaces().stream()
            .anyMatch(anInterface -> exception.getMessage().contains(anInterface.getName() + ":suffix"));

        assertThat("Expected to get at least one interface name in the exception but got none: " + exception.getMessage(),
            atLeastOneInterfaceIsPresentInExceptionMessage,
            equalTo(true)
        );
    }
}
