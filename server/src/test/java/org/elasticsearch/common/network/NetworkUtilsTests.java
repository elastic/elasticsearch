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

package org.elasticsearch.common.network;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.hamcrest.OptionalMatchers;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.List;
import java.util.Optional;

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
        final List<NetworkInterface> networkInterfaces = NetworkUtils.getInterfaces();
        for (NetworkInterface netIf : networkInterfaces) {
            final Optional<NetworkInterface> maybeNetworkInterface =
                NetworkUtils.maybeGetInterfaceByName(networkInterfaces, netIf.getName());
            assertThat(maybeNetworkInterface, OptionalMatchers.isPresent());
            assertThat(maybeNetworkInterface.get().getName(), equalTo(netIf.getName()));
        }
    }

    public void testNonExistingInterface() throws Exception {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> NetworkUtils.getAddressesForInterface("non-existing"));
        assertThat(exception.getMessage(), containsString("No interface named 'non-existing' found"));
    }
}
