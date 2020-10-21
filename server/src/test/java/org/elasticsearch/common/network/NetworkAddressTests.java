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

import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Random;

/**
 * Tests for network address formatting. Please avoid using any methods that cause DNS lookups!
 */
public class NetworkAddressTests extends ESTestCase {

    public void testFormatV4() throws Exception {
        assertEquals("127.0.0.1", NetworkAddress.format(forge("localhost", "127.0.0.1")));
        assertEquals("127.0.0.1", NetworkAddress.format(forge(null, "127.0.0.1")));
    }

    public void testFormatV6() throws Exception {
        assertEquals("::1", NetworkAddress.format(forge("localhost", "::1")));
        assertEquals("::1", NetworkAddress.format(forge(null, "::1")));
    }

    public void testFormatPortV4() throws Exception {
        assertEquals("127.0.0.1:1234", NetworkAddress.format(new InetSocketAddress(forge("localhost", "127.0.0.1"), 1234)));
        assertEquals("127.0.0.1:1234", NetworkAddress.format(new InetSocketAddress(forge(null, "127.0.0.1"), 1234)));
    }

    public void testFormatPortV6() throws Exception {
        assertEquals("[::1]:1234", NetworkAddress.format(new InetSocketAddress(forge("localhost", "::1"), 1234)));
        assertEquals("[::1]:1234", NetworkAddress.format(new InetSocketAddress(forge(null, "::1"), 1234)));
    }

    public void testFormatPortsRangeV4() throws Exception {
        assertEquals("127.0.0.1", NetworkAddress.format(forge("localhost", "127.0.0.1"), new PortsRange("")));
        assertEquals("127.0.0.1", NetworkAddress.format(forge(null, "127.0.0.1"), new PortsRange("")));

        assertEquals("127.0.0.1:9300", NetworkAddress.format(forge("localhost", "127.0.0.1"), new PortsRange("9300")));
        assertEquals("127.0.0.1:9300", NetworkAddress.format(forge(null, "127.0.0.1"), new PortsRange("9300")));

        assertEquals("127.0.0.1:[9300-9400]", NetworkAddress.format(forge("localhost", "127.0.0.1"), new PortsRange("9300-9400")));
        assertEquals("127.0.0.1:[9300-9400]", NetworkAddress.format(forge(null, "127.0.0.1"), new PortsRange("9300-9400")));
    }

    public void testFormatPortsRangeV6() throws Exception {
        assertEquals("::1", NetworkAddress.format(forge("localhost", "::1"), new PortsRange("")));
        assertEquals("::1", NetworkAddress.format(forge(null, "::1"), new PortsRange("")));

        assertEquals("[::1]:9300", NetworkAddress.format(forge("localhost", "::1"), new PortsRange("9300")));
        assertEquals("[::1]:9300", NetworkAddress.format(forge(null, "::1"), new PortsRange("9300")));

        assertEquals("[::1]:[9300-9400]", NetworkAddress.format(forge("localhost", "::1"), new PortsRange("9300-9400")));
        assertEquals("[::1]:[9300-9400]", NetworkAddress.format(forge(null, "::1"), new PortsRange("9300-9400")));
    }

    public void testNoScopeID() throws Exception {
        assertEquals("::1", NetworkAddress.format(forgeScoped(null, "::1", 5)));
        assertEquals("::1", NetworkAddress.format(forgeScoped("localhost", "::1", 5)));
        assertEquals("::1", NetworkAddress.format(forgeScoped("localhost", "::1", 5), new PortsRange("")));

        assertEquals("[::1]:1234", NetworkAddress.format(new InetSocketAddress(forgeScoped(null, "::1", 5), 1234)));
        assertEquals("[::1]:1234", NetworkAddress.format(new InetSocketAddress(forgeScoped("localhost", "::1", 5), 1234)));
        assertEquals("[::1]:[9300-9400]", NetworkAddress.format(forgeScoped("localhost", "::1", 5), new PortsRange("9300-9400")));
    }

    /** Test that ipv4 address formatting round trips */
    public void testRoundTripV4() throws Exception {
        roundTrip(new byte[4]);
    }

    /** Test that ipv6 address formatting round trips */
    public void testRoundTripV6() throws Exception {
        roundTrip(new byte[16]);
    }

    /**
     * Round trip test code for both IPv4 and IPv6. {@link InetAddress} contains the {@code getByAddress} and
     * {@code getbyName} methods for both IPv4 and IPv6, unless you also specify a {@code scopeid}, which this does not
     * test.
     *
     * @param bytes 4 (32-bit for IPv4) or 16 bytes (128-bit for IPv6)
     * @throws Exception if any error occurs while interacting with the network address
     */
    private void roundTrip(byte[] bytes) throws Exception {
        Random random = random();
        for (int i = 0; i < 10000; i++) {
            random.nextBytes(bytes);
            InetAddress expected = InetAddress.getByAddress(bytes);
            String formatted = NetworkAddress.format(expected);
            InetAddress actual = InetAddress.getByName(formatted);
            assertEquals(expected, actual);
        }
    }

    /** creates address without any lookups. hostname can be null, for missing */
    private InetAddress forge(String hostname, String address) throws IOException {
        byte bytes[] = InetAddress.getByName(address).getAddress();
        return InetAddress.getByAddress(hostname, bytes);
    }

    /** creates scoped ipv6 address without any lookups. hostname can be null, for missing */
    private InetAddress forgeScoped(String hostname, String address, int scopeid) throws IOException {
        byte bytes[] = InetAddress.getByName(address).getAddress();
        return Inet6Address.getByAddress(hostname, bytes, scopeid);
    }

}
