/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

        assertEquals("127.0.0.1:[9300-9399]", NetworkAddress.format(forge("localhost", "127.0.0.1"), new PortsRange("9300-9399")));
        assertEquals("127.0.0.1:[9300-9399]", NetworkAddress.format(forge(null, "127.0.0.1"), new PortsRange("9300-9399")));
    }

    public void testFormatPortsRangeV6() throws Exception {
        assertEquals("::1", NetworkAddress.format(forge("localhost", "::1"), new PortsRange("")));
        assertEquals("::1", NetworkAddress.format(forge(null, "::1"), new PortsRange("")));

        assertEquals("[::1]:9300", NetworkAddress.format(forge("localhost", "::1"), new PortsRange("9300")));
        assertEquals("[::1]:9300", NetworkAddress.format(forge(null, "::1"), new PortsRange("9300")));

        assertEquals("[::1]:[9300-9399]", NetworkAddress.format(forge("localhost", "::1"), new PortsRange("9300-9399")));
        assertEquals("[::1]:[9300-9399]", NetworkAddress.format(forge(null, "::1"), new PortsRange("9300-9399")));
    }

    public void testNoScopeID() throws Exception {
        assertEquals("::1", NetworkAddress.format(forgeScoped(null, "::1", 5)));
        assertEquals("::1", NetworkAddress.format(forgeScoped("localhost", "::1", 5)));
        assertEquals("::1", NetworkAddress.format(forgeScoped("localhost", "::1", 5), new PortsRange("")));

        assertEquals("[::1]:1234", NetworkAddress.format(new InetSocketAddress(forgeScoped(null, "::1", 5), 1234)));
        assertEquals("[::1]:1234", NetworkAddress.format(new InetSocketAddress(forgeScoped("localhost", "::1", 5), 1234)));
        assertEquals("[::1]:[9300-9399]", NetworkAddress.format(forgeScoped("localhost", "::1", 5), new PortsRange("9300-9399")));
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
