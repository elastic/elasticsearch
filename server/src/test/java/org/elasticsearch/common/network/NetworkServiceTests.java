/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.network;

import org.elasticsearch.test.ESTestCase;

import java.net.InetAddress;
import java.util.Collections;

import static org.hamcrest.Matchers.is;

/**
 * Tests for network service... try to keep them safe depending upon configuration
 * please don't actually bind to anything, just test the addresses.
 */
public class NetworkServiceTests extends ESTestCase {

    /**
     * ensure exception if we bind to multicast ipv4 address
     */
    public void testBindMulticastV4() throws Exception {
        NetworkService service = new NetworkService(Collections.emptyList());
        try {
            service.resolveBindHostAddresses(new String[] { "239.1.1.1" });
            fail("should have hit exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("invalid: multicast"));
        }
    }
    /**
     * ensure exception if we bind to multicast ipv6 address
     */
    public void testBindMulticastV6() throws Exception {
        NetworkService service = new NetworkService(Collections.emptyList());
        try {
            service.resolveBindHostAddresses(new String[] { "FF08::108" });
            fail("should have hit exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("invalid: multicast"));
        }
    }

    /**
     * ensure exception if we publish to multicast ipv4 address
     */
    public void testPublishMulticastV4() throws Exception {
        NetworkService service = new NetworkService(Collections.emptyList());
        try {
            service.resolvePublishHostAddresses(new String[] { "239.1.1.1" });
            fail("should have hit exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("invalid: multicast"));
        }
    }

    /**
     * ensure exception if we publish to multicast ipv6 address
     */
    public void testPublishMulticastV6() throws Exception {
        NetworkService service = new NetworkService(Collections.emptyList());
        try {
            service.resolvePublishHostAddresses(new String[] { "FF08::108" });
            fail("should have hit exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("invalid: multicast"));
        }
    }

    /**
     * ensure specifying wildcard ipv4 address will bind to all interfaces
     */
    public void testBindAnyLocalV4() throws Exception {
        NetworkService service = new NetworkService(Collections.emptyList());
        assertEquals(InetAddress.getByName("0.0.0.0"), service.resolveBindHostAddresses(new String[] { "0.0.0.0" }
        )[0]);
    }

    /**
     * ensure specifying wildcard ipv6 address will bind to all interfaces
     */
    public void testBindAnyLocalV6() throws Exception {
        NetworkService service = new NetworkService(Collections.emptyList());
        assertEquals(InetAddress.getByName("::"), service.resolveBindHostAddresses(new String[] { "::" })[0]);
    }

    /**
     * ensure specifying wildcard ipv4 address selects reasonable publish address
     */
    public void testPublishAnyLocalV4() throws Exception {
        NetworkService service = new NetworkService(Collections.emptyList());
        InetAddress address = service.resolvePublishHostAddresses(new String[] { "0.0.0.0" });
        assertFalse(address.isAnyLocalAddress());
    }

    /**
     * ensure specifying wildcard ipv6 address selects reasonable publish address
     */
    public void testPublishAnyLocalV6() throws Exception {
        NetworkService service = new NetworkService(Collections.emptyList());
        InetAddress address = service.resolvePublishHostAddresses(new String[] { "::" });
        assertFalse(address.isAnyLocalAddress());
    }

    /**
     * ensure we can bind to multiple addresses
     */
    public void testBindMultipleAddresses() throws Exception {
        NetworkService service = new NetworkService(Collections.emptyList());
        InetAddress[] addresses = service.resolveBindHostAddresses(new String[]{"127.0.0.1", "127.0.0.2"});
        assertThat(addresses.length, is(2));
    }

    /**
     * ensure we can't bind to multiple addresses when using wildcard
     */
    public void testBindMultipleAddressesWithWildcard() throws Exception {
        NetworkService service = new NetworkService(Collections.emptyList());
        try {
            service.resolveBindHostAddresses(new String[]{"0.0.0.0", "127.0.0.1"});
            fail("should have hit exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("is wildcard, but multiple addresses specified"));
        }
    }
}
