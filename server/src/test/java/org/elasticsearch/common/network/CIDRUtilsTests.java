/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.network;

import org.elasticsearch.test.ESTestCase;

public class CIDRUtilsTests extends ESTestCase {

    public void testCIDRUtils() {
        // Missing or empty param
        assertFalse(CIDRUtils.isInRange("10.6.48.157"));
        assertFalse(CIDRUtils.isInRange("10.6.48.157", (String) null));

        // EQL tests matches
        assertTrue(CIDRUtils.isInRange("10.6.48.157", "10.6.48.157/8"));
        assertFalse(CIDRUtils.isInRange("10.6.48.157", "192.168.0.0/16"));
        assertTrue(CIDRUtils.isInRange("10.6.48.157", "192.168.0.0/16", "10.6.48.157/8"));
        assertTrue(CIDRUtils.isInRange("10.6.48.157", "0.0.0.0/0"));
        assertFalse(CIDRUtils.isInRange("10.6.48.157", "0.0.0.0"));

        // Random things
        assertTrue(CIDRUtils.isInRange("192.168.2.1", "192.168.2.1"));
        assertFalse(CIDRUtils.isInRange("192.168.2.1", "192.168.2.0/32"));
        assertTrue(CIDRUtils.isInRange("192.168.2.5", "192.168.2.0/24"));
        assertFalse(CIDRUtils.isInRange("92.168.2.1", "fe80:0:0:0:0:0:c0a8:1/120"));
        assertTrue(CIDRUtils.isInRange("192.168.2.5", "192.168.2.0/24"));

        assertTrue(CIDRUtils.isInRange("fe80:0:0:0:0:0:c0a8:11", "fe80:0:0:0:0:0:c0a8:1/120"));
        assertFalse(CIDRUtils.isInRange("fe80:0:0:0:0:0:c0a8:11", "fe80:0:0:0:0:0:c0a8:1/128"));
        assertFalse(CIDRUtils.isInRange("fe80:0:0:0:0:0:c0a8:11", "192.168.2.0/32"));

        assertTrue(CIDRUtils.isInRange("2001:db8:3c0d:5b6d:0:0:42:8329", "2001:db8:3c0d:5b6d:0:0:42:8329/58"));
        assertTrue(CIDRUtils.isInRange("2001:db8:3c0d:5b40::", "2001:db8:3c0d:5b6d:0:0:42:8329/58"));
        assertTrue(CIDRUtils.isInRange("2001:db8:3c0d:5b7f:ffff:ffff:ffff:ffff", "2001:db8:3c0d:5b6d:0:0:42:8329/58"));
        assertTrue(CIDRUtils.isInRange("2001:db8:3c0d:5b53:0:0:0:1", "2001:db8:3c0d:5b6d:0:0:42:8329/58"));
        assertFalse(CIDRUtils.isInRange("2001:db8:3c0d:5b3f:ffff:ffff:ffff:ffff", "2001:db8:3c0d:5b6d:0:0:42:8329/58"));
        assertFalse(CIDRUtils.isInRange("2001:db8:3c0d:5b80::", "2001:db8:3c0d:5b6d:0:0:42:8329/58"));

        assertTrue(CIDRUtils.isInRange("2001:db8:3c0d:5b6d:0:0:42:8329", "2001:db8:3c0d:5b6d:0:0:42:8329/128"));

        assertFalse(CIDRUtils.isInRange("2001:db8:3c0d:5b6d:0:0:42:8329", "192.168.2.0/32"));

        assertTrue(CIDRUtils.isInRange("127.0.0.1", "127.0.0.1/8"));

        assertTrue(CIDRUtils.isInRange("127.0.0.1", "::1/64"));
    }
}
