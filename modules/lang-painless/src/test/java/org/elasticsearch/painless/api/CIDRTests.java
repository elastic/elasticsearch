/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.api;

import org.elasticsearch.test.ESTestCase;

public class CIDRTests extends ESTestCase {
    public void testCIDR() {
        CIDR cidr = new CIDR("192.168.8.0/24");
        assertTrue(cidr.contains("192.168.8.0"));
        assertTrue(cidr.contains("192.168.8.255"));
        assertFalse(cidr.contains("192.168.9.0"));
        assertFalse(cidr.contains("192.168.7.255"));
        assertFalse(cidr.contains(null));
        assertFalse(cidr.contains(""));

        CIDR cidrNoRange = new CIDR("169.254.0.0");
        assertTrue(cidrNoRange.contains("169.254.0.0"));
        assertFalse(cidrNoRange.contains("169.254.0.1"));

        CIDR cidrIPv6 = new CIDR("b181:3a88:339c:97f5:2b40:5175:bf3d:f77d/64");
        assertTrue(cidrIPv6.contains("b181:3a88:339c:97f5:2b40:5175:bf3d:f77e"));
        assertFalse(cidrIPv6.contains("254.120.25.32"));

        expectThrows(IllegalArgumentException.class, () -> new CIDR("10.2.0.0/36"));
    }
}
