/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.test.ESTestCase;

public class IPAddressTests extends ESTestCase {

    public void testToString() {
        String v4 = "192.168.7.255";
        assertEquals(v4, new IPAddress(v4).toString());
        String v6 = "b181:3a88:339c:97f5:2b40:5175:bf3d:f77e";
        assertEquals(v6, new IPAddress(v6).toString());
    }

    public void testV4() {
        IPAddress addr4 = new IPAddress("169.254.0.0");
        assertTrue(addr4.isV4());
        assertFalse(addr4.isV6());
    }

    public void testV6() {
        IPAddress addr4 = new IPAddress("b181:3a88:339c:97f5:2b40:5175:bf3d:f77e");
        assertFalse(addr4.isV4());
        assertTrue(addr4.isV6());
    }
}
