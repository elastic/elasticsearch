/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

public class CidrTests extends ScriptTestCase {
    public void testContains() {
        Object bool = exec("CIDR c = new CIDR('10.1.1.0/23'); c.contains('10.1.1.128') && c.contains('10.1.0.255')");
        assertEquals(Boolean.TRUE, bool);

        bool = exec("CIDR c = new CIDR('10.1.1.0/25'); c.contains('10.1.1.127')");
        assertEquals(Boolean.TRUE, bool);

        bool = exec("CIDR c = new CIDR('10.1.1.0/25'); c.contains('10.1.1.129')");
        assertEquals(Boolean.FALSE, bool);

        bool = exec("new CIDR('192.168.3.5').contains('192.168.3.5')");
        assertEquals(Boolean.TRUE, bool);

        bool = exec("new CIDR('192.168.3.5').contains('')");
        assertEquals(Boolean.FALSE, bool);

        bool = exec("new CIDR('2001:0db8:85a3::/64').contains('2001:0db8:85a3:0000:0000:8a2e:0370:7334')");
        assertEquals(Boolean.TRUE, bool);

        bool = exec("new CIDR('2001:0db8:85a3::/64').contains('2001:0db8:85a3:0001:0000:8a2e:0370:7334')");
        assertEquals(Boolean.FALSE, bool);

    }

    public void testInvalidIPs() {
        IllegalArgumentException e = expectScriptThrows(IllegalArgumentException.class, () -> exec("new CIDR('abc')"));
        assertEquals("'abc' is not an IP string literal.", e.getMessage());

        e = expectScriptThrows(IllegalArgumentException.class, () -> exec("new CIDR('10.257.3.5')"));
        assertEquals("'10.257.3.5' is not an IP string literal.", e.getMessage());

        e = expectScriptThrows(IllegalArgumentException.class, () -> exec("new CIDR('2001:0db8:85a3:0000:0000:8a2e:0370:733g')"));
        assertEquals("'2001:0db8:85a3:0000:0000:8a2e:0370:733g' is not an IP string literal.", e.getMessage());

        e = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec("new CIDR('2001:0db8:85a3::/64').contains('2001:0db8:85a3:0000:0000:8a2g:0370:7334')")
        );
        assertEquals("'2001:0db8:85a3:0000:0000:8a2g:0370:7334' is not an IP string literal.", e.getMessage());
    }
}
