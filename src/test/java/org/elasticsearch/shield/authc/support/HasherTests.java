/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 *
 */
public class HasherTests {

    @Test
    public void testHtpasswdToolGenerated() throws Exception {
        Hasher hasher = Hasher.HTPASSWD;
        char[] passwd = "test123".toCharArray();
        assertTrue(hasher.verify(passwd, "$2a$05$zxnP0vdREMxnEpkLCDI2OuSaSk/QEKA2.A42iOpI6U2u.RLLOWm1e".toCharArray()));
        assertTrue(hasher.verify(passwd, "$2a$10$FMhmFjwU5.qxQ/BsEciS9OqcJVkFMgXMo4uH5CelOR1j4N9zIv67e".toCharArray()));
        assertTrue(hasher.verify(passwd, "$apr1$R3DdqiAZ$aljIkaIVPSarmDMlJUBBP.".toCharArray()));
        assertTrue(hasher.verify(passwd, "hsP1PYSLsEEvs".toCharArray()));
        assertTrue(hasher.verify(passwd, "{plain}test123".toCharArray()));
        assertTrue(hasher.verify(passwd, "{SHA}cojt0Pw//L6ToM8G41aOKFIWh7w=".toCharArray()));
    }

    @Test
    public void testHtpasswdSelfGenerated() throws Exception {
        Hasher hasher = Hasher.HTPASSWD;
        char[] passwd = "test123".toCharArray();
        assertTrue(hasher.verify(passwd, hasher.hash(passwd)));
    }
}
