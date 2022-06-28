/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.secure_sm;

import junit.framework.TestCase;

import java.security.AllPermission;

/**
 * Simple tests for ThreadPermission
 */
public class ThreadPermissionTests extends TestCase {

    public void testEquals() {
        assertEquals(new ThreadPermission("modifyArbitraryThread"), new ThreadPermission("modifyArbitraryThread"));
        assertFalse(new ThreadPermission("modifyArbitraryThread").equals(new AllPermission()));
        assertFalse(new ThreadPermission("modifyArbitraryThread").equals(new ThreadPermission("modifyArbitraryThreadGroup")));
    }

    public void testImplies() {
        assertTrue(new ThreadPermission("modifyArbitraryThread").implies(new ThreadPermission("modifyArbitraryThread")));
        assertTrue(new ThreadPermission("modifyArbitraryThreadGroup").implies(new ThreadPermission("modifyArbitraryThreadGroup")));
        assertFalse(new ThreadPermission("modifyArbitraryThread").implies(new ThreadPermission("modifyArbitraryThreadGroup")));
        assertFalse(new ThreadPermission("modifyArbitraryThreadGroup").implies(new ThreadPermission("modifyArbitraryThread")));
        assertFalse(new ThreadPermission("modifyArbitraryThread").implies(new AllPermission()));
        assertFalse(new ThreadPermission("modifyArbitraryThreadGroup").implies(new AllPermission()));
        assertTrue(new ThreadPermission("*").implies(new ThreadPermission("modifyArbitraryThread")));
        assertTrue(new ThreadPermission("*").implies(new ThreadPermission("modifyArbitraryThreadGroup")));
        assertFalse(new ThreadPermission("*").implies(new AllPermission()));
    }
}
