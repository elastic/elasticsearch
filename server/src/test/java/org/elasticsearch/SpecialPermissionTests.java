/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.test.ESTestCase;

import java.security.AllPermission;

/** Very simple sanity checks for {@link SpecialPermission} */
public class SpecialPermissionTests extends ESTestCase {

    public void testEquals() {
        assertEquals(new SpecialPermission(), new SpecialPermission());
        assertEquals(SpecialPermission.INSTANCE, new SpecialPermission());
        assertFalse(new SpecialPermission().equals(new AllPermission()));
        assertFalse(SpecialPermission.INSTANCE.equals(new AllPermission()));
    }

    public void testImplies() {
        assertTrue(SpecialPermission.INSTANCE.implies(new SpecialPermission()));
        assertTrue(SpecialPermission.INSTANCE.implies(SpecialPermission.INSTANCE));
        assertFalse(new SpecialPermission().implies(new AllPermission()));
        assertFalse(SpecialPermission.INSTANCE.implies(new AllPermission()));
    }
}
