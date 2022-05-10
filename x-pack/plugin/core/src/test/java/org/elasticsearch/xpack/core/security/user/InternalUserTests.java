/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.test.ESTestCase;

public class InternalUserTests extends ESTestCase {

    public void testInternalUsersHaveEmptyRoles() {
        assertEquals(SystemUser.INSTANCE.roles().length, 0);
        assertEquals(XPackUser.INSTANCE.roles().length, 0);
        assertEquals(XPackSecurityUser.INSTANCE.roles().length, 0);
        assertEquals(AsyncSearchUser.INSTANCE.roles().length, 0);
        assertEquals(SecurityProfileUser.INSTANCE.roles().length, 0);
    }
}
