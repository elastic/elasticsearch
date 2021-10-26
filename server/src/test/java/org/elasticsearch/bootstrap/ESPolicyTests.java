/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.test.ESTestCase;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;

/**
 * Tests for ESPolicy
 */
public class ESPolicyTests extends ESTestCase {

    /**
     * test restricting privileges to no permissions actually works
     */
    public void testRestrictPrivileges() {
        assumeTrue("test requires security manager", System.getSecurityManager() != null);
        try {
            System.getProperty("user.home");
        } catch (SecurityException e) {
            fail("this test needs to be fixed: user.home not available by policy");
        }

        PermissionCollection noPermissions = new Permissions();
        AccessControlContext noPermissionsAcc = new AccessControlContext(
            new ProtectionDomain[] {
                new ProtectionDomain(null, noPermissions)
            }
        );
        try {
            AccessController.doPrivileged(new PrivilegedAction<Void>() {
                public Void run() {
                    System.getProperty("user.home");
                    fail("access should have been denied");
                    return null;
                }
            }, noPermissionsAcc);
        } catch (SecurityException expected) {
            // expected exception
        }
    }
}
