/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
