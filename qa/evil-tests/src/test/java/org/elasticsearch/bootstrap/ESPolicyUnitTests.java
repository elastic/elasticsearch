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

import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

import java.io.FilePermission;
import java.net.SocketPermission;
import java.security.AllPermission;
import java.security.CodeSource;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.ProtectionDomain;
import java.security.cert.Certificate;
import java.util.Collections;

/**
 * Unit tests for ESPolicy: these cannot run with security manager,
 * we don't allow messing with the policy
 */
public class ESPolicyUnitTests extends ESTestCase {
    /**
     * Test policy with null codesource.
     * <p>
     * This can happen when restricting privileges with doPrivileged,
     * even though ProtectionDomain's ctor javadocs might make you think
     * that the policy won't be consulted.
     */
    @SuppressForbidden(reason = "to create FilePermission object")
    public void testNullCodeSource() throws Exception {
        assumeTrue("test cannot run with security manager", System.getSecurityManager() == null);
        // create a policy with AllPermission
        Permission all = new AllPermission();
        PermissionCollection allCollection = all.newPermissionCollection();
        allCollection.add(all);
        ESPolicy policy = new ESPolicy(allCollection, Collections.emptyMap(), true);
        // restrict ourselves to NoPermission
        PermissionCollection noPermissions = new Permissions();
        assertFalse(policy.implies(new ProtectionDomain(null, noPermissions), new FilePermission("foo", "read")));
    }

    /**
     * test with null location
     * <p>
     * its unclear when/if this happens, see https://bugs.openjdk.java.net/browse/JDK-8129972
     */
    @SuppressForbidden(reason = "to create FilePermission object")
    public void testNullLocation() throws Exception {
        assumeTrue("test cannot run with security manager", System.getSecurityManager() == null);
        PermissionCollection noPermissions = new Permissions();
        ESPolicy policy = new ESPolicy(noPermissions, Collections.emptyMap(), true);
        assertFalse(policy.implies(new ProtectionDomain(new CodeSource(null, (Certificate[]) null), noPermissions),
                new FilePermission("foo", "read")));
    }

    public void testListen() {
        assumeTrue("test cannot run with security manager", System.getSecurityManager() == null);
        final PermissionCollection noPermissions = new Permissions();
        final ESPolicy policy = new ESPolicy(noPermissions, Collections.emptyMap(), true);
        assertFalse(
            policy.implies(
                new ProtectionDomain(ESPolicyUnitTests.class.getProtectionDomain().getCodeSource(), noPermissions),
                new SocketPermission("localhost:" + randomFrom(0, randomIntBetween(49152, 65535)), "listen")));
    }

}
