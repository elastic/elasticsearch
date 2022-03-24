/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

import java.io.FilePermission;
import java.net.SocketPermission;
import java.net.URL;
import java.security.AllPermission;
import java.security.CodeSource;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.ProtectionDomain;
import java.security.cert.Certificate;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for ESPolicy: these cannot run with security manager,
 * we don't allow messing with the policy
 */
public class ESPolicyUnitTests extends ESTestCase {

    static final Map<String, URL> TEST_CODEBASES = BootstrapForTesting.getCodebases();

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
        ESPolicy policy = new ESPolicy(TEST_CODEBASES, allCollection, Collections.emptyMap(), true, List.of());
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
        ESPolicy policy = new ESPolicy(TEST_CODEBASES, noPermissions, Collections.emptyMap(), true, List.of());
        assertFalse(
            policy.implies(
                new ProtectionDomain(new CodeSource(null, (Certificate[]) null), noPermissions),
                new FilePermission("foo", "read")
            )
        );
    }

    public void testListen() {
        assumeTrue("test cannot run with security manager", System.getSecurityManager() == null);
        final PermissionCollection noPermissions = new Permissions();
        final ESPolicy policy = new ESPolicy(TEST_CODEBASES, noPermissions, Collections.emptyMap(), true, List.of());
        assertFalse(
            policy.implies(
                new ProtectionDomain(ESPolicyUnitTests.class.getProtectionDomain().getCodeSource(), noPermissions),
                new SocketPermission("localhost:" + randomFrom(0, randomIntBetween(49152, 65535)), "listen")
            )
        );
    }

    @SuppressForbidden(reason = "to create FilePermission object")
    public void testDataPathPermissionIsChecked() {
        assumeTrue("test cannot run with security manager", System.getSecurityManager() == null);
        final ESPolicy policy = new ESPolicy(
            TEST_CODEBASES,
            new Permissions(),
            Collections.emptyMap(),
            true,
            List.of(new FilePermission("/home/elasticsearch/data/-", "read"))
        );
        assertTrue(
            policy.implies(
                new ProtectionDomain(new CodeSource(null, (Certificate[]) null), new Permissions()),
                new FilePermission("/home/elasticsearch/data/index/file.si", "read")
            )
        );
    }
}
