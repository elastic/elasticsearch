/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.jdk.RuntimeVersionFeature;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.FilePermission;
import java.net.SocketPermission;
import java.net.URL;
import java.security.AllPermission;
import java.security.CodeSource;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.security.cert.Certificate;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Map.entry;
import static org.elasticsearch.bootstrap.ESPolicy.POLICY_RESOURCE;

/**
 * Unit tests for ESPolicy: these cannot run with security manager,
 * we don't allow messing with the policy
 */
public class ESPolicyUnitTests extends ESTestCase {

    static final Map<String, URL> TEST_CODEBASES = BootstrapForTesting.getCodebases();
    static Policy DEFAULT_POLICY;

    @BeforeClass
    public static void setupPolicy() {
        assumeTrue("test requires security manager to be supported", RuntimeVersionFeature.isSecurityManagerAvailable());
        assumeTrue("test cannot run with security manager", System.getSecurityManager() == null);
        DEFAULT_POLICY = PolicyUtil.readPolicy(ESPolicy.class.getResource(POLICY_RESOURCE), TEST_CODEBASES);
    }

    /**
     * Test policy with null codesource.
     * <p>
     * This can happen when restricting privileges with doPrivileged,
     * even though ProtectionDomain's ctor javadocs might make you think
     * that the policy won't be consulted.
     */
    @SuppressForbidden(reason = "to create FilePermission object")
    public void testNullCodeSource() throws Exception {
        // create a policy with AllPermission
        Permission all = new AllPermission();
        PermissionCollection allCollection = all.newPermissionCollection();
        allCollection.add(all);
        ESPolicy policy = new ESPolicy(DEFAULT_POLICY, allCollection, Map.of(), true, List.of(), Map.of());
        // restrict ourselves to NoPermission
        PermissionCollection noPermissions = new Permissions();
        assertFalse(policy.implies(new ProtectionDomain(null, noPermissions), new FilePermission("foo", "read")));
    }

    /**
     * As of JDK 9, {@link CodeSource#getLocation} is documented to potentially return {@code null}
     */
    @SuppressForbidden(reason = "to create FilePermission object")
    public void testNullLocation() throws Exception {
        PermissionCollection noPermissions = new Permissions();
        ESPolicy policy = new ESPolicy(DEFAULT_POLICY, noPermissions, Map.of(), true, List.of(), Map.of());
        assertFalse(
            policy.implies(
                new ProtectionDomain(new CodeSource(null, (Certificate[]) null), noPermissions),
                new FilePermission("foo", "read")
            )
        );
    }

    public void testListen() {
        final PermissionCollection noPermissions = new Permissions();
        final ESPolicy policy = new ESPolicy(DEFAULT_POLICY, noPermissions, Map.of(), true, List.of(), Map.of());
        assertFalse(
            policy.implies(
                new ProtectionDomain(ESPolicyUnitTests.class.getProtectionDomain().getCodeSource(), noPermissions),
                new SocketPermission("localhost:" + randomFrom(0, randomIntBetween(49152, 65535)), "listen")
            )
        );
    }

    @SuppressForbidden(reason = "to create FilePermission object")
    public void testDataPathPermissionIsChecked() {
        final ESPolicy policy = new ESPolicy(
            DEFAULT_POLICY,
            new Permissions(),
            Map.of(),
            true,
            List.of(new FilePermission("/home/elasticsearch/data/-", "read")),
            Map.of()
        );
        assertTrue(
            policy.implies(
                new ProtectionDomain(new CodeSource(null, (Certificate[]) null), new Permissions()),
                new FilePermission("/home/elasticsearch/data/index/file.si", "read")
            )
        );
    }

    @SuppressForbidden(reason = "to create FilePermission object")
    public void testSecuredAccess() {
        String file1 = "/home/elasticsearch/config/pluginFile1.yml";
        URL codebase1 = randomFrom(TEST_CODEBASES.values());
        String file2 = "/home/elasticsearch/config/pluginFile2.yml";
        URL codebase2 = randomValueOtherThan(codebase1, () -> randomFrom(TEST_CODEBASES.values()));
        String dir1 = "/home/elasticsearch/config/pluginDir/";
        URL codebase3 = randomValueOtherThanMany(Set.of(codebase1, codebase2)::contains, () -> randomFrom(TEST_CODEBASES.values()));
        URL otherCodebase = randomValueOtherThanMany(
            Set.of(codebase1, codebase2, codebase3)::contains,
            () -> randomFrom(TEST_CODEBASES.values())
        );

        ESPolicy policy = new ESPolicy(
            DEFAULT_POLICY,
            new Permissions(),
            Map.of(),
            true,
            List.of(),
            Map.ofEntries(entry(file1, Set.of(codebase1)), entry(file2, Set.of(codebase1, codebase2)), entry(dir1 + "*", Set.of(codebase3)))
        );

        ProtectionDomain nullDomain = new ProtectionDomain(new CodeSource(null, (Certificate[]) null), new Permissions());
        ProtectionDomain codebase1Domain = new ProtectionDomain(new CodeSource(codebase1, (Certificate[]) null), new Permissions());
        ProtectionDomain codebase2Domain = new ProtectionDomain(new CodeSource(codebase2, (Certificate[]) null), new Permissions());
        ProtectionDomain codebase3Domain = new ProtectionDomain(new CodeSource(codebase3, (Certificate[]) null), new Permissions());
        ProtectionDomain otherCodebaseDomain = new ProtectionDomain(new CodeSource(otherCodebase, (Certificate[]) null), new Permissions());

        Set<String> actions = Set.of("read", "write", "read,write", "delete", "read,write,execute,readlink,delete");

        assertFalse(policy.implies(nullDomain, new FilePermission(file1, randomFrom(actions))));
        assertFalse(policy.implies(otherCodebaseDomain, new FilePermission(file1, randomFrom(actions))));
        assertTrue(policy.implies(codebase1Domain, new FilePermission(file1, randomFrom(actions))));
        assertFalse(policy.implies(codebase2Domain, new FilePermission(file1, randomFrom(actions))));
        assertFalse(policy.implies(codebase3Domain, new FilePermission(file1, randomFrom(actions))));

        assertFalse(policy.implies(nullDomain, new FilePermission(file2, randomFrom(actions))));
        assertFalse(policy.implies(otherCodebaseDomain, new FilePermission(file2, randomFrom(actions))));
        assertTrue(policy.implies(codebase1Domain, new FilePermission(file2, randomFrom(actions))));
        assertTrue(policy.implies(codebase2Domain, new FilePermission(file2, randomFrom(actions))));
        assertFalse(policy.implies(codebase3Domain, new FilePermission(file2, randomFrom(actions))));

        String dirFile = dir1 + "file.yml";
        assertFalse(policy.implies(nullDomain, new FilePermission(dirFile, randomFrom(actions))));
        assertFalse(policy.implies(otherCodebaseDomain, new FilePermission(dirFile, randomFrom(actions))));
        assertFalse(policy.implies(codebase1Domain, new FilePermission(dirFile, randomFrom(actions))));
        assertFalse(policy.implies(codebase2Domain, new FilePermission(dirFile, randomFrom(actions))));
        assertTrue(policy.implies(codebase3Domain, new FilePermission(dirFile, randomFrom(actions))));
    }
}
