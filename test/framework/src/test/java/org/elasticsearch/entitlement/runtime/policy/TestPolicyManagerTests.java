/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.entitlement.runtime.policy.PolicyManager.PolicyScope;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.elasticsearch.entitlement.runtime.policy.PolicyManager.ComponentKind.PLUGIN;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TestPolicyManagerTests extends ESTestCase {
    TestPolicyManager policyManager;

    @Before
    public void setupPolicyManager() {
        AtomicInteger scopeCounter = new AtomicInteger(0);
        policyManager = new TestPolicyManager(
            new Policy("empty", List.of()),
            List.of(),
            Map.of(),
            c -> new PolicyScope(PLUGIN, "example-plugin" + scopeCounter.incrementAndGet(), "org.example.module"),
            new PathLookup() {
                @Override
                public Path pidFile() {
                    return null;
                }

                @Override
                public Stream<Path> getBaseDirPaths(BaseDir baseDir) {
                    return Stream.empty();
                }

                @Override
                public Stream<Path> resolveSettingPaths(BaseDir baseDir, String settingName) {
                    return Stream.empty();
                }

                @Override
                public boolean isPathOnDefaultFilesystem(Path path) {
                    return true;
                }
            },
            List.of(),
            List.of()
        );
        policyManager.setActive(true);
    }

    public void testClearModuleEntitlementsCache() {
        assertTrue(policyManager.classEntitlementsMap.isEmpty());
        assertEquals("example-plugin1", policyManager.getEntitlements(getClass()).componentName());
        assertEquals("example-plugin1", policyManager.getEntitlements(getClass()).componentName());
        assertFalse(policyManager.classEntitlementsMap.isEmpty());

        policyManager.clearModuleEntitlementsCache();

        assertTrue(policyManager.classEntitlementsMap.isEmpty());
        assertEquals("example-plugin2", policyManager.getEntitlements(getClass()).componentName());
        assertEquals("example-plugin2", policyManager.getEntitlements(getClass()).componentName());
        assertFalse(policyManager.classEntitlementsMap.isEmpty());
    }

    public void testIsTriviallyAllowed() {
        assertTrue(policyManager.isTriviallyAllowed(String.class));
        assertTrue(policyManager.isTriviallyAllowed(org.junit.Before.class));
        assertTrue(policyManager.isTriviallyAllowed(PolicyManager.class));

        assertTrue(policyManager.isTriviallyAllowed(getClass()));
        policyManager.setTriviallyAllowingTestCode(false);
        assertFalse(policyManager.isTriviallyAllowed(getClass()));
    }

    public void testDefaultEntitledTestPackages() {
        String[] testPackages = policyManager.entitledTestPackages.clone();
        TestPolicyManager.assertNoRedundantPrefixes(testPackages, testPackages, true);

        Arrays.sort(testPackages);
        assertThat("Entitled test framework packages are not sorted", policyManager.entitledTestPackages, equalTo(testPackages));
    }

    public void testRejectSetRedundantEntitledTestPackages() {
        var throwable = expectThrows(AssertionError.class, () -> policyManager.setEntitledTestPackages("org.apache.lucene.tests"));
        var baseMatcher = both(containsString("Redundant prefix entries"));
        assertThat(throwable.getMessage(), baseMatcher.and(containsString("org.apache.lucene.tests, org.apache.lucene.tests")));

        throwable = expectThrows(AssertionError.class, () -> policyManager.setEntitledTestPackages("org.apache.lucene"));
        assertThat(throwable.getMessage(), baseMatcher.and(containsString("org.apache.lucene.tests, org.apache.lucene")));

        throwable = expectThrows(AssertionError.class, () -> policyManager.setEntitledTestPackages("org.apache.lucene.tests.whatever"));
        assertThat(throwable.getMessage(), baseMatcher.and(containsString("org.apache.lucene.tests, org.apache.lucene.tests.whatever")));

        throwable = expectThrows(AssertionError.class, () -> policyManager.setEntitledTestPackages("my.package", "my.package.sub"));
        assertThat(throwable.getMessage(), baseMatcher.and(containsString("my.package, my.package.sub")));

        throwable = expectThrows(AssertionError.class, () -> policyManager.setEntitledTestPackages("trailing.dot."));
        assertThat(throwable.getMessage(), containsString("Invalid package prefix ending with '.' [trailing.dot.]"));
    }

    public void testIsTestFrameworkClass() {
        String[] sortedPrefixes = { "a.b", "a.bc", "a.c" };

        assertTrue(TestPolicyManager.isTestFrameworkClass(sortedPrefixes, "a.b"));
        assertTrue(TestPolicyManager.isTestFrameworkClass(sortedPrefixes, "a.b.c"));
        assertTrue(TestPolicyManager.isTestFrameworkClass(sortedPrefixes, "a.bc"));
        assertTrue(TestPolicyManager.isTestFrameworkClass(sortedPrefixes, "a.bc.a"));

        assertFalse(TestPolicyManager.isTestFrameworkClass(sortedPrefixes, "a"));
        assertFalse(TestPolicyManager.isTestFrameworkClass(sortedPrefixes, "a.ba"));
        assertFalse(TestPolicyManager.isTestFrameworkClass(sortedPrefixes, "a.bcc"));
    }
}
