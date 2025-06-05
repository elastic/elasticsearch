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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.entitlement.runtime.policy.PolicyManager.ComponentKind.PLUGIN;

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
            Map.of(),
            new TestPathLookup(List.of()),
            List.of()
        );
        policyManager.setActive(true);
    }

    public void testReset() {
        assertTrue(policyManager.classEntitlementsMap.isEmpty());
        assertEquals("example-plugin1", policyManager.getEntitlements(getClass()).componentName());
        assertEquals("example-plugin1", policyManager.getEntitlements(getClass()).componentName());
        assertFalse(policyManager.classEntitlementsMap.isEmpty());

        policyManager.reset();

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
}
