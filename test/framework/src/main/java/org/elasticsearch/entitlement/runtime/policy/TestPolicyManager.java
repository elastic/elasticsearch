/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.entitlement.runtime.policy.entitlements.Entitlement;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class TestPolicyManager extends PolicyManager {
    public TestPolicyManager(
        Policy serverPolicy,
        List<Entitlement> apmAgentEntitlements,
        Map<String, Policy> pluginPolicies,
        Function<Class<?>, PolicyScope> scopeResolver,
        Map<String, Collection<Path>> pluginSourcePaths,
        PathLookup pathLookup
    ) {
        super(serverPolicy, apmAgentEntitlements, pluginPolicies, scopeResolver, pluginSourcePaths, pathLookup);
    }

    /**
     * Called between tests so each test is not affected by prior tests
     */
    public void reset() {
        super.moduleEntitlementsMap.clear();
    }

    @Override
    protected boolean isTrustedSystemClass(Class<?> requestingClass) {
        ClassLoader loader = requestingClass.getClassLoader();
        return loader == null || loader == ClassLoader.getPlatformClassLoader();
    }

    @Override
    boolean isTriviallyAllowed(Class<?> requestingClass) {
        return isTestFrameworkClass(requestingClass) || isEntitlementClass(requestingClass) || super.isTriviallyAllowed(requestingClass);
    }

    private boolean isEntitlementClass(Class<?> requestingClass) {
        return requestingClass.getPackageName().startsWith("org.elasticsearch.entitlement")
            && (requestingClass.getName().contains("Test") == false);
    }

    private boolean isTestFrameworkClass(Class<?> requestingClass) {
        String packageName = requestingClass.getPackageName();
        return packageName.startsWith("org.junit") || packageName.startsWith("org.gradle");
    }
}
