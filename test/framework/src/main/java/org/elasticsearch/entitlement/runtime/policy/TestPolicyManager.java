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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class TestPolicyManager extends PolicyManager {
    final AtomicBoolean isActive;

    public TestPolicyManager(
        Policy serverPolicy,
        List<Entitlement> apmAgentEntitlements,
        Map<String, Policy> pluginPolicies,
        Function<Class<?>, PolicyScope> scopeResolver,
        Map<String, Path> sourcePaths,
        PathLookup pathLookup,
        AtomicBoolean isActive
    ) {
        super(serverPolicy, apmAgentEntitlements, pluginPolicies, scopeResolver, sourcePaths, pathLookup);
        this.isActive = isActive;
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
        if (isActive.get() == false) {
            return true;
        }
        if (isTestFrameworkClass(requestingClass) || isEntitlementClass(requestingClass)) {
            return true;
        }
        return super.isTriviallyAllowed(requestingClass);
    }

    private boolean isEntitlementClass(Class<?> requestingClass) {
        return requestingClass.getPackageName().startsWith("org.elasticsearch.entitlement")
            && (requestingClass.getName().contains("Test") == false);
    }

    private boolean isTestFrameworkClass(Class<?> requestingClass) {
        String packageName = requestingClass.getPackageName();
        for (String prefix: TEST_FRAMEWORK_PACKAGE_PREFIXES) {
            if (packageName.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private static final String[] TEST_FRAMEWORK_PACKAGE_PREFIXES = {
        "com.carrotsearch.randomizedtesting",
        "org.apache.lucene.tests",
        "org.gradle",
        "org.junit",
    };
}
