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
import org.elasticsearch.test.ESTestCase;

import java.net.URL;
import java.nio.file.Path;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class TestPolicyManager extends PolicyManager {
    public static final Path TEST_LOCATION_SUFFIX = Path.of("classes", "java", "test");
    boolean isActive;
    boolean isTriviallyAllowingTestCode;

    /**
     * We don't have modules in tests, so we can't use the inherited map of entitlements per module.
     * We need this larger map per class instead.
     */
    final Map<Class<?>, ModuleEntitlements> classEntitlementsMap = new ConcurrentHashMap<>();

    public TestPolicyManager(
        Policy serverPolicy,
        List<Entitlement> apmAgentEntitlements,
        Map<String, Policy> pluginPolicies,
        Function<Class<?>, PolicyScope> scopeResolver,
        Map<String, Collection<Path>> pluginSourcePaths,
        PathLookup pathLookup
    ) {
        super(serverPolicy, apmAgentEntitlements, pluginPolicies, scopeResolver, pluginSourcePaths, pathLookup);
        reset();
    }

    public void setActive(boolean newValue) {
        this.isActive = newValue;
    }

    public void setTriviallyAllowingTestCode(boolean newValue) {
        this.isTriviallyAllowingTestCode = newValue;
    }

    /**
     * Called between tests so each test is not affected by prior tests
     */
    public final void reset() {
        assert moduleEntitlementsMap.isEmpty(): "We're not supposed to be using moduleEntitlementsMap in tests";
        classEntitlementsMap.clear();
        isActive = false;
        isTriviallyAllowingTestCode = true;
    }

    @Override
    protected boolean isTrustedSystemClass(Class<?> requestingClass) {
        ClassLoader loader = requestingClass.getClassLoader();
        return loader == null || loader == ClassLoader.getPlatformClassLoader();
    }

    @Override
    boolean isTriviallyAllowed(Class<?> requestingClass) {
        if (isActive == false) {
            return true;
        }
        if (isTriviallyAllowingTestCode && isTestCaseClass(requestingClass)) {
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

    private boolean isTestCaseClass(Class<?> requestingClass) {
        for (Class<?> candidate = requireNonNull(requestingClass); candidate != null; candidate = candidate.getDeclaringClass()) {
            if (ESTestCase.class.isAssignableFrom(candidate)) {
                return true;
            }
        }
        ProtectionDomain protectionDomain = requestingClass.getProtectionDomain();
        CodeSource codeSource = protectionDomain.getCodeSource();
        if (codeSource == null) {
            // This can happen for JDK classes
            return false;
        }
        URL location = codeSource.getLocation();
        if (location.getProtocol().equals("file") && Path.of(location.getPath()).endsWith(TEST_LOCATION_SUFFIX)) {
            return true;
        }
        return false;
    }

    private static final String[] TEST_FRAMEWORK_PACKAGE_PREFIXES = {
        "com.carrotsearch.randomizedtesting",
        "com.sun.tools.javac",
        "org.apache.lucene.tests",
        "org.gradle",
        "org.junit",
        "org.mockito",
        "net.bytebuddy", // Mockito uses this
    };

    @Override
    protected Path getComponentPathFromClass(Class<?> requestingClass) {
        return Path.of("/");
    }

    @Override
    protected ModuleEntitlements getEntitlements(Class<?> requestingClass) {
        return classEntitlementsMap.computeIfAbsent(requestingClass, c -> computeEntitlements(requestingClass));
    }
}
