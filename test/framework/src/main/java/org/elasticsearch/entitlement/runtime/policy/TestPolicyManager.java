/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.entitlement.runtime.policy.entitlements.Entitlement;
import org.elasticsearch.test.ESTestCase;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class TestPolicyManager extends PolicyManager {

    boolean isActive;
    boolean isTriviallyAllowingTestCode;
    String[] entitledTestPackages = TEST_FRAMEWORK_PACKAGE_PREFIXES;

    /**
     * We don't have modules in tests, so we can't use the inherited map of entitlements per module.
     * We need this larger map per class instead.
     */
    final Map<Class<?>, ModuleEntitlements> classEntitlementsMap = new ConcurrentHashMap<>();

    final Collection<URI> testOnlyClasspath;

    public TestPolicyManager(
        Policy serverPolicy,
        List<Entitlement> apmAgentEntitlements,
        Map<String, Policy> pluginPolicies,
        Function<Class<?>, PolicyScope> scopeResolver,
        Map<String, Collection<Path>> pluginSourcePaths,
        PathLookup pathLookup,
        Collection<URI> testOnlyClasspath
    ) {
        super(serverPolicy, apmAgentEntitlements, pluginPolicies, scopeResolver, pluginSourcePaths, pathLookup);
        this.testOnlyClasspath = testOnlyClasspath;
        reset();
    }

    public void setActive(boolean newValue) {
        this.isActive = newValue;
    }

    public void setTriviallyAllowingTestCode(boolean newValue) {
        this.isTriviallyAllowingTestCode = newValue;
    }

    public void setEntitledTestPackages(String... entitledTestPackages) {
        assertNoRedundantPrefixes(TEST_FRAMEWORK_PACKAGE_PREFIXES, entitledTestPackages, false);
        if (entitledTestPackages.length > 1) {
            assertNoRedundantPrefixes(entitledTestPackages, entitledTestPackages, true);
        }
        String[] packages = ArrayUtils.concat(this.entitledTestPackages, entitledTestPackages);
        Arrays.sort(packages);
        this.entitledTestPackages = packages;
    }

    /**
     * Called between tests so each test is not affected by prior tests
     */
    public final void reset() {
        assert moduleEntitlementsMap.isEmpty() : "We're not supposed to be using moduleEntitlementsMap in tests";
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
        if (isEntitlementClass(requestingClass)) {
            return true;
        }
        if (isTestFrameworkClass(requestingClass)) {
            return true;
        }
        if ("org.elasticsearch.jdk".equals(requestingClass.getPackageName())) {
            // PluginsLoaderTests, PluginsServiceTests, PluginsUtilsTests
            return true;
        }
        if ("org.elasticsearch.nativeaccess".equals(requestingClass.getPackageName())) {
            // UberModuleClassLoaderTests
            return true;
        }
        if (requestingClass.getPackageName().startsWith("org.elasticsearch.plugins")) {
            // PluginsServiceTests, NamedComponentReaderTests
            return true;
        }
        if (isTriviallyAllowingTestCode && isTestCode(requestingClass)) {
            return true;
        }
        return super.isTriviallyAllowed(requestingClass);
    }

    private boolean isEntitlementClass(Class<?> requestingClass) {
        return requestingClass.getPackageName().startsWith("org.elasticsearch.entitlement")
            && (requestingClass.getName().contains("Test") == false);
    }

    private boolean isTestFrameworkClass(Class<?> requestingClass) {
        return isTestFrameworkClass(entitledTestPackages, requestingClass.getPackageName());
    }

    // no redundant entries allowed, see assertNoRedundantPrefixes
    static boolean isTestFrameworkClass(String[] sortedPrefixes, String packageName) {
        int idx = Arrays.binarySearch(sortedPrefixes, packageName);
        if (idx >= 0) {
            return true;
        }
        idx = -idx - 2; // candidate package index (insertion point - 1)
        if (idx >= 0 && idx < sortedPrefixes.length) {
            String candidate = sortedPrefixes[idx];
            if (packageName.startsWith(candidate)
                && (packageName.length() == candidate.length() || packageName.charAt(candidate.length()) == '.')) {
                return true;
            }
        }
        return false;
    }

    private static boolean isNotPrefixMatch(String name, String prefix, boolean discardExactMatch) {
        assert prefix.endsWith(".") == false : "Invalid package prefix ending with '.' [" + prefix + "]";
        if (name == prefix || name.startsWith(prefix)) {
            if (name.length() == prefix.length()) {
                return discardExactMatch;
            }
            return false == (name.length() > prefix.length() && name.charAt(prefix.length()) == '.');
        }
        return true;
    }

    static void assertNoRedundantPrefixes(String[] setA, String[] setB, boolean discardExactMatch) {
        for (String a : setA) {
            for (String b : setB) {
                assert isNotPrefixMatch(a, b, discardExactMatch) && isNotPrefixMatch(b, a, discardExactMatch)
                    : "Redundant prefix entries: [" + a + ", " + b + "]";
            }
        }
    }

    private boolean isTestCode(Class<?> requestingClass) {
        // TODO: Cache this? It's expensive
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
        URI needle;
        try {
            needle = codeSource.getLocation().toURI();
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
        boolean result = testOnlyClasspath.contains(needle);
        return result;
    }

    private static final String[] TEST_FRAMEWORK_PACKAGE_PREFIXES = {
        "org.gradle",

        "org.jcodings", // A library loaded with SPI that tries to create a CharsetProvider
        "com.google.common.jimfs", // Used on Windows

        // We shouldn't really need the rest of these. They should be discovered on the testOnlyClasspath.
        "com.carrotsearch.randomizedtesting",
        "com.sun.tools.javac",
        "org.apache.lucene.tests", // Interferes with SSLErrorMessageFileTests.testMessageForPemCertificateOutsideConfigDir
        "org.junit",
        "org.mockito",
        "net.bytebuddy", // Mockito uses this

        "org.bouncycastle.jsse.provider" // Used in test code if FIPS is enabled, support more fine-grained config in ES-12128
    };

    static {
        Arrays.sort(TEST_FRAMEWORK_PACKAGE_PREFIXES);
    }

    @Override
    protected ModuleEntitlements getEntitlements(Class<?> requestingClass) {
        return classEntitlementsMap.computeIfAbsent(requestingClass, this::computeEntitlements);
    }
}
