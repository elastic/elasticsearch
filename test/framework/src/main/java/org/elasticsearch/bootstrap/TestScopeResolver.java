/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.runtime.policy.PolicyManager.PolicyScope;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.lang.module.ModuleDescriptor;
import java.lang.module.ModuleFinder;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.entitlement.runtime.policy.PolicyManager.ALL_UNNAMED;
import static org.elasticsearch.entitlement.runtime.policy.PolicyManager.ComponentKind.PLUGIN;
import static org.elasticsearch.entitlement.runtime.policy.PolicyManager.ComponentKind.SERVER;
import static org.elasticsearch.entitlement.runtime.policy.PolicyManager.MODULES_EXCLUDED_FROM_SYSTEM_MODULES;

public final class TestScopeResolver {

    private static final Logger logger = LogManager.getLogger(TestScopeResolver.class);
    private final Map<String, PolicyScope> scopeMap;
    private static final Map<String, PolicyScope> excludedSystemPackageScopes = computeExcludedSystemPackageScopes();

    public TestScopeResolver(Map<String, PolicyScope> scopeMap) {
        this.scopeMap = scopeMap;
    }

    private static Map<String, PolicyScope> computeExcludedSystemPackageScopes() {
        // Within any one module layer, module names are unique, so we just need the names
        Set<String> systemModuleNames = ModuleFinder.ofSystem()
            .findAll()
            .stream()
            .map(ref -> ref.descriptor().name())
            .filter(MODULES_EXCLUDED_FROM_SYSTEM_MODULES::contains)
            .collect(toSet());

        Map<String, PolicyScope> result = new TreeMap<>();
        ModuleLayer.boot().modules().stream().filter(m -> systemModuleNames.contains(m.getName())).forEach(m -> {
            ModuleDescriptor desc = m.getDescriptor();
            if (desc != null) {
                desc.packages().forEach(pkg ->
                // Our component identification logic returns SERVER for these
                result.put(pkg, new PolicyScope(SERVER, SERVER.componentName, m.getName())));
            }
        });
        return result;
    }

    public static @Nullable PolicyScope getExcludedSystemPackageScope(Class<?> callerClass) {
        return excludedSystemPackageScopes.get(callerClass.getPackageName());
    }

    PolicyScope getScope(Class<?> callerClass) {
        var callerCodeSource = callerClass.getProtectionDomain().getCodeSource();
        if (callerCodeSource == null) {
            // This only happens for JDK classes. Furthermore, for trivially allowed modules, we shouldn't even get here.
            // Hence, this must be an excluded system module, so check for that.
            return requireNonNull(getExcludedSystemPackageScope(callerClass));
        }

        var location = callerCodeSource.getLocation().toString();
        var scope = scopeMap.get(location);
        if (scope == null) {
            // Special cases for libraries not handled by our automatically-generated scopeMap
            if (callerClass.getPackageName().startsWith("org.bouncycastle")) {
                scope = new PolicyScope(PLUGIN, "security", ALL_UNNAMED);
                logger.debug("Assuming bouncycastle is part of the security plugin");
            }
        }
        if (scope == null) {
            logger.warn("Cannot identify a scope for class [{}], location [{}]", callerClass.getName(), location);
            return PolicyScope.unknown(location);
        }
        return scope;
    }

    public static Function<Class<?>, PolicyScope> createScopeResolver(
        TestBuildInfo serverBuildInfo,
        List<TestBuildInfo> pluginsBuildInfo,
        Set<String> modularPlugins
    ) {
        Map<String, PolicyScope> scopeMap = new TreeMap<>(); // Sorted to make it easier to read during debugging
        for (var pluginBuildInfo : pluginsBuildInfo) {
            boolean isModular = modularPlugins.contains(pluginBuildInfo.component());
            for (var location : pluginBuildInfo.locations()) {
                var codeSource = TestScopeResolver.class.getClassLoader().getResource(location.representativeClass());
                if (codeSource == null) {
                    throw new IllegalArgumentException("Cannot locate class [" + location.representativeClass() + "]");
                }
                try {
                    String module = isModular ? location.module() : ALL_UNNAMED;
                    scopeMap.put(
                        getCodeSource(codeSource, location.representativeClass()),
                        PolicyScope.plugin(pluginBuildInfo.component(), module)
                    );
                } catch (MalformedURLException e) {
                    throw new IllegalArgumentException("Cannot locate class [" + location.representativeClass() + "]", e);
                }
            }
        }

        for (var location : serverBuildInfo.locations()) {
            var classUrl = TestScopeResolver.class.getClassLoader().getResource(location.representativeClass());
            if (classUrl == null) {
                logger.debug("Representative class is unavailable; proceeding without {}", location);
                continue;
            }
            try {
                scopeMap.put(getCodeSource(classUrl, location.representativeClass()), PolicyScope.server(location.module()));
            } catch (MalformedURLException e) {
                throw new IllegalArgumentException("Cannot locate class [" + location.representativeClass() + "]", e);
            }
        }

        var testScopeResolver = new TestScopeResolver(scopeMap);
        return testScopeResolver::getScope;
    }

    private static String getCodeSource(URL classUrl, String className) throws MalformedURLException {
        if (isJarUrl(classUrl)) {
            return extractJarFileUrl(classUrl).toString();
        }
        var s = classUrl.toString();
        return s.substring(0, s.indexOf(className));
    }

    private static boolean isJarUrl(URL url) {
        return "jar".equals(url.getProtocol());
    }

    @SuppressWarnings("deprecation")
    @SuppressForbidden(reason = "need file spec in string form to extract the inner URL form the JAR URL")
    private static URL extractJarFileUrl(URL jarUrl) throws MalformedURLException {
        String spec = jarUrl.getFile();
        int separator = spec.indexOf("!/");

        if (separator == -1) {
            throw new MalformedURLException();
        }

        return new URL(spec.substring(0, separator));
    }
}
