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
import org.elasticsearch.entitlement.runtime.policy.PolicyManager;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public record TestScopeResolver(Map<String, PolicyManager.PolicyScope> scopeMap) {

    private static final Logger logger = LogManager.getLogger(TestScopeResolver.class);

    PolicyManager.PolicyScope getScope(Class<?> callerClass) {
        var callerCodeSource = callerClass.getProtectionDomain().getCodeSource();
        assert callerCodeSource != null;

        var location = callerCodeSource.getLocation().toString();
        var scope = scopeMap.get(location);
        if (scope == null) {
            logger.warn("Cannot identify a scope for class [{}], location [{}]", callerClass.getName(), location);
            return PolicyManager.PolicyScope.unknown(location);
        }
        return scope;
    }

    public static Function<Class<?>, PolicyManager.PolicyScope> createScopeResolver(
        TestBuildInfo serverBuildInfo,
        List<TestBuildInfo> pluginsBuildInfo
    ) {

        Map<String, PolicyManager.PolicyScope> scopeMap = new HashMap<>();
        for (var pluginBuildInfo : pluginsBuildInfo) {
            for (var location : pluginBuildInfo.locations()) {
                var codeSource = TestScopeResolver.class.getClassLoader().getResource(location.representativeClass());
                if (codeSource == null) {
                    throw new IllegalArgumentException("Cannot locate class [" + location.representativeClass() + "]");
                }
                try {
                    scopeMap.put(
                        getCodeSource(codeSource, location.representativeClass()),
                        PolicyManager.PolicyScope.plugin(pluginBuildInfo.component(), location.module())
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
                scopeMap.put(getCodeSource(classUrl, location.representativeClass()), PolicyManager.PolicyScope.server(location.module()));
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
