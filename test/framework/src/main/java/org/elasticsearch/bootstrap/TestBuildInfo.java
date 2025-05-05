/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.entitlement.runtime.policy.PolicyManager;

import java.security.CodeSource;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

record TestBuildInfoLocations(String className, String moduleName) { }

record TestBuildInfo(String componentName, List<TestBuildInfoLocations> locations) {

    private static class TestScopeResolver {

        private final Map<CodeSource, PolicyManager.PolicyScope> scopeMap;

        PolicyManager.PolicyScope getScope(Class<?> callerClass) {
            CodeSource callerCodeSource = callerClass.getProtectionDomain().getCodeSource();
            return scopeMap.getOrDefault(callerCodeSource, PolicyManager.PolicyScope.unknown(null));
        }

    }

    Function<Class<?>, PolicyManager.PolicyScope> toScopeResolver() {

        for (var location: locations) {
            location.className()
        }

        var testScopeResolver = new TestScopeResolver();
        return testScopeResolver::getScope;
    }

}
