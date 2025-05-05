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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

record TestScopeResolver(Map<String, PolicyManager.PolicyScope> scopeMap) {
    PolicyManager.PolicyScope getScope(Class<?> callerClass) {
        var callerCodeSource = callerClass.getProtectionDomain().getCodeSource().getLocation().toString();
        return scopeMap.getOrDefault(callerCodeSource, PolicyManager.PolicyScope.unknown(null));
    }

    Function<Class<?>, PolicyManager.PolicyScope> createScopeResolver(TestBuildInfo serverBuildInfo, List<TestBuildInfo> pluginsBuildInfo) {

        Map<String, PolicyManager.PolicyScope> scopeMap = new HashMap<>();
        for (var pluginBuildInfo: pluginsBuildInfo) {
            for (var location : pluginBuildInfo.locations()) {
                var codeSource = this.getClass().getClassLoader().getResource(location.className());
                if (codeSource == null) {
                    throw new IllegalArgumentException("Cannot locate class [" + location.className() + "]");
                }
                scopeMap.put(
                    codeSource.toString(),
                    PolicyManager.PolicyScope.plugin(pluginBuildInfo.componentName(), location.moduleName())
                );
            }
        }

        for (var location : serverBuildInfo.locations()) {
            var codeSource = this.getClass().getClassLoader().getResource(location.className());
            if (codeSource == null) {
                throw new IllegalArgumentException("Cannot locate class [" + location.className() + "]");
            }
            scopeMap.put(codeSource.toString(), PolicyManager.PolicyScope.server(location.moduleName()));
        }

        var testScopeResolver = new TestScopeResolver(scopeMap);
        return testScopeResolver::getScope;
    }
}
