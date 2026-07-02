/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.registry;

import org.elasticsearch.entitlement.bridge.NotEntitledException;
import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.elasticsearch.entitlement.instrumentation.MethodSignature;
import org.elasticsearch.entitlement.rules.DeniedEntitlementStrategy;
import org.elasticsearch.entitlement.rules.EntitlementRule;
import org.elasticsearch.entitlement.rules.function.CheckMethod;
import org.elasticsearch.entitlement.rules.function.VarargCall;
import org.elasticsearch.entitlement.runtime.policy.PolicyChecker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class InstrumentationRegistryImpl implements InternalInstrumentationRegistry {
    private final PolicyChecker policyChecker;
    private final Map<String, Map<MethodSignature, InstrumentationInfo>> rulesByClass = new HashMap<>();
    private final Map<String, DeniedEntitlementStrategy> implementationIdToStrategy = new HashMap<>();
    private final Map<String, VarargCall<CheckMethod>> implementationIdToProvider = new HashMap<>();

    public InstrumentationRegistryImpl(PolicyChecker policyChecker) {
        this.policyChecker = policyChecker;
    }

    @Override
    public void check$(String instrumentationId, Class<?> callingClass, Object... args) throws Exception {
        CheckMethod checkMethod = implementationIdToProvider.get(instrumentationId).call(args);
        DeniedEntitlementStrategy strategy = implementationIdToStrategy.get(instrumentationId);
        if (strategy instanceof DeniedEntitlementStrategy.ExceptionDeniedEntitlementStrategy exceptionStrategy) {
            try {
                checkMethod.check(callingClass, policyChecker);
            } catch (NotEntitledException e) {
                throw exceptionStrategy.getExceptionSupplier().apply(e);
            }
        } else {
            checkMethod.check(callingClass, policyChecker);
        }
    }

    @Override
    public Object defaultValue$(String instrumentationId) {
        DeniedEntitlementStrategy strategy = implementationIdToStrategy.get(instrumentationId);
        if (strategy instanceof DeniedEntitlementStrategy.DefaultValueDeniedEntitlementStrategy<?> defaultValue) {
            return defaultValue.getDefaultValue();
        }
        throw new IllegalStateException("No default value configured for instrumentation id [" + instrumentationId + "]");
    }

    @Override
    public Map<String, Map<MethodSignature, InstrumentationInfo>> getInstrumentedMethods() {
        return Collections.unmodifiableMap(rulesByClass);
    }

    public void registerRule(EntitlementRule rule) {
        MethodKey methodKey = rule.methodKey();
        String id = UUID.randomUUID().toString();
        InstrumentationInfo info = new InstrumentationInfo(id, rule.strategy());
        InstrumentationInfo previous = rulesByClass.computeIfAbsent(methodKey.className(), k -> new HashMap<>())
            .put(methodKey.methodSignature(), info);
        if (previous != null) {
            throw new IllegalStateException("Rule has already been registered for method [" + methodKey + "].");
        }
        implementationIdToStrategy.put(id, rule.strategy());
        implementationIdToProvider.put(id, rule.checkMethod());
    }

    @Override
    public void validate() {
        Map<MethodSignature, List<String>> signatureToClasses = new HashMap<>();
        for (var entry : rulesByClass.entrySet()) {
            for (MethodSignature sig : entry.getValue().keySet()) {
                if ("<init>".equals(sig.methodName())) {
                    continue;
                }
                signatureToClasses.computeIfAbsent(sig, k -> new ArrayList<>()).add(entry.getKey());
            }
        }
        for (var entry : signatureToClasses.entrySet()) {
            List<String> classNames = entry.getValue();
            if (classNames.size() < 2) {
                continue;
            }
            for (int i = 0; i < classNames.size(); i++) {
                for (int j = i + 1; j < classNames.size(); j++) {
                    checkNoHierarchyOverlap(entry.getKey(), classNames.get(i), classNames.get(j));
                }
            }
        }
    }

    private static void checkNoHierarchyOverlap(MethodSignature signature, String internalNameA, String internalNameB) {
        Class<?> classA = loadClassFromInternalName(internalNameA);
        Class<?> classB = loadClassFromInternalName(internalNameB);
        if (classA.isAssignableFrom(classB) || classB.isAssignableFrom(classA)) {
            throw new IllegalStateException(
                "Overlapping rules for method ["
                    + signature
                    + "]: ["
                    + internalNameA
                    + "] and ["
                    + internalNameB
                    + "] are in the same type hierarchy"
            );
        }
    }

    private static Class<?> loadClassFromInternalName(String internalName) {
        String binaryName = internalName.replace('/', '.');
        try {
            return Class.forName(binaryName);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Cannot load class [" + binaryName + "] for rule validation", e);
        }
    }
}
