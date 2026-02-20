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
import org.elasticsearch.entitlement.rules.DeniedEntitlementStrategy;
import org.elasticsearch.entitlement.rules.EntitlementRule;
import org.elasticsearch.entitlement.rules.function.CheckMethod;
import org.elasticsearch.entitlement.rules.function.VarargCall;
import org.elasticsearch.entitlement.runtime.policy.PolicyChecker;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class InstrumentationRegistryImpl implements InternalInstrumentationRegistry {
    private final PolicyChecker policyChecker;
    private final Map<MethodKey, InstrumentationInfo> methodToImplementationInfo = new HashMap<>();
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
    public Map<MethodKey, InstrumentationInfo> getInstrumentedMethods() {
        return Collections.unmodifiableMap(methodToImplementationInfo);
    }

    public void registerRule(EntitlementRule rule) {
        String id = UUID.randomUUID().toString();
        methodToImplementationInfo.put(rule.methodKey(), new InstrumentationInfo(id, rule.strategy()));
        implementationIdToStrategy.put(id, rule.strategy());
        implementationIdToProvider.put(id, rule.checkMethod());
    }
}
