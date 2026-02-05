/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.rules;

import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.elasticsearch.entitlement.rules.function.CheckMethod;
import org.elasticsearch.entitlement.rules.function.VarargCall;
import org.elasticsearch.entitlement.runtime.registry.InternalInstrumentationRegistry;

public class VoidRuleHandlerBuilder<T> extends AbstractRuleHandlerBuilder<T> {

    public VoidRuleHandlerBuilder(
        InternalInstrumentationRegistry registry,
        Class<? extends T> clazz,
        MethodKey methodKey,
        VarargCall<CheckMethod> checkMethod
    ) {
        super(registry, clazz, methodKey, checkMethod);
    }

    public ClassMethodBuilder<T> elseReturnEarly() {
        registry.registerRule(
            new EntitlementRule(methodKey, checkMethod, new DeniedEntitlementStrategy.ReturnEarlyDeniedEntitlementStrategy())
        );
        return new ClassMethodBuilder<>(registry, clazz);
    }
}
