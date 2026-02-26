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

/**
 * Builder for configuring failure handling strategies for methods that return values.
 * <p>
 * This class extends {@link VoidRuleHandlerBuilder} and adds additional strategies
 * specific to methods with return values, such as returning a default value or
 * returning one of the method's arguments when an entitlement check fails.
 *
 * @param <T> the type of the class containing the method
 * @param <R> the return type of the method
 */
public class RuleHandlerBuilder<T, R> extends VoidRuleHandlerBuilder<T> {

    public RuleHandlerBuilder(
        InternalInstrumentationRegistry registry,
        Class<? extends T> clazz,
        MethodKey methodKey,
        VarargCall<CheckMethod> checkMethod
    ) {
        super(registry, clazz, methodKey, checkMethod);
    }

    /**
     * Specifies that when the entitlement check fails, the method should return
     * the provided default value instead of throwing an exception.
     *
     * @param defaultValue the value to return when the entitlement check fails
     * @return a class method builder for continuing rule definition
     */
    public ClassMethodBuilder<T> elseReturn(R defaultValue) {
        registry.registerRule(
            new EntitlementRule(methodKey, checkMethod, new DeniedEntitlementStrategy.DefaultValueDeniedEntitlementStrategy<>(defaultValue))
        );
        return new ClassMethodBuilder<>(registry, clazz);
    }

    /**
     * Specifies that when the entitlement check fails, the method should return
     * the value of the argument at the specified index.
     *
     * @param index the zero-based index of the method argument to return
     * @return a class method builder for continuing rule definition
     */
    public ClassMethodBuilder<T> elseReturnArg(int index) {
        registry.registerRule(
            new EntitlementRule(methodKey, checkMethod, new DeniedEntitlementStrategy.MethodArgumentValueDeniedEntitlementStrategy(index))
        );
        return new ClassMethodBuilder<>(registry, clazz);
    }
}
