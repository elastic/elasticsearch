/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.rules;

import org.elasticsearch.entitlement.bridge.NotEntitledException;
import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.elasticsearch.entitlement.rules.function.CheckMethod;
import org.elasticsearch.entitlement.rules.function.VarargCall;
import org.elasticsearch.entitlement.runtime.registry.InternalInstrumentationRegistry;

import java.util.function.Function;

/**
 * Abstract base builder for configuring failure handling strategies for entitlement rules.
 * <p>
 * This class provides common failure handling strategies that apply to both void and
 * non-void methods, including throwing exceptions when entitlement checks fail.
 * Subclasses extend this with method-signature-specific strategies.
 *
 * @param <T> the type of the class containing the method
 */
public abstract class AbstractRuleHandlerBuilder<T> {
    protected final InternalInstrumentationRegistry registry;
    protected final Class<? extends T> clazz;
    protected final MethodKey methodKey;
    protected final VarargCall<CheckMethod> checkMethod;

    public AbstractRuleHandlerBuilder(
        InternalInstrumentationRegistry registry,
        Class<? extends T> clazz,
        MethodKey methodKey,
        VarargCall<CheckMethod> checkMethod
    ) {
        this.registry = registry;
        this.clazz = clazz;
        this.methodKey = methodKey;
        this.checkMethod = checkMethod;
    }

    /**
     * Specifies that when the entitlement check fails, a {@link NotEntitledException}
     * should be thrown.
     *
     * @return a class method builder for continuing rule definition
     */
    public ClassMethodBuilder<T> elseThrowNotEntitled() {
        registry.registerRule(
            new EntitlementRule(methodKey, checkMethod, new DeniedEntitlementStrategy.NotEntitledDeniedEntitlementStrategy())
        );
        return new ClassMethodBuilder<>(registry, clazz);
    }

    /**
     * Specifies that when the entitlement check fails, a custom exception should be thrown.
     * The exception is created by the provided function, which receives the original
     * {@link NotEntitledException} as input.
     *
     * @param exceptionSupplier a function that creates the exception to throw
     * @return a class method builder for continuing rule definition
     */
    public ClassMethodBuilder<T> elseThrow(Function<NotEntitledException, ? extends Exception> exceptionSupplier) {
        registry.registerRule(
            new EntitlementRule(methodKey, checkMethod, new DeniedEntitlementStrategy.ExceptionDeniedEntitlementStrategy(exceptionSupplier))
        );
        return new ClassMethodBuilder<>(registry, clazz);
    }
}
