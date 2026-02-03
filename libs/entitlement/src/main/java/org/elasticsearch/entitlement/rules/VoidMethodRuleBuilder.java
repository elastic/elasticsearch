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
import org.elasticsearch.entitlement.rules.function.Call0;
import org.elasticsearch.entitlement.rules.function.Call1;
import org.elasticsearch.entitlement.rules.function.Call2;
import org.elasticsearch.entitlement.rules.function.Call3;
import org.elasticsearch.entitlement.rules.function.Call4;
import org.elasticsearch.entitlement.rules.function.Call5;
import org.elasticsearch.entitlement.rules.function.Call6;
import org.elasticsearch.entitlement.rules.function.CheckMethod;
import org.elasticsearch.entitlement.runtime.registry.InternalInstrumentationRegistry;

public class VoidMethodRuleBuilder<T> {
    protected final InternalInstrumentationRegistry registry;
    protected final MethodKey methodKey;
    protected final Class<? extends T> clazz;

    public VoidMethodRuleBuilder(InternalInstrumentationRegistry registry, Class<? extends T> clazz, MethodKey methodKey) {
        this.registry = registry;
        this.clazz = clazz;
        this.methodKey = methodKey;
    }

    public VoidRuleHandlerBuilder<T> enforce(Call0<CheckMethod> policyCheckSupplier) {
        return new VoidRuleHandlerBuilder<>(registry, clazz, methodKey, policyCheckSupplier.asVarargCall());
    }

    public static class VoidMethodRuleBuilder1<T, A> extends VoidMethodRuleBuilder<T> {

        public VoidMethodRuleBuilder1(InternalInstrumentationRegistry registry, Class<? extends T> clazz, MethodKey methodKey) {
            super(registry, clazz, methodKey);
        }

        public VoidRuleHandlerBuilder<T> enforce(Call1<CheckMethod, A> policyCheckSupplier) {
            return new VoidRuleHandlerBuilder<>(registry, clazz, methodKey, policyCheckSupplier.asVarargCall());
        }
    }

    public static class VoidMethodRuleBuilder2<T, A, B> extends VoidMethodRuleBuilder1<T, A> {
        public VoidMethodRuleBuilder2(InternalInstrumentationRegistry registry, Class<? extends T> clazz, MethodKey methodKey) {
            super(registry, clazz, methodKey);
        }

        public VoidRuleHandlerBuilder<T> enforce(Call2<CheckMethod, A, B> policyCheckSupplier) {
            return new VoidRuleHandlerBuilder<>(registry, clazz, methodKey, policyCheckSupplier.asVarargCall());
        }
    }

    public static class VoidMethodRuleBuilder3<T, A, B, C> extends VoidMethodRuleBuilder2<T, A, B> {

        public VoidMethodRuleBuilder3(InternalInstrumentationRegistry registry, Class<? extends T> clazz, MethodKey methodKey) {
            super(registry, clazz, methodKey);
        }

        public VoidRuleHandlerBuilder<T> enforce(Call3<CheckMethod, A, B, C> policyCheckSupplier) {
            return new VoidRuleHandlerBuilder<>(registry, clazz, methodKey, policyCheckSupplier.asVarargCall());
        }
    }

    public static class VoidMethodRuleBuilder4<T, A, B, C, D> extends VoidMethodRuleBuilder3<T, A, B, C> {
        public VoidMethodRuleBuilder4(InternalInstrumentationRegistry registry, Class<? extends T> clazz, MethodKey methodKey) {
            super(registry, clazz, methodKey);
        }

        public VoidRuleHandlerBuilder<T> enforce(Call4<CheckMethod, A, B, C, D> policyCheckSupplier) {
            return new VoidRuleHandlerBuilder<>(registry, clazz, methodKey, policyCheckSupplier.asVarargCall());
        }
    }

    public static class VoidMethodRuleBuilder5<T, A, B, C, D, E> extends VoidMethodRuleBuilder4<T, A, B, C, D> {
        public VoidMethodRuleBuilder5(InternalInstrumentationRegistry registry, Class<? extends T> clazz, MethodKey methodKey) {
            super(registry, clazz, methodKey);
        }

        public VoidRuleHandlerBuilder<T> enforce(Call5<CheckMethod, A, B, C, D, E> policyCheckSupplier) {
            return new VoidRuleHandlerBuilder<>(registry, clazz, methodKey, policyCheckSupplier.asVarargCall());
        }
    }

    public static class VoidMethodRuleBuilder6<T, A, B, C, D, E, F> extends VoidMethodRuleBuilder5<T, A, B, C, D, E> {
        public VoidMethodRuleBuilder6(InternalInstrumentationRegistry registry, Class<? extends T> clazz, MethodKey methodKey) {
            super(registry, clazz, methodKey);
        }

        public VoidRuleHandlerBuilder<T> enforce(Call6<CheckMethod, A, B, C, D, E, F> policyCheckSupplier) {
            return new VoidRuleHandlerBuilder<>(registry, clazz, methodKey, policyCheckSupplier.asVarargCall());
        }
    }
}
