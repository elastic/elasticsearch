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

/**
 * Builder for creating entitlement rules for methods that return values.
 * <p>
 * This class is part of a fluent API for defining entitlement rules. It provides
 * methods to specify entitlement checks that should be enforced when the target
 * method is invoked.
 *
 * @param <T> the type of the class containing the method
 * @param <R> the return type of the method
 */
public class MethodRuleBuilder<T, R> {
    protected final InternalInstrumentationRegistry registry;
    protected final MethodKey methodKey;
    protected final Class<? extends T> clazz;

    public MethodRuleBuilder(InternalInstrumentationRegistry registry, Class<? extends T> clazz, MethodKey methodKey) {
        this.registry = registry;
        this.clazz = clazz;
        this.methodKey = methodKey;
    }

    /**
     * Specifies the entitlement check to enforce for a method with no parameters.
     *
     * @param policyCheckSupplier a supplier that provides the check method
     * @return a rule handler builder for further configuration
     */
    public RuleHandlerBuilder<T, R> enforce(Call0<CheckMethod> policyCheckSupplier) {
        return new RuleHandlerBuilder<>(registry, clazz, methodKey, policyCheckSupplier.asVarargCall());
    }

    /**
     * Builder for methods with one parameter.
     *
     * @param <T> the type of the class containing the method
     * @param <R> the return type of the method
     * @param <A> the type of the first parameter
     */
    public static class MethodRuleBuilder1<T, R, A> extends MethodRuleBuilder<T, R> {

        public MethodRuleBuilder1(InternalInstrumentationRegistry registry, Class<? extends T> clazz, MethodKey methodKey) {
            super(registry, clazz, methodKey);
        }

        /**
         * Specifies the entitlement check to enforce for a method with one parameter.
         *
         * @param policyCheckSupplier a supplier that provides the check method based on the method argument
         * @return a rule handler builder for further configuration
         */
        public RuleHandlerBuilder<T, R> enforce(Call1<CheckMethod, A> policyCheckSupplier) {
            return new RuleHandlerBuilder<>(registry, clazz, methodKey, policyCheckSupplier.asVarargCall());
        }
    }

    /**
     * Builder for methods with two parameters.
     *
     * @param <T> the type of the class containing the method
     * @param <R> the return type of the method
     * @param <A> the type of the first parameter
     * @param <B> the type of the second parameter
     */
    public static class MethodRuleBuilder2<T, R, A, B> extends MethodRuleBuilder1<T, R, A> {
        public MethodRuleBuilder2(InternalInstrumentationRegistry registry, Class<? extends T> clazz, MethodKey methodKey) {
            super(registry, clazz, methodKey);
        }

        /**
         * Specifies the entitlement check to enforce for a method with two parameters.
         *
         * @param policyCheckSupplier a supplier that provides the check method based on the method arguments
         * @return a rule handler builder for further configuration
         */
        public RuleHandlerBuilder<T, R> enforce(Call2<CheckMethod, A, B> policyCheckSupplier) {
            return new RuleHandlerBuilder<>(registry, clazz, methodKey, policyCheckSupplier.asVarargCall());
        }
    }

    /**
     * Builder for methods with three parameters.
     *
     * @param <T> the type of the class containing the method
     * @param <R> the return type of the method
     * @param <A> the type of the first parameter
     * @param <B> the type of the second parameter
     * @param <C> the type of the third parameter
     */
    public static class MethodRuleBuilder3<T, R, A, B, C> extends MethodRuleBuilder2<T, R, A, B> {

        public MethodRuleBuilder3(InternalInstrumentationRegistry registry, Class<? extends T> clazz, MethodKey methodKey) {
            super(registry, clazz, methodKey);
        }

        /**
         * Specifies the entitlement check to enforce for a method with three parameters.
         *
         * @param policyCheckSupplier a supplier that provides the check method based on the method arguments
         * @return a rule handler builder for further configuration
         */
        public RuleHandlerBuilder<T, R> enforce(Call3<CheckMethod, A, B, C> policyCheckSupplier) {
            return new RuleHandlerBuilder<>(registry, clazz, methodKey, policyCheckSupplier.asVarargCall());
        }
    }

    /**
     * Builder for methods with four parameters.
     *
     * @param <T> the type of the class containing the method
     * @param <R> the return type of the method
     * @param <A> the type of the first parameter
     * @param <B> the type of the second parameter
     * @param <C> the type of the third parameter
     * @param <D> the type of the fourth parameter
     */
    public static class MethodRuleBuilder4<T, R, A, B, C, D> extends MethodRuleBuilder3<T, R, A, B, C> {
        public MethodRuleBuilder4(InternalInstrumentationRegistry registry, Class<? extends T> clazz, MethodKey methodKey) {
            super(registry, clazz, methodKey);
        }

        /**
         * Specifies the entitlement check to enforce for a method with four parameters.
         *
         * @param policyCheckSupplier a supplier that provides the check method based on the method arguments
         * @return a rule handler builder for further configuration
         */
        public RuleHandlerBuilder<T, R> enforce(Call4<CheckMethod, A, B, C, D> policyCheckSupplier) {
            return new RuleHandlerBuilder<>(registry, clazz, methodKey, policyCheckSupplier.asVarargCall());
        }
    }

    /**
     * Builder for methods with five parameters.
     *
     * @param <T> the type of the class containing the method
     * @param <R> the return type of the method
     * @param <A> the type of the first parameter
     * @param <B> the type of the second parameter
     * @param <C> the type of the third parameter
     * @param <D> the type of the fourth parameter
     * @param <E> the type of the fifth parameter
     */
    public static class MethodRuleBuilder5<T, R, A, B, C, D, E> extends MethodRuleBuilder4<T, R, A, B, C, D> {
        public MethodRuleBuilder5(InternalInstrumentationRegistry registry, Class<? extends T> clazz, MethodKey methodKey) {
            super(registry, clazz, methodKey);
        }

        /**
         * Specifies the entitlement check to enforce for a method with five parameters.
         *
         * @param policyCheckSupplier a supplier that provides the check method based on the method arguments
         * @return a rule handler builder for further configuration
         */
        public RuleHandlerBuilder<T, R> enforce(Call5<CheckMethod, A, B, C, D, E> policyCheckSupplier) {
            return new RuleHandlerBuilder<>(registry, clazz, methodKey, policyCheckSupplier.asVarargCall());
        }
    }

    /**
     * Builder for methods with six parameters.
     *
     * @param <T> the type of the class containing the method
     * @param <R> the return type of the method
     * @param <A> the type of the first parameter
     * @param <B> the type of the second parameter
     * @param <C> the type of the third parameter
     * @param <D> the type of the fourth parameter
     * @param <E> the type of the fifth parameter
     * @param <F> the type of the sixth parameter
     */
    public static class MethodRuleBuilder6<T, R, A, B, C, D, E, F> extends MethodRuleBuilder5<T, R, A, B, C, D, E> {
        public MethodRuleBuilder6(InternalInstrumentationRegistry registry, Class<? extends T> clazz, MethodKey methodKey) {
            super(registry, clazz, methodKey);
        }

        /**
         * Specifies the entitlement check to enforce for a method with six parameters.
         *
         * @param policyCheckSupplier a supplier that provides the check method based on the method arguments
         * @return a rule handler builder for further configuration
         */
        public RuleHandlerBuilder<T, R> enforce(Call6<CheckMethod, A, B, C, D, E, F> policyCheckSupplier) {
            return new RuleHandlerBuilder<>(registry, clazz, methodKey, policyCheckSupplier.asVarargCall());
        }
    }
}
