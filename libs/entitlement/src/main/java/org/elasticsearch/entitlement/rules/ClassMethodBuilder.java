/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.rules;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.elasticsearch.entitlement.rules.function.Call0;
import org.elasticsearch.entitlement.rules.function.Call1;
import org.elasticsearch.entitlement.rules.function.Call2;
import org.elasticsearch.entitlement.rules.function.Call3;
import org.elasticsearch.entitlement.rules.function.Call4;
import org.elasticsearch.entitlement.rules.function.Call5;
import org.elasticsearch.entitlement.rules.function.Call6;
import org.elasticsearch.entitlement.rules.function.VoidCall0;
import org.elasticsearch.entitlement.rules.function.VoidCall1;
import org.elasticsearch.entitlement.rules.function.VoidCall2;
import org.elasticsearch.entitlement.rules.function.VoidCall3;
import org.elasticsearch.entitlement.rules.function.VoidCall4;
import org.elasticsearch.entitlement.rules.function.VoidCall5;
import org.elasticsearch.entitlement.rules.function.VoidCall6;
import org.elasticsearch.entitlement.runtime.registry.InternalInstrumentationRegistry;

import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * Builder for selecting and configuring methods to be instrumented with entitlement checks.
 * <p>
 * This class provides a fluent API for specifying which methods on a class should have
 * entitlement rules applied. It supports:
 * <ul>
 *   <li>Instance methods via {@code calling()} methods</li>
 *   <li>Static methods via {@code callingStatic()} methods</li>
 *   <li>Void methods via {@code callingVoid*()} methods</li>
 *   <li>Non-public constructors via {@code protectedCtor()} methods</li>
 * </ul>
 * <p>
 * Methods are identified using method references, which are resolved at build time to
 * determine the actual method signatures. Type parameters can be specified using either
 * {@link Class} objects or {@link TypeToken} instances for generic types.
 * <p>
 * Example usage:
 * <pre>{@code
 * builder.on(Path.class)
 *     .calling(Path::toFile)
 *     .enforce(path -> Policies.fileRead(path))
 *     .elseThrowNotEntitled();
 * }</pre>
 *
 * @param <T> the type of the class whose methods are being configured
 */
public class ClassMethodBuilder<T> {
    private final Class<? extends T> clazz;
    private final InternalInstrumentationRegistry registry;

    public ClassMethodBuilder(InternalInstrumentationRegistry registry, Class<? extends T> clazz) {
        this.clazz = clazz;
        this.registry = registry;
    }

    /**
     * Selects a no-argument constructor for instrumentation.
     *
     * @return a builder for configuring the constructor rule
     */
    public VoidMethodRuleBuilder<T> protectedCtor() {
        MethodKey methodKey = getConstructorMethodKey();
        return new VoidMethodRuleBuilder<>(registry, clazz, methodKey);
    }

    /**
     * Selects a constructor with one parameter for instrumentation.
     *
     * @param <A> the type of the constructor parameter
     * @param arg0 the class of the first constructor parameter
     * @return a builder for configuring the constructor rule
     */
    public <A> VoidMethodRuleBuilder.VoidMethodRuleBuilder1<T, A> protectedCtor(Class<A> arg0) {
        MethodKey methodKey = getConstructorMethodKey(arg0);
        return new VoidMethodRuleBuilder.VoidMethodRuleBuilder1<>(registry, clazz, methodKey);
    }

    /**
     * Selects a constructor with two parameters for instrumentation.
     *
     * @param <A> the type of the first constructor parameter
     * @param <B> the type of the second constructor parameter
     * @param arg0 the class of the first constructor parameter
     * @param arg1 the class of the second constructor parameter
     * @return a builder for configuring the constructor rule
     */
    public <A, B> VoidMethodRuleBuilder.VoidMethodRuleBuilder2<T, A, B> protectedCtor(Class<A> arg0, Class<B> arg1) {
        MethodKey methodKey = getConstructorMethodKey(arg0, arg1);
        return new VoidMethodRuleBuilder.VoidMethodRuleBuilder2<>(registry, clazz, methodKey);
    }

    /**
     * Selects a void instance method with no parameters for instrumentation using a method reference.
     *
     * @param call a method reference to the void method (e.g., {@code File::delete})
     * @return a builder for configuring the method rule
     */
    public VoidMethodRuleBuilder.VoidMethodRuleBuilder1<T, T> callingVoid(VoidCall1<T> call) {
        MethodKey methodKey = resolveMethodReference(clazz, call);
        return new VoidMethodRuleBuilder.VoidMethodRuleBuilder1<>(registry, clazz, methodKey);
    }

    /**
     * Selects an instance method with no parameters that returns a value, using a method reference.
     *
     * @param <R> the return type of the method
     * @param call a method reference to the method (e.g., {@code File::exists})
     * @return a builder for configuring the method rule
     */
    public <R> MethodRuleBuilder.MethodRuleBuilder1<T, R, T> calling(Call1<R, T> call) {
        MethodKey methodKey = resolveMethodReference(clazz, call);
        return new MethodRuleBuilder.MethodRuleBuilder1<>(registry, clazz, methodKey);
    }

    /**
     * Selects a void instance method with one parameter for instrumentation using a method reference.
     *
     * @param <A> the type of the method parameter
     * @param call a method reference to the void method
     * @param arg0 the class of the method parameter
     * @return a builder for configuring the method rule
     */
    public <A> VoidMethodRuleBuilder.VoidMethodRuleBuilder2<T, T, A> callingVoid(VoidCall2<T, A> call, Class<A> arg0) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0);
        return new VoidMethodRuleBuilder.VoidMethodRuleBuilder2<>(registry, clazz, methodKey);
    }

    /**
     * Selects an instance method with one parameter that returns a value, using a method reference.
     *
     * @param <R> the return type of the method
     * @param <A> the type of the method parameter
     * @param call a method reference to the method
     * @param arg0 the class of the method parameter
     * @return a builder for configuring the method rule
     */
    public <R, A> MethodRuleBuilder.MethodRuleBuilder2<T, R, T, A> calling(Call2<R, T, A> call, Class<A> arg0) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0);
        return new MethodRuleBuilder.MethodRuleBuilder2<>(registry, clazz, methodKey);
    }

    /**
     * Selects a void instance method with one generic parameter for instrumentation using a method reference.
     *
     * @param <A> the type of the method parameter
     * @param call a method reference to the void method
     * @param arg0 a type token representing the generic type of the method parameter
     * @return a builder for configuring the method rule
     */
    public <A> VoidMethodRuleBuilder.VoidMethodRuleBuilder2<T, T, A> callingVoid(VoidCall2<T, A> call, TypeToken<A> arg0) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0.getRawType());
        return new VoidMethodRuleBuilder.VoidMethodRuleBuilder2<>(registry, clazz, methodKey);
    }

    /**
     * Selects an instance method with one generic parameter that returns a value, using a method reference.
     *
     * @param <R> the return type of the method
     * @param <A> the type of the method parameter
     * @param call a method reference to the method
     * @param arg0 a type token representing the generic type of the method parameter
     * @return a builder for configuring the method rule
     */
    public <R, A> MethodRuleBuilder.MethodRuleBuilder2<T, R, T, A> calling(Call2<R, T, A> call, TypeToken<A> arg0) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0.getRawType());
        return new MethodRuleBuilder.MethodRuleBuilder2<>(registry, clazz, methodKey);
    }

    /**
     * Selects a void instance method with two parameters for instrumentation using a method reference.
     *
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param call a method reference to the void method
     * @param arg0 the class of the first method parameter
     * @param arg1 the class of the second method parameter
     * @return a builder for configuring the method rule
     */
    public <A, B> VoidMethodRuleBuilder.VoidMethodRuleBuilder3<T, T, A, B> callingVoid(
        VoidCall3<T, A, B> call,
        Class<A> arg0,
        Class<B> arg1
    ) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0, arg1);
        return new VoidMethodRuleBuilder.VoidMethodRuleBuilder3<>(registry, clazz, methodKey);
    }

    /**
     * Selects an instance method with two parameters that returns a value, using a method reference.
     *
     * @param <R> the return type of the method
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param call a method reference to the method
     * @param arg0 the class of the first method parameter
     * @param arg1 the class of the second method parameter
     * @return a builder for configuring the method rule
     */
    public <R, A, B> MethodRuleBuilder.MethodRuleBuilder3<T, R, T, A, B> calling(Call3<R, T, A, B> call, Class<A> arg0, Class<B> arg1) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0, arg1);
        return new MethodRuleBuilder.MethodRuleBuilder3<>(registry, clazz, methodKey);
    }

    /**
     * Selects a void instance method with two generic parameters for instrumentation using a method reference.
     *
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param call a method reference to the void method
     * @param arg0 a type token representing the generic type of the first method parameter
     * @param arg1 a type token representing the generic type of the second method parameter
     * @return a builder for configuring the method rule
     */
    public <A, B> VoidMethodRuleBuilder.VoidMethodRuleBuilder3<T, T, A, B> callingVoid(
        VoidCall3<T, A, B> call,
        TypeToken<A> arg0,
        TypeToken<B> arg1
    ) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0.getRawType(), arg1.getRawType());
        return new VoidMethodRuleBuilder.VoidMethodRuleBuilder3<>(registry, clazz, methodKey);
    }

    /**
     * Selects an instance method with two generic parameters that returns a value, using a method reference.
     *
     * @param <R> the return type of the method
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param call a method reference to the method
     * @param arg0 a type token representing the generic type of the first method parameter
     * @param arg1 a type token representing the generic type of the second method parameter
     * @return a builder for configuring the method rule
     */
    public <R, A, B> MethodRuleBuilder.MethodRuleBuilder3<T, R, T, A, B> calling(
        Call3<R, T, A, B> call,
        TypeToken<A> arg0,
        TypeToken<B> arg1
    ) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0.getRawType(), arg1.getRawType());
        return new MethodRuleBuilder.MethodRuleBuilder3<>(registry, clazz, methodKey);
    }

    /**
     * Selects a void instance method with three parameters for instrumentation using a method reference.
     *
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param <C> the type of the third method parameter
     * @param call a method reference to the void method
     * @param arg0 the class of the first method parameter
     * @param arg1 the class of the second method parameter
     * @param arg2 the class of the third method parameter
     * @return a builder for configuring the method rule
     */
    public <A, B, C> VoidMethodRuleBuilder.VoidMethodRuleBuilder4<T, T, A, B, C> callingVoid(
        VoidCall4<T, A, B, C> call,
        Class<A> arg0,
        Class<B> arg1,
        Class<C> arg2
    ) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0, arg1, arg2);
        return new VoidMethodRuleBuilder.VoidMethodRuleBuilder4<>(registry, clazz, methodKey);
    }

    /**
     * Selects an instance method with three parameters that returns a value, using a method reference.
     *
     * @param <R> the return type of the method
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param <C> the type of the third method parameter
     * @param call a method reference to the method
     * @param arg0 the class of the first method parameter
     * @param arg1 the class of the second method parameter
     * @param arg2 the class of the third method parameter
     * @return a builder for configuring the method rule
     */
    public <R, A, B, C> MethodRuleBuilder.MethodRuleBuilder4<T, R, T, A, B, C> calling(
        Call4<R, T, A, B, C> call,
        Class<A> arg0,
        Class<B> arg1,
        Class<C> arg2
    ) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0, arg1, arg2);
        return new MethodRuleBuilder.MethodRuleBuilder4<>(registry, clazz, methodKey);
    }

    /**
     * Selects a void instance method with three generic parameters for instrumentation using a method reference.
     *
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param <C> the type of the third method parameter
     * @param call a method reference to the void method
     * @param arg0 a type token representing the generic type of the first method parameter
     * @param arg1 a type token representing the generic type of the second method parameter
     * @param arg2 a type token representing the generic type of the third method parameter
     * @return a builder for configuring the method rule
     */
    public <A, B, C> VoidMethodRuleBuilder.VoidMethodRuleBuilder4<T, T, A, B, C> callingVoid(
        VoidCall4<T, A, B, C> call,
        TypeToken<A> arg0,
        TypeToken<B> arg1,
        TypeToken<C> arg2
    ) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0.getRawType(), arg1.getRawType(), arg2.getRawType());
        return new VoidMethodRuleBuilder.VoidMethodRuleBuilder4<>(registry, clazz, methodKey);
    }

    /**
     * Selects an instance method with three generic parameters that returns a value, using a method reference.
     *
     * @param <R> the return type of the method
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param <C> the type of the third method parameter
     * @param call a method reference to the method
     * @param arg0 a type token representing the generic type of the first method parameter
     * @param arg1 a type token representing the generic type of the second method parameter
     * @param arg2 a type token representing the generic type of the third method parameter
     * @return a builder for configuring the method rule
     */
    public <R, A, B, C> MethodRuleBuilder.MethodRuleBuilder4<T, R, T, A, B, C> calling(
        Call4<R, T, A, B, C> call,
        TypeToken<A> arg0,
        TypeToken<B> arg1,
        TypeToken<C> arg2
    ) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0.getRawType(), arg1.getRawType(), arg2.getRawType());
        return new MethodRuleBuilder.MethodRuleBuilder4<>(registry, clazz, methodKey);
    }

    /**
     * Selects a void instance method with four parameters for instrumentation using a method reference.
     *
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param <C> the type of the third method parameter
     * @param <D> the type of the fourth method parameter
     * @param call a method reference to the void method
     * @param arg0 the class of the first method parameter
     * @param arg1 the class of the second method parameter
     * @param arg2 the class of the third method parameter
     * @param arg3 the class of the fourth method parameter
     * @return a builder for configuring the method rule
     */
    public <A, B, C, D> VoidMethodRuleBuilder.VoidMethodRuleBuilder5<T, T, A, B, C, D> callingVoid(
        VoidCall5<T, A, B, C, D> call,
        Class<A> arg0,
        Class<B> arg1,
        Class<C> arg2,
        Class<D> arg3
    ) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0, arg1, arg2, arg3);
        return new VoidMethodRuleBuilder.VoidMethodRuleBuilder5<>(registry, clazz, methodKey);
    }

    /**
     * Selects an instance method with four parameters that returns a value, using a method reference.
     *
     * @param <R> the return type of the method
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param <C> the type of the third method parameter
     * @param <D> the type of the fourth method parameter
     * @param call a method reference to the method
     * @param arg0 the class of the first method parameter
     * @param arg1 the class of the second method parameter
     * @param arg2 the class of the third method parameter
     * @param arg3 the class of the fourth method parameter
     * @return a builder for configuring the method rule
     */
    public <R, A, B, C, D> MethodRuleBuilder.MethodRuleBuilder5<T, R, T, A, B, C, D> calling(
        Call5<R, T, A, B, C, D> call,
        Class<A> arg0,
        Class<B> arg1,
        Class<C> arg2,
        Class<D> arg3
    ) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0, arg1, arg2, arg3);
        return new MethodRuleBuilder.MethodRuleBuilder5<>(registry, clazz, methodKey);
    }

    /**
     * Selects a void instance method with four generic parameters for instrumentation using a method reference.
     *
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param <C> the type of the third method parameter
     * @param <D> the type of the fourth method parameter
     * @param call a method reference to the void method
     * @param arg0 a type token representing the generic type of the first method parameter
     * @param arg1 a type token representing the generic type of the second method parameter
     * @param arg2 a type token representing the generic type of the third method parameter
     * @param arg3 a type token representing the generic type of the fourth method parameter
     * @return a builder for configuring the method rule
     */
    public <A, B, C, D> VoidMethodRuleBuilder.VoidMethodRuleBuilder5<T, T, A, B, C, D> callingVoid(
        VoidCall5<T, A, B, C, D> call,
        TypeToken<A> arg0,
        TypeToken<B> arg1,
        TypeToken<C> arg2,
        TypeToken<D> arg3
    ) {
        MethodKey methodKey = resolveMethodReference(
            clazz,
            call,
            arg0.getRawType(),
            arg1.getRawType(),
            arg2.getRawType(),
            arg3.getRawType()
        );
        return new VoidMethodRuleBuilder.VoidMethodRuleBuilder5<>(registry, clazz, methodKey);
    }

    /**
     * Selects an instance method with four generic parameters that returns a value, using a method reference.
     *
     * @param <R> the return type of the method
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param <C> the type of the third method parameter
     * @param <D> the type of the fourth method parameter
     * @param call a method reference to the method
     * @param arg0 a type token representing the generic type of the first method parameter
     * @param arg1 a type token representing the generic type of the second method parameter
     * @param arg2 a type token representing the generic type of the third method parameter
     * @param arg3 a type token representing the generic type of the fourth method parameter
     * @return a builder for configuring the method rule
     */
    public <R, A, B, C, D> MethodRuleBuilder.MethodRuleBuilder5<T, R, T, A, B, C, D> calling(
        Call5<R, T, A, B, C, D> call,
        TypeToken<A> arg0,
        TypeToken<B> arg1,
        TypeToken<C> arg2,
        TypeToken<D> arg3
    ) {
        MethodKey methodKey = resolveMethodReference(
            clazz,
            call,
            arg0.getRawType(),
            arg1.getRawType(),
            arg2.getRawType(),
            arg3.getRawType()
        );
        return new MethodRuleBuilder.MethodRuleBuilder5<>(registry, clazz, methodKey);
    }

    /**
     * Selects a void instance method with five parameters for instrumentation using a method reference.
     *
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param <C> the type of the third method parameter
     * @param <D> the type of the fourth method parameter
     * @param <E> the type of the fifth method parameter
     * @param call a method reference to the void method
     * @param arg0 the class of the first method parameter
     * @param arg1 the class of the second method parameter
     * @param arg2 the class of the third method parameter
     * @param arg3 the class of the fourth method parameter
     * @param arg4 the class of the fifth method parameter
     * @return a builder for configuring the method rule
     */
    public <A, B, C, D, E> VoidMethodRuleBuilder.VoidMethodRuleBuilder6<T, T, A, B, C, D, E> callingVoid(
        VoidCall6<T, A, B, C, D, E> call,
        Class<A> arg0,
        Class<B> arg1,
        Class<C> arg2,
        Class<D> arg3,
        Class<E> arg4
    ) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0, arg1, arg2, arg3, arg4);
        return new VoidMethodRuleBuilder.VoidMethodRuleBuilder6<>(registry, clazz, methodKey);
    }

    /**
     * Selects an instance method with five parameters that returns a value, using a method reference.
     *
     * @param <R> the return type of the method
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param <C> the type of the third method parameter
     * @param <D> the type of the fourth method parameter
     * @param <E> the type of the fifth method parameter
     * @param call a method reference to the method
     * @param arg0 the class of the first method parameter
     * @param arg1 the class of the second method parameter
     * @param arg2 the class of the third method parameter
     * @param arg3 the class of the fourth method parameter
     * @param arg4 the class of the fifth method parameter
     * @return a builder for configuring the method rule
     */
    public <R, A, B, C, D, E> MethodRuleBuilder.MethodRuleBuilder6<T, R, T, A, B, C, D, E> calling(
        Call6<R, T, A, B, C, D, E> call,
        Class<A> arg0,
        Class<B> arg1,
        Class<C> arg2,
        Class<D> arg3,
        Class<E> arg4
    ) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0, arg1, arg2, arg3, arg4);
        return new MethodRuleBuilder.MethodRuleBuilder6<>(registry, clazz, methodKey);
    }

    /**
     * Selects a void instance method with five generic parameters for instrumentation using a method reference.
     *
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param <C> the type of the third method parameter
     * @param <D> the type of the fourth method parameter
     * @param <E> the type of the fifth method parameter
     * @param call a method reference to the void method
     * @param arg0 a type token representing the generic type of the first method parameter
     * @param arg1 a type token representing the generic type of the second method parameter
     * @param arg2 a type token representing the generic type of the third method parameter
     * @param arg3 a type token representing the generic type of the fourth method parameter
     * @param arg4 a type token representing the generic type of the fifth method parameter
     * @return a builder for configuring the method rule
     */
    public <A, B, C, D, E> VoidMethodRuleBuilder.VoidMethodRuleBuilder6<T, T, A, B, C, D, E> callingVoid(
        VoidCall6<T, A, B, C, D, E> call,
        TypeToken<A> arg0,
        TypeToken<B> arg1,
        TypeToken<C> arg2,
        TypeToken<D> arg3,
        TypeToken<E> arg4
    ) {
        MethodKey methodKey = resolveMethodReference(
            clazz,
            call,
            arg0.getRawType(),
            arg1.getRawType(),
            arg2.getRawType(),
            arg3.getRawType(),
            arg4.getRawType()
        );
        return new VoidMethodRuleBuilder.VoidMethodRuleBuilder6<>(registry, clazz, methodKey);
    }

    /**
     * Selects an instance method with five generic parameters that returns a value, using a method reference.
     *
     * @param <R> the return type of the method
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param <C> the type of the third method parameter
     * @param <D> the type of the fourth method parameter
     * @param <E> the type of the fifth method parameter
     * @param call a method reference to the method
     * @param arg0 a type token representing the generic type of the first method parameter
     * @param arg1 a type token representing the generic type of the second method parameter
     * @param arg2 a type token representing the generic type of the third method parameter
     * @param arg3 a type token representing the generic type of the fourth method parameter
     * @param arg4 a type token representing the generic type of the fifth method parameter
     * @return a builder for configuring the method rule
     */
    public <R, A, B, C, D, E> MethodRuleBuilder.MethodRuleBuilder6<T, R, T, A, B, C, D, E> calling(
        Call6<R, T, A, B, C, D, E> call,
        TypeToken<A> arg0,
        TypeToken<B> arg1,
        TypeToken<C> arg2,
        TypeToken<D> arg3,
        TypeToken<E> arg4
    ) {
        MethodKey methodKey = resolveMethodReference(
            clazz,
            call,
            arg0.getRawType(),
            arg1.getRawType(),
            arg2.getRawType(),
            arg3.getRawType(),
            arg4.getRawType()
        );
        return new MethodRuleBuilder.MethodRuleBuilder6<>(registry, clazz, methodKey);
    }

    /**
     * Selects a static void method with no parameters for instrumentation using a method reference.
     *
     * @param call a method reference to the static void method
     * @return a builder for configuring the method rule
     */
    public VoidMethodRuleBuilder<T> callingVoidStatic(VoidCall0 call) {
        MethodKey methodKey = resolveMethodReference(clazz, call);
        return new VoidMethodRuleBuilder<>(registry, clazz, methodKey);
    }

    /**
     * Selects a static method with no parameters that returns a value, using a method reference.
     *
     * @param <R> the return type of the method
     * @param call a method reference to the static method
     * @return a builder for configuring the method rule
     */
    public <R> MethodRuleBuilder<T, R> callingStatic(Call0<R> call) {
        MethodKey methodKey = resolveMethodReference(clazz, call);
        return new MethodRuleBuilder<>(registry, clazz, methodKey);
    }

    /**
     * Selects a static void method with one parameter for instrumentation using a method reference.
     *
     * @param <A> the type of the method parameter
     * @param call a method reference to the static void method
     * @param arg0 the class of the method parameter
     * @return a builder for configuring the method rule
     */
    public <A> VoidMethodRuleBuilder.VoidMethodRuleBuilder1<T, A> callingVoidStatic(VoidCall1<A> call, Class<A> arg0) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0);
        return new VoidMethodRuleBuilder.VoidMethodRuleBuilder1<>(registry, clazz, methodKey);
    }

    /**
     * Selects a static method with one parameter that returns a value, using a method reference.
     *
     * @param <R> the return type of the method
     * @param <A> the type of the method parameter
     * @param call a method reference to the static method
     * @param arg0 the class of the method parameter
     * @return a builder for configuring the method rule
     */
    public <R, A> MethodRuleBuilder.MethodRuleBuilder1<T, R, A> callingStatic(Call1<R, A> call, Class<A> arg0) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0);
        return new MethodRuleBuilder.MethodRuleBuilder1<>(registry, clazz, methodKey);
    }

    /**
     * Selects a static void method with one generic parameter for instrumentation using a method reference.
     *
     * @param <A> the type of the method parameter
     * @param call a method reference to the static void method
     * @param arg0 a type token representing the generic type of the method parameter
     * @return a builder for configuring the method rule
     */
    public <A> VoidMethodRuleBuilder.VoidMethodRuleBuilder1<T, A> callingVoidStatic(VoidCall1<A> call, TypeToken<A> arg0) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0.getRawType());
        return new VoidMethodRuleBuilder.VoidMethodRuleBuilder1<>(registry, clazz, methodKey);
    }

    /**
     * Selects a static method with one generic parameter that returns a value, using a method reference.
     *
     * @param <R> the return type of the method
     * @param <A> the type of the method parameter
     * @param call a method reference to the static method
     * @param arg0 a type token representing the generic type of the method parameter
     * @return a builder for configuring the method rule
     */
    public <R, A> MethodRuleBuilder.MethodRuleBuilder1<T, R, A> callingStatic(Call1<R, A> call, TypeToken<A> arg0) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0.getRawType());
        return new MethodRuleBuilder.MethodRuleBuilder1<>(registry, clazz, methodKey);
    }

    /**
     * Selects a static void method with two parameters for instrumentation using a method reference.
     *
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param call a method reference to the static void method
     * @param arg0 the class of the first method parameter
     * @param arg1 the class of the second method parameter
     * @return a builder for configuring the method rule
     */
    public <A, B> VoidMethodRuleBuilder.VoidMethodRuleBuilder2<T, A, B> callingVoidStatic(
        VoidCall2<A, B> call,
        Class<A> arg0,
        Class<B> arg1
    ) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0, arg1);
        return new VoidMethodRuleBuilder.VoidMethodRuleBuilder2<>(registry, clazz, methodKey);
    }

    /**
     * Selects a static method with two parameters that returns a value, using a method reference.
     *
     * @param <R> the return type of the method
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param call a method reference to the static method
     * @param arg0 the class of the first method parameter
     * @param arg1 the class of the second method parameter
     * @return a builder for configuring the method rule
     */
    public <R, A, B> MethodRuleBuilder.MethodRuleBuilder2<T, R, A, B> callingStatic(Call2<R, A, B> call, Class<A> arg0, Class<B> arg1) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0, arg1);
        return new MethodRuleBuilder.MethodRuleBuilder2<>(registry, clazz, methodKey);
    }

    /**
     * Selects a static void method with two generic parameters for instrumentation using a method reference.
     *
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param call a method reference to the static void method
     * @param arg0 a type token representing the generic type of the first method parameter
     * @param arg1 a type token representing the generic type of the second method parameter
     * @return a builder for configuring the method rule
     */
    public <A, B> VoidMethodRuleBuilder.VoidMethodRuleBuilder2<T, A, B> callingVoidStatic(
        VoidCall2<A, B> call,
        TypeToken<A> arg0,
        TypeToken<B> arg1
    ) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0.getRawType(), arg1.getRawType());
        return new VoidMethodRuleBuilder.VoidMethodRuleBuilder2<>(registry, clazz, methodKey);
    }

    /**
     * Selects a static method with two generic parameters that returns a value, using a method reference.
     *
     * @param <R> the return type of the method
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param call a method reference to the static method
     * @param arg0 a type token representing the generic type of the first method parameter
     * @param arg1 a type token representing the generic type of the second method parameter
     * @return a builder for configuring the method rule
     */
    public <R, A, B> MethodRuleBuilder.MethodRuleBuilder2<T, R, A, B> callingStatic(
        Call2<R, A, B> call,
        TypeToken<A> arg0,
        TypeToken<B> arg1
    ) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0.getRawType(), arg1.getRawType());
        return new MethodRuleBuilder.MethodRuleBuilder2<>(registry, clazz, methodKey);
    }

    /**
     * Selects a static void method with three parameters for instrumentation using a method reference.
     *
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param <C> the type of the third method parameter
     * @param call a method reference to the static void method
     * @param arg0 the class of the first method parameter
     * @param arg1 the class of the second method parameter
     * @param arg2 the class of the third method parameter
     * @return a builder for configuring the method rule
     */
    public <A, B, C> VoidMethodRuleBuilder.VoidMethodRuleBuilder3<T, A, B, C> callingVoidStatic(
        VoidCall3<A, B, C> call,
        Class<A> arg0,
        Class<B> arg1,
        Class<C> arg2
    ) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0, arg1, arg2);
        return new VoidMethodRuleBuilder.VoidMethodRuleBuilder3<>(registry, clazz, methodKey);
    }

    /**
     * Selects a static method with three parameters that returns a value, using a method reference.
     *
     * @param <R> the return type of the method
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param <C> the type of the third method parameter
     * @param call a method reference to the static method
     * @param arg0 the class of the first method parameter
     * @param arg1 the class of the second method parameter
     * @param arg2 the class of the third method parameter
     * @return a builder for configuring the method rule
     */
    public <R, A, B, C> MethodRuleBuilder.MethodRuleBuilder3<T, R, A, B, C> callingStatic(
        Call3<R, A, B, C> call,
        Class<A> arg0,
        Class<B> arg1,
        Class<C> arg2
    ) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0, arg1, arg2);
        return new MethodRuleBuilder.MethodRuleBuilder3<>(registry, clazz, methodKey);
    }

    /**
     * Selects a static void method with three generic parameters for instrumentation using a method reference.
     *
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param <C> the type of the third method parameter
     * @param call a method reference to the static void method
     * @param arg0 a type token representing the generic type of the first method parameter
     * @param arg1 a type token representing the generic type of the second method parameter
     * @param arg2 a type token representing the generic type of the third method parameter
     * @return a builder for configuring the method rule
     */
    public <A, B, C> VoidMethodRuleBuilder.VoidMethodRuleBuilder3<T, A, B, C> callingVoidStatic(
        VoidCall3<A, B, C> call,
        TypeToken<A> arg0,
        TypeToken<B> arg1,
        TypeToken<C> arg2
    ) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0.getRawType(), arg1.getRawType(), arg2.getRawType());
        return new VoidMethodRuleBuilder.VoidMethodRuleBuilder3<>(registry, clazz, methodKey);
    }

    /**
     * Selects a static method with three generic parameters that returns a value, using a method reference.
     *
     * @param <R> the return type of the method
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param <C> the type of the third method parameter
     * @param call a method reference to the static method
     * @param arg0 a type token representing the generic type of the first method parameter
     * @param arg1 a type token representing the generic type of the second method parameter
     * @param arg2 a type token representing the generic type of the third method parameter
     * @return a builder for configuring the method rule
     */
    public <R, A, B, C> MethodRuleBuilder.MethodRuleBuilder3<T, R, A, B, C> callingStatic(
        Call3<R, A, B, C> call,
        TypeToken<A> arg0,
        TypeToken<B> arg1,
        TypeToken<C> arg2
    ) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0.getRawType(), arg1.getRawType(), arg2.getRawType());
        return new MethodRuleBuilder.MethodRuleBuilder3<>(registry, clazz, methodKey);
    }

    /**
     * Selects a static void method with four parameters for instrumentation using a method reference.
     *
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param <C> the type of the third method parameter
     * @param <D> the type of the fourth method parameter
     * @param call a method reference to the static void method
     * @param arg0 the class of the first method parameter
     * @param arg1 the class of the second method parameter
     * @param arg2 the class of the third method parameter
     * @param arg3 the class of the fourth method parameter
     * @return a builder for configuring the method rule
     */
    public <A, B, C, D> VoidMethodRuleBuilder.VoidMethodRuleBuilder4<T, A, B, C, D> callingVoidStatic(
        VoidCall4<A, B, C, D> call,
        Class<A> arg0,
        Class<B> arg1,
        Class<C> arg2,
        Class<D> arg3
    ) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0, arg1, arg2, arg3);
        return new VoidMethodRuleBuilder.VoidMethodRuleBuilder4<>(registry, clazz, methodKey);
    }

    /**
     * Selects a static method with four parameters that returns a value, using a method reference.
     *
     * @param <R> the return type of the method
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param <C> the type of the third method parameter
     * @param <D> the type of the fourth method parameter
     * @param call a method reference to the static method
     * @param arg0 the class of the first method parameter
     * @param arg1 the class of the second method parameter
     * @param arg2 the class of the third method parameter
     * @param arg3 the class of the fourth method parameter
     * @return a builder for configuring the method rule
     */
    public <R, A, B, C, D> MethodRuleBuilder.MethodRuleBuilder4<T, R, A, B, C, D> callingStatic(
        Call4<R, A, B, C, D> call,
        Class<A> arg0,
        Class<B> arg1,
        Class<C> arg2,
        Class<D> arg3
    ) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0, arg1, arg2, arg3);
        return new MethodRuleBuilder.MethodRuleBuilder4<>(registry, clazz, methodKey);
    }

    /**
     * Selects a static void method with four generic parameters for instrumentation using a method reference.
     *
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param <C> the type of the third method parameter
     * @param <D> the type of the fourth method parameter
     * @param call a method reference to the static void method
     * @param arg0 a type token representing the generic type of the first method parameter
     * @param arg1 a type token representing the generic type of the second method parameter
     * @param arg2 a type token representing the generic type of the third method parameter
     * @param arg3 a type token representing the generic type of the fourth method parameter
     * @return a builder for configuring the method rule
     */
    public <A, B, C, D> VoidMethodRuleBuilder.VoidMethodRuleBuilder4<T, A, B, C, D> callingVoidStatic(
        VoidCall4<A, B, C, D> call,
        TypeToken<A> arg0,
        TypeToken<B> arg1,
        TypeToken<C> arg2,
        TypeToken<D> arg3
    ) {
        MethodKey methodKey = resolveMethodReference(
            clazz,
            call,
            arg0.getRawType(),
            arg1.getRawType(),
            arg2.getRawType(),
            arg3.getRawType()
        );
        return new VoidMethodRuleBuilder.VoidMethodRuleBuilder4<>(registry, clazz, methodKey);
    }

    /**
     * Selects a static method with four generic parameters that returns a value, using a method reference.
     *
     * @param <R> the return type of the method
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param <C> the type of the third method parameter
     * @param <D> the type of the fourth method parameter
     * @param call a method reference to the static method
     * @param arg0 a type token representing the generic type of the first method parameter
     * @param arg1 a type token representing the generic type of the second method parameter
     * @param arg2 a type token representing the generic type of the third method parameter
     * @param arg3 a type token representing the generic type of the fourth method parameter
     * @return a builder for configuring the method rule
     */
    public <R, A, B, C, D> MethodRuleBuilder.MethodRuleBuilder4<T, R, A, B, C, D> callingStatic(
        Call4<R, A, B, C, D> call,
        TypeToken<A> arg0,
        TypeToken<B> arg1,
        TypeToken<C> arg2,
        TypeToken<D> arg3
    ) {
        MethodKey methodKey = resolveMethodReference(
            clazz,
            call,
            arg0.getRawType(),
            arg1.getRawType(),
            arg2.getRawType(),
            arg3.getRawType()
        );
        return new MethodRuleBuilder.MethodRuleBuilder4<>(registry, clazz, methodKey);
    }

    /**
     * Selects a static void method with five parameters for instrumentation using a method reference.
     *
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param <C> the type of the third method parameter
     * @param <D> the type of the fourth method parameter
     * @param <E> the type of the fifth method parameter
     * @param call a method reference to the static void method
     * @param arg0 the class of the first method parameter
     * @param arg1 the class of the second method parameter
     * @param arg2 the class of the third method parameter
     * @param arg3 the class of the fourth method parameter
     * @param arg4 the class of the fifth method parameter
     * @return a builder for configuring the method rule
     */
    public <A, B, C, D, E> VoidMethodRuleBuilder.VoidMethodRuleBuilder5<T, A, B, C, D, E> callingVoidStatic(
        VoidCall5<A, B, C, D, E> call,
        Class<A> arg0,
        Class<B> arg1,
        Class<C> arg2,
        Class<D> arg3,
        Class<E> arg4
    ) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0, arg1, arg2, arg3, arg4);
        return new VoidMethodRuleBuilder.VoidMethodRuleBuilder5<>(registry, clazz, methodKey);
    }

    /**
     * Selects a static method with five parameters that returns a value, using a method reference.
     *
     * @param <R> the return type of the method
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param <C> the type of the third method parameter
     * @param <D> the type of the fourth method parameter
     * @param <E> the type of the fifth method parameter
     * @param call a method reference to the static method
     * @param arg0 the class of the first method parameter
     * @param arg1 the class of the second method parameter
     * @param arg2 the class of the third method parameter
     * @param arg3 the class of the fourth method parameter
     * @param arg4 the class of the fifth method parameter
     * @return a builder for configuring the method rule
     */
    public <R, A, B, C, D, E> MethodRuleBuilder.MethodRuleBuilder5<T, R, A, B, C, D, E> callingStatic(
        Call5<R, A, B, C, D, E> call,
        Class<A> arg0,
        Class<B> arg1,
        Class<C> arg2,
        Class<D> arg3,
        Class<E> arg4
    ) {
        MethodKey methodKey = resolveMethodReference(clazz, call, arg0, arg1, arg2, arg3, arg4);
        return new MethodRuleBuilder.MethodRuleBuilder5<>(registry, clazz, methodKey);
    }

    /**
     * Selects a static void method with five generic parameters for instrumentation using a method reference.
     *
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param <C> the type of the third method parameter
     * @param <D> the type of the fourth method parameter
     * @param <E> the type of the fifth method parameter
     * @param call a method reference to the static void method
     * @param arg0 a type token representing the generic type of the first method parameter
     * @param arg1 a type token representing the generic type of the second method parameter
     * @param arg2 a type token representing the generic type of the third method parameter
     * @param arg3 a type token representing the generic type of the fourth method parameter
     * @param arg4 a type token representing the generic type of the fifth method parameter
     * @return a builder for configuring the method rule
     */
    public <A, B, C, D, E> VoidMethodRuleBuilder.VoidMethodRuleBuilder5<T, A, B, C, D, E> callingVoidStatic(
        VoidCall5<A, B, C, D, E> call,
        TypeToken<A> arg0,
        TypeToken<B> arg1,
        TypeToken<C> arg2,
        TypeToken<D> arg3,
        TypeToken<E> arg4
    ) {
        MethodKey methodKey = resolveMethodReference(
            clazz,
            call,
            arg0.getRawType(),
            arg1.getRawType(),
            arg2.getRawType(),
            arg3.getRawType(),
            arg4.getRawType()
        );
        return new VoidMethodRuleBuilder.VoidMethodRuleBuilder5<>(registry, clazz, methodKey);
    }

    /**
     * Selects a static method with five generic parameters that returns a value, using a method reference.
     *
     * @param <R> the return type of the method
     * @param <A> the type of the first method parameter
     * @param <B> the type of the second method parameter
     * @param <C> the type of the third method parameter
     * @param <D> the type of the fourth method parameter
     * @param <E> the type of the fifth method parameter
     * @param call a method reference to the static method
     * @param arg0 a type token representing the generic type of the first method parameter
     * @param arg1 a type token representing the generic type of the second method parameter
     * @param arg2 a type token representing the generic type of the third method parameter
     * @param arg3 a type token representing the generic type of the fourth method parameter
     * @param arg4 a type token representing the generic type of the fifth method parameter
     * @return a builder for configuring the method rule
     */
    public <R, A, B, C, D, E> MethodRuleBuilder.MethodRuleBuilder5<T, R, A, B, C, D, E> callingStatic(
        Call5<R, A, B, C, D, E> call,
        TypeToken<A> arg0,
        TypeToken<B> arg1,
        TypeToken<C> arg2,
        TypeToken<D> arg3,
        TypeToken<E> arg4
    ) {
        MethodKey methodKey = resolveMethodReference(
            clazz,
            call,
            arg0.getRawType(),
            arg1.getRawType(),
            arg2.getRawType(),
            arg3.getRawType(),
            arg4.getRawType()
        );
        return new MethodRuleBuilder.MethodRuleBuilder5<>(registry, clazz, methodKey);
    }

    @SuppressForbidden(reason = "relies on reflection")
    private static MethodKey resolveMethodReference(Class<?> clazz, Object ref, Class<?>... args) {
        try {
            Method writeReplace = ref.getClass().getDeclaredMethod("writeReplace");
            writeReplace.setAccessible(true);

            SerializedLambda serialized = (SerializedLambda) writeReplace.invoke(ref);
            String className = serialized.getImplClass();
            String methodName = serialized.getImplMethodName();

            assertImplementationClass(clazz, className);

            return new MethodKey(
                resolveDeclaringClass(clazz, methodName, args).getTypeName().replace(".", "/"),
                methodName,
                Arrays.stream(args).map(ClassMethodBuilder::getParameterTypeName).toList()
            );
        } catch (Exception e) {
            throw new RuntimeException("Error occurred when inspecting class: " + clazz.getName(), e);
        }
    }

    private static String getParameterTypeName(Class<?> clazz) {
        if (clazz.isArray()) {
            return clazz.getComponentType().getName() + "[]";
        }
        return clazz.getName();
    }

    private static void assertImplementationClass(Class<?> clazz, String implClassName) throws ClassNotFoundException {
        Class<?> implClass = Class.forName(implClassName.replace("/", "."));
        if (implClass.isAssignableFrom(clazz) == false) {
            throw new IllegalArgumentException(
                "Method reference passed to 'calling()' does not belong to " + clazz.getName() + " or one of its subclasses."
            );
        }
    }

    @SuppressForbidden(reason = "relies on reflection")
    private static Class<?> resolveDeclaringClass(Class<?> clazz, String methodName, Class<?>... args) throws NoSuchMethodException {
        if ("<init>".equals(methodName)) {
            return clazz;
        }

        Class<?>[] resolvedArgs = Arrays.stream(args).map(ClassMethodBuilder::toPrimitiveIfBoxed).toArray(Class[]::new);
        Class<?> current = clazz;
        while (current != null) {
            try {
                current.getDeclaredMethod(methodName, resolvedArgs);
                return current;
            } catch (NoSuchMethodException e) {
                current = current.getSuperclass();
            }
        }

        throw new NoSuchMethodException("Method " + methodName + " not found on class hierarchy of " + clazz.getName());
    }

    private static Class<?> toPrimitiveIfBoxed(Class<?> type) {
        if (type == Boolean.class) {
            return boolean.class;
        }
        if (type == Byte.class) {
            return byte.class;
        }
        if (type == Short.class) {
            return short.class;
        }
        if (type == Character.class) {
            return char.class;
        }
        if (type == Integer.class) {
            return int.class;
        }
        if (type == Long.class) {
            return long.class;
        }
        if (type == Float.class) {
            return float.class;
        }
        if (type == Double.class) {
            return double.class;
        }
        if (type == Void.class) {
            return void.class;
        }
        return type;
    }

    private MethodKey getConstructorMethodKey(Class<?>... args) {
        return new MethodKey(clazz.getName().replace(".", "/"), "<init>", Arrays.stream(args).map(Class::getCanonicalName).toList());
    }
}
