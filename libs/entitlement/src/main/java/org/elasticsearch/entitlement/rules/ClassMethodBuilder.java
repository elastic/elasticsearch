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
import org.elasticsearch.entitlement.rules.function.VarargCallAdapter;
import org.elasticsearch.entitlement.rules.function.VoidCall0;
import org.elasticsearch.entitlement.rules.function.VoidCall1;
import org.elasticsearch.entitlement.rules.function.VoidCall2;
import org.elasticsearch.entitlement.rules.function.VoidCall3;
import org.elasticsearch.entitlement.rules.function.VoidCall4;
import org.elasticsearch.entitlement.rules.function.VoidCall5;
import org.elasticsearch.entitlement.rules.function.VoidCall6;
import org.elasticsearch.entitlement.runtime.registry.InternalInstrumentationRegistry;
import org.elasticsearch.entitlement.util.TypeUtils;

import java.lang.invoke.SerializedLambda;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;

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
 * Instance method rules are automatically inherited by subtypes: any class that extends or
 * implements the target class will have the same rules applied to its methods.
 * A method reference is resolved to its declaring class in the type hierarchy, so
 * {@code .on(Concrete.class).calling(Interface::method)} attributes the rule to the
 * interface or superclass that actually declares the method, enabling correct inheritance.
 * Defining a rule on a method that already has a rule on an ancestor or descendant type is
 * forbidden. Constructor rules ({@code protectedCtor()}) are not inherited.
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
        MethodKey methodKey = resolveConstructor(clazz);
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
        MethodKey methodKey = resolveConstructor(clazz, arg0);
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
        MethodKey methodKey = resolveConstructor(clazz, arg0, arg1);
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
            return resolveMethodReferenceViaSerializedLambda(clazz, ref, args);
        } catch (Exception e) {
            if (clazz.isInterface()) {
                try {
                    return resolveMethodReferenceViaProxy(clazz, ref, args);
                } catch (Exception proxyException) {
                    proxyException.addSuppressed(e);
                    throw new RuntimeException(
                        "Error occurred when inspecting class: "
                            + clazz.getName()
                            + "; SerializedLambda resolution failed and proxy fallback failed",
                        proxyException
                    );
                }
            }
            throw new RuntimeException("Error occurred when inspecting class: " + clazz.getName(), e);
        }
    }

    @SuppressForbidden(reason = "relies on reflection")
    private static MethodKey resolveMethodReferenceViaSerializedLambda(Class<?> clazz, Object ref, Class<?>... args) throws Exception {
        Method writeReplace = ref.getClass().getDeclaredMethod("writeReplace");
        writeReplace.setAccessible(true);

        SerializedLambda serialized = (SerializedLambda) writeReplace.invoke(ref);
        String className = serialized.getImplClass();
        String methodName = serialized.getImplMethodName();

        assertImplementationClass(clazz, className);

        return new MethodKey(
            resolveDeclaringClass(clazz, methodName, args).getTypeName().replace(".", "/"),
            methodName,
            Arrays.stream(args).map(TypeUtils::getParameterTypeName).toList()
        );
    }

    /**
     * Resolves a method reference for an interface type by invoking the lambda with a recording proxy.
     * The proxy records which method was invoked; we then build a MethodKey from that Method.
     * Only used when SerializedLambda resolution fails (e.g. interface method refs pointing at synthetic classes).
     */
    @SuppressForbidden(reason = "relies on reflection")
    private static MethodKey resolveMethodReferenceViaProxy(Class<?> clazz, Object ref, Class<?>... args) throws Exception {
        var holder = new Object() {
            Method recordedMethod;
        };
        InvocationHandler handler = (proxy, method, methodArgs) -> {
            holder.recordedMethod = method;
            return TypeUtils.defaultValueFor(method.getReturnType());
        };
        Object proxy = Proxy.newProxyInstance(clazz.getClassLoader(), new Class<?>[] { clazz }, handler);

        Object[] callArgs = new Object[1 + args.length];
        callArgs[0] = proxy;
        for (int i = 0; i < args.length; i++) {
            callArgs[1 + i] = TypeUtils.defaultValueFor(args[i]);
        }

        if (ref instanceof VarargCallAdapter<?> adapter) {
            adapter.asVarargCall().call(callArgs);
        } else {
            Class<?>[] paramTypes = new Class<?>[1 + args.length];
            paramTypes[0] = clazz;
            System.arraycopy(args, 0, paramTypes, 1, args.length);
            Method callMethod = ref.getClass().getMethod("call", paramTypes);
            callMethod.invoke(ref, callArgs);
        }

        if (holder.recordedMethod == null) {
            throw new IllegalStateException(
                "Proxy fallback only supports instance method references; no method was invoked on the proxy for "
                    + clazz.getName()
                    + ". If this is a static method reference, resolution must use SerializedLambda."
            );
        }
        return methodKeyFromMethod(holder.recordedMethod);
    }

    private static MethodKey methodKeyFromMethod(Method method) {
        String className = method.getDeclaringClass().getName().replace(".", "/");
        String methodName = method.getName();
        List<String> parameterTypes = Arrays.stream(method.getParameterTypes()).map(TypeUtils::getParameterTypeName).toList();
        return new MethodKey(className, methodName, parameterTypes);
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
        Class<?>[] resolvedArgs = Arrays.stream(args).map(TypeUtils::toPrimitive).toArray(Class[]::new);
        return clazz.getMethod(methodName, resolvedArgs).getDeclaringClass();
    }

    @SuppressForbidden(reason = "relies on reflection")
    private static void validateConstructorExists(Class<?> clazz, Class<?>... args) {
        Class<?>[] resolvedArgs = Arrays.stream(args).map(TypeUtils::toPrimitive).toArray(Class[]::new);
        try {
            clazz.getDeclaredConstructor(resolvedArgs);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(
                "No constructor found on " + clazz.getName() + " with parameter types " + Arrays.stream(args).map(Class::getName).toList(),
                e
            );
        }
    }

    private static MethodKey resolveConstructor(Class<?> clazz, Class<?>... args) {
        validateConstructorExists(clazz, args);
        return new MethodKey(
            clazz.getName().replace(".", "/"),
            "<init>",
            Arrays.stream(args).map(TypeUtils::getParameterTypeName).toList()
        );
    }
}
