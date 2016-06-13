package org.elasticsearch.painless;

import org.elasticsearch.common.SuppressForbidden;

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.lang.invoke.CallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.invoke.MutableCallSite;

/**
 * Painless invokedynamic bootstrap for the call site.
 * <p>
 * Has 5 flavors (passed as static bootstrap parameters): dynamic method call,
 * dynamic field load (getter), and dynamic field store (setter), dynamic array load,
 * and dynamic array store.
 * <p>
 * When a new type is encountered at the call site, we lookup from the appropriate
 * whitelist, and cache with a guard. If we encounter too many types, we stop caching.
 * <p>
 * Based on the cascaded inlining cache from the JSR 292 cookbook
 * (https://code.google.com/archive/p/jsr292-cookbook/, BSD license)
 */
// NOTE: this class must be public, because generated painless classes are in a different classloader,
// and it needs to be accessible by that code.
public final class DefBootstrap {

    private DefBootstrap() {} // no instance!

    // NOTE: these must be primitive types, see https://docs.oracle.com/javase/specs/jvms/se7/html/jvms-6.html#jvms-6.5.invokedynamic
    /** static bootstrap parameter indicating a dynamic method call, e.g. foo.bar(...) */
    public static final int METHOD_CALL = 0;
    /** static bootstrap parameter indicating a dynamic load (getter), e.g. baz = foo.bar */
    public static final int LOAD = 1;
    /** static bootstrap parameter indicating a dynamic store (setter), e.g. foo.bar = baz */
    public static final int STORE = 2;
    /** static bootstrap parameter indicating a dynamic array load, e.g. baz = foo[bar] */
    public static final int ARRAY_LOAD = 3;
    /** static bootstrap parameter indicating a dynamic array store, e.g. foo[bar] = baz */
    public static final int ARRAY_STORE = 4;
    /** static bootstrap parameter indicating a dynamic iteration, e.g. for (x : y) */
    public static final int ITERATOR = 5;
    /** static bootstrap parameter indicating a dynamic method reference, e.g. foo::bar */
    public static final int REFERENCE = 6;

    /**
     * CallSite that implements the polymorphic inlining cache (PIC).
     */
    static final class PIC extends MutableCallSite {
        /** maximum number of types before we go megamorphic */
        static final int MAX_DEPTH = 5;

        private final Lookup lookup;
        private final String name;
        private final int flavor;
        private final Object[] args;
        int depth; // pkg-protected for testing

        PIC(Lookup lookup, String name, MethodType type, int flavor, Object[] args) {
            super(type);
            this.lookup = lookup;
            this.name = name;
            this.flavor = flavor;
            this.args = args;

            final MethodHandle fallback = FALLBACK.bindTo(this)
              .asCollector(Object[].class, type.parameterCount())
              .asType(type);

            setTarget(fallback);
        }
        
        /**
         * guard method for inline caching: checks the receiver's class is the same
         * as the cached class
         */
        static boolean checkClass(Class<?> clazz, Object receiver) {
            return receiver.getClass() == clazz;
        }

        /**
         * Does a slow lookup against the whitelist.
         */
        private MethodHandle lookup(int flavor, Class<?> clazz, String name, Object[] args) throws Throwable {
            switch(flavor) {
                case METHOD_CALL:
                    return Def.lookupMethod(lookup, type(), clazz, name, args, (Long) this.args[0]);
                case LOAD:
                    return Def.lookupGetter(clazz, name);
                case STORE:
                    return Def.lookupSetter(clazz, name);
                case ARRAY_LOAD:
                    return Def.lookupArrayLoad(clazz);
                case ARRAY_STORE:
                    return Def.lookupArrayStore(clazz);
                case ITERATOR:
                    return Def.lookupIterator(clazz);
                case REFERENCE:
                    return Def.lookupReference(lookup, (String) this.args[0], clazz, name);
                default: throw new AssertionError();
            }
        }

        /**
         * Called when a new type is encountered (or, when we have encountered more than {@code MAX_DEPTH}
         * types at this call site and given up on caching).
         */
        @SuppressForbidden(reason = "slow path")
        Object fallback(Object[] args) throws Throwable {
            final MethodType type = type();
            final Object receiver = args[0];
            final Class<?> receiverClass = receiver.getClass();
            final MethodHandle target = lookup(flavor, receiverClass, name, args).asType(type);

            if (depth >= MAX_DEPTH) {
                // revert to a vtable call
                setTarget(target);
                return target.invokeWithArguments(args);
            }

            MethodHandle test = CHECK_CLASS.bindTo(receiverClass);
            test = test.asType(test.type().changeParameterType(0, type.parameterType(0)));

            final MethodHandle guard = MethodHandles.guardWithTest(test, target, getTarget());
            
            depth++;

            setTarget(guard);
            return target.invokeWithArguments(args);
        }

        private static final MethodHandle CHECK_CLASS;
        private static final MethodHandle FALLBACK;
        static {
            final Lookup lookup = MethodHandles.lookup();
            try {
                CHECK_CLASS = lookup.findStatic(lookup.lookupClass(), "checkClass",
                                                MethodType.methodType(boolean.class, Class.class, Object.class));
                FALLBACK = lookup.findVirtual(lookup.lookupClass(), "fallback",
                                              MethodType.methodType(Object.class, Object[].class));
            } catch (ReflectiveOperationException e) {
                throw new AssertionError(e);
            }
        }
    }

    /**
     * invokeDynamic bootstrap method
     * <p>
     * In addition to ordinary parameters, we also take a static parameter {@code flavor} which
     * tells us what type of dynamic call it is (and which part of whitelist to look at).
     * <p>
     * see https://docs.oracle.com/javase/specs/jvms/se7/html/jvms-6.html#jvms-6.5.invokedynamic
     */
    public static CallSite bootstrap(Lookup lookup, String name, MethodType type, int flavor, Object... args) {
        // validate arguments
        switch(flavor) {
            case METHOD_CALL:
                if (args.length != 1) {
                    throw new BootstrapMethodError("Invalid number of parameters for method call");
                }
                if (args[0] instanceof Long == false) {
                    throw new BootstrapMethodError("Illegal parameter for method call: " + args[0]);
                }
                long recipe = (Long) args[0];
                if (Long.bitCount(recipe) > type.parameterCount()) {
                    throw new BootstrapMethodError("Illegal recipe for method call: too many bits");
                }
                break;
            case REFERENCE:
                if (args.length != 1) {
                    throw new BootstrapMethodError("Invalid number of parameters for reference call");
                }
                if (args[0] instanceof String == false) {
                    throw new BootstrapMethodError("Illegal parameter for reference call: " + args[0]);
                }
                break;
            default:
                if (args.length > 0) {
                    throw new BootstrapMethodError("Illegal static bootstrap parameters for flavor: " + flavor);
                }
                break;
        }
        return new PIC(lookup, name, type, flavor, args);
    }

}
