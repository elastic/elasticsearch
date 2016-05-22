package org.elasticsearch.painless;

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

    /**
     * CallSite that implements the polymorphic inlining cache (PIC).
     */
    static final class PIC extends MutableCallSite {
        /** maximum number of types before we go megamorphic */
        static final int MAX_DEPTH = 5;

        private final String name;
        private final int flavor;
        int depth; // pkg-protected for testing

        PIC(String name, MethodType type, int flavor) {
            super(type);
            this.name = name;
            this.flavor = flavor;

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
        private static MethodHandle lookup(int flavor, Class<?> clazz, String name, MethodType type) {
            switch(flavor) {
                case METHOD_CALL:
                    return Def.lookupMethod(clazz, name, type);
                case LOAD:
                    return Def.lookupGetter(clazz, name);
                case STORE:
                    return Def.lookupSetter(clazz, name);
                case ARRAY_LOAD:
                    return Def.lookupArrayLoad(clazz);
                case ARRAY_STORE:
                    return Def.lookupArrayStore(clazz);
                default: throw new AssertionError();
            }
        }

        /**
         * Called when a new type is encountered (or, when we have encountered more than {@code MAX_DEPTH}
         * types at this call site and given up on caching).
         */
        Object fallback(Object[] args) throws Throwable {
            final MethodType type = type();
            final Object receiver = args[0];
            final Class<?> receiverClass = receiver.getClass();
            final MethodHandle target = lookup(flavor, receiverClass, name, type).asType(type);

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
    public static CallSite bootstrap(Lookup lookup, String name, MethodType type, int flavor) {
        return new PIC(name, type, flavor);
    }

}
