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
 * Painless invokedynamic call site.
 * <p>
 * Has 3 flavors (passed as static bootstrap parameters): dynamic method call,
 * dynamic field load (getter), and dynamic field store (setter).
 * <p>
 * When a new type is encountered at the call site, we lookup from the appropriate
 * whitelist, and cache with a guard. If we encounter too many types, we stop caching.
 * <p>
 * Based on the cascaded inlining cache from the JSR 292 cookbook 
 * (https://code.google.com/archive/p/jsr292-cookbook/, BSD license)
 */
// NOTE: this class must be public, because generated painless classes are in a different package,
// and it needs to be accessible by that code.
public final class DynamicCallSite {
    // NOTE: these must be primitive types, see https://docs.oracle.com/javase/specs/jvms/se7/html/jvms-6.html#jvms-6.5.invokedynamic
    /** static bootstrap parameter indicating a dynamic method call, e.g. foo.bar(...) */
    static final int METHOD_CALL = 0;
    /** static bootstrap parameter indicating a dynamic load (getter), e.g. baz = foo.bar */
    static final int LOAD = 1;
    /** static bootstrap parameter indicating a dynamic store (setter), e.g. foo.bar = baz */
    static final int STORE = 2;
    
    static class InliningCacheCallSite extends MutableCallSite {
        /** maximum number of types before we go megamorphic */
        static final int MAX_DEPTH = 5;
        
        final Lookup lookup;
        final String name;
        final int flavor;
        int depth;
        
        InliningCacheCallSite(Lookup lookup, String name, MethodType type, int flavor) {
            super(type);
            this.lookup = lookup;
            this.name = name;
            this.flavor = flavor;
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
        InliningCacheCallSite callSite = new InliningCacheCallSite(lookup, name, type, flavor);
        
        MethodHandle fallback = FALLBACK.bindTo(callSite);
        fallback = fallback.asCollector(Object[].class, type.parameterCount());
        fallback = fallback.asType(type);
        
        callSite.setTarget(fallback);
        return callSite;
    }
    
    /** 
     * guard method for inline caching: checks the receiver's class is the same
     * as the cached class
     */
    public static boolean checkClass(Class<?> clazz, Object receiver) {
        return receiver.getClass() == clazz;
    }
    
    /**
     * Does a slow lookup against the whitelist.
     */
    private static MethodHandle lookup(int flavor, Class<?> clazz, String name) {
        switch(flavor) {
            case METHOD_CALL: 
                return Def.lookupMethod(clazz, name, Definition.INSTANCE);
            case LOAD: 
                return Def.lookupGetter(clazz, name, Definition.INSTANCE);
            case STORE:
                return Def.lookupSetter(clazz, name, Definition.INSTANCE);
            default: throw new AssertionError();
        }
    }
    
    /**
     * Called when a new type is encountered (or, when we have encountered more than {@code MAX_DEPTH}
     * types at this call site and given up on caching). 
     */
    public static Object fallback(InliningCacheCallSite callSite, Object[] args) throws Throwable {
        MethodType type = callSite.type();
        Object receiver = args[0];
        Class<?> receiverClass = receiver.getClass();
        MethodHandle target = lookup(callSite.flavor, receiverClass, callSite.name);
        target = target.asType(type);
        
        if (callSite.depth >= InliningCacheCallSite.MAX_DEPTH) {
            // revert to a vtable call
            callSite.setTarget(target);
            return target.invokeWithArguments(args);
        }
        
        MethodHandle test = CHECK_CLASS.bindTo(receiverClass);
        test = test.asType(test.type().changeParameterType(0, type.parameterType(0)));
        
        MethodHandle guard = MethodHandles.guardWithTest(test, target, callSite.getTarget());
        callSite.depth++;
        
        callSite.setTarget(guard);
        return target.invokeWithArguments(args);
    }
    
    private static final MethodHandle CHECK_CLASS;
    private static final MethodHandle FALLBACK;
    static {
        Lookup lookup = MethodHandles.lookup();
        try {
            CHECK_CLASS = lookup.findStatic(DynamicCallSite.class, "checkClass",
                                            MethodType.methodType(boolean.class, Class.class, Object.class));
            FALLBACK = lookup.findStatic(DynamicCallSite.class, "fallback",
                                         MethodType.methodType(Object.class, InliningCacheCallSite.class, Object[].class));
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }
}
