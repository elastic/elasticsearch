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
public final class DynamicCallSite {
    static final int METHOD_CALL = 0;
    static final int LOAD = 1;
    static final int STORE = 2;
    
    static class InliningCacheCallSite extends MutableCallSite {
        private static final int MAX_DEPTH = 5;
        
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
    
    public static CallSite bootstrap(Lookup lookup, String name, MethodType type, int flavor) {
        InliningCacheCallSite callSite = new InliningCacheCallSite(lookup, name, type, flavor);
        
        MethodHandle fallback = FALLBACK.bindTo(callSite);
        fallback = fallback.asCollector(Object[].class, type.parameterCount());
        fallback = fallback.asType(type);
        
        callSite.setTarget(fallback);
        return callSite;
    }
    
    public static boolean checkClass(Class<?> clazz, Object receiver) {
        return receiver.getClass() == clazz;
    }
    
    private static MethodHandle lookup(int flavor, Class<?> clazz, String name) {
        switch(flavor) {
            case METHOD_CALL: 
                return Def.methodHandle(clazz, name, Definition.INSTANCE);
            case LOAD: 
                return Def.loadHandle(clazz, name, Definition.INSTANCE);
            case STORE:
                return Def.storeHandle(clazz, name, Definition.INSTANCE);
            default: throw new AssertionError();
        }
    }
    
    public static Object fallback(InliningCacheCallSite callSite, Object[] args) throws Throwable {
        MethodType type = callSite.type();
        if (callSite.depth >= InliningCacheCallSite.MAX_DEPTH) {
            // revert to a vtable call
            MethodHandle target = lookup(callSite.flavor, type.parameterType(0), callSite.name);
            callSite.setTarget(target);
            return target.invokeWithArguments(args);
        }
        
        Object receiver = args[0];
        Class<?> receiverClass = receiver.getClass();
        MethodHandle target = lookup(callSite.flavor, receiverClass, callSite.name);
        target = target.asType(type);
        
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
