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
 * Has 7 flavors (passed as static bootstrap parameters): dynamic method call,
 * dynamic field load (getter), and dynamic field store (setter), dynamic array load,
 * dynamic array store, iterator, and method reference.
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
    /** static bootstrap parameter indicating a unary math operator, e.g. ~foo */
    public static final int UNARY_OPERATOR = 7;
    /** static bootstrap parameter indicating a binary math operator, e.g. foo / bar */
    public static final int BINARY_OPERATOR = 8;
    /** static bootstrap parameter indicating a shift operator, e.g. foo &gt;&gt; bar */
    public static final int SHIFT_OPERATOR = 9;
    
    // constants for the flags parameter of operators
    /** 
     * static bootstrap parameter indicating the binary operator allows nulls (e.g. == and +) 
     * <p>
     * requires additional {@link MethodHandles#catchException} guard, which will invoke
     * the fallback if a null is encountered.
     */
    public static final int OPERATOR_ALLOWS_NULL = 1 << 0;

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
            
            // For operators use a monomorphic cache, fallback is fast.
            // Just start with a depth of MAX-1, to keep it a constant.
            if (flavor == UNARY_OPERATOR || flavor == BINARY_OPERATOR || flavor == SHIFT_OPERATOR) {
                depth = MAX_DEPTH - 1;
            }

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
         * guard method for inline caching: checks the receiver's class and the first argument
         * are the same as the cached receiver and first argument.
         */
        static boolean checkBinary(Class<?> left, Class<?> right, Object leftObject, Object rightObject) {
            return leftObject.getClass() == left && rightObject.getClass() == right;
        }
        
        /**
         * guard method for inline caching: checks the first argument is the same
         * as the cached first argument.
         */
        static boolean checkBinaryArg(Class<?> left, Class<?> right, Object leftObject, Object rightObject) {
            return rightObject.getClass() == right;
        }

        /**
         * Does a slow lookup against the whitelist.
         */
        private MethodHandle lookup(int flavor, String name, Object[] args) throws Throwable {
            switch(flavor) {
                case METHOD_CALL:
                    return Def.lookupMethod(lookup, type(), args[0].getClass(), name, args, (Long) this.args[0]);
                case LOAD:
                    return Def.lookupGetter(args[0].getClass(), name);
                case STORE:
                    return Def.lookupSetter(args[0].getClass(), name);
                case ARRAY_LOAD:
                    return Def.lookupArrayLoad(args[0].getClass());
                case ARRAY_STORE:
                    return Def.lookupArrayStore(args[0].getClass());
                case ITERATOR:
                    return Def.lookupIterator(args[0].getClass());
                case REFERENCE:
                    return Def.lookupReference(lookup, (String) this.args[0], args[0].getClass(), name);
                case UNARY_OPERATOR:
                case SHIFT_OPERATOR:
                    // shifts are treated as unary, as java allows long arguments without a cast (but bits are ignored)
                    return DefMath.lookupUnary(args[0].getClass(), name);
                case BINARY_OPERATOR:
                    if (args[0] == null || args[1] == null) {
                        return getGeneric(flavor, name); // can handle nulls, if supported
                    } else {
                        return DefMath.lookupBinary(args[0].getClass(), args[1].getClass(), name);
                    }
                default: throw new AssertionError();
            }
        }
        
        /**
         * Installs a permanent, generic solution that works with any parameter types, if possible.
         */
        private MethodHandle getGeneric(int flavor, String name) throws Throwable {
            switch(flavor) {
                case UNARY_OPERATOR:
                case BINARY_OPERATOR:
                case SHIFT_OPERATOR:
                    return DefMath.lookupGeneric(name);
                default:
                    return null;
            }
        }

        /**
         * Called when a new type is encountered (or, when we have encountered more than {@code MAX_DEPTH}
         * types at this call site and given up on caching).
         */
        @SuppressForbidden(reason = "slow path")
        Object fallback(Object[] args) throws Throwable {
            if (depth >= MAX_DEPTH) {
                // caching defeated
                MethodHandle generic = getGeneric(flavor, name);
                if (generic != null) {
                    setTarget(generic.asType(type()));
                    return generic.invokeWithArguments(args);
                } else {
                    return lookup(flavor, name, args).invokeWithArguments(args);
                }
            }
            
            final MethodType type = type();
            final MethodHandle target = lookup(flavor, name, args).asType(type);

            final MethodHandle test;
            if (flavor == BINARY_OPERATOR || flavor == SHIFT_OPERATOR) {
                // some binary operators support nulls, we handle them separate
                Class<?> clazz0 = args[0] == null ? null : args[0].getClass();
                Class<?> clazz1 = args[1] == null ? null : args[1].getClass();
                if (type.parameterType(1) != Object.class) {
                    // case 1: only the receiver is unknown, just check that
                    MethodHandle unaryTest = CHECK_CLASS.bindTo(clazz0);
                    test = unaryTest.asType(unaryTest.type()
                                            .changeParameterType(0, type.parameterType(0)));
                } else if (type.parameterType(0) != Object.class) {
                    // case 2: only the argument is unknown, just check that
                    MethodHandle unaryTest = CHECK_BINARY_ARG.bindTo(clazz0).bindTo(clazz1);
                    test = unaryTest.asType(unaryTest.type()
                                            .changeParameterType(0, type.parameterType(0))
                                            .changeParameterType(1, type.parameterType(1)));
                } else {
                    // case 3: check both receiver and argument
                    MethodHandle binaryTest = CHECK_BINARY.bindTo(clazz0).bindTo(clazz1);
                    test = binaryTest.asType(binaryTest.type()
                                            .changeParameterType(0, type.parameterType(0))
                                            .changeParameterType(1, type.parameterType(1)));
                }
            } else {
                MethodHandle receiverTest = CHECK_CLASS.bindTo(args[0].getClass());
                test = receiverTest.asType(receiverTest.type()
                                        .changeParameterType(0, type.parameterType(0)));
            }

            MethodHandle guard = MethodHandles.guardWithTest(test, target, getTarget());
            // very special cases, where even the receiver can be null (see JLS rules for string concat)
            // we wrap + with an NPE catcher, and use our generic method in that case.
            if (flavor == BINARY_OPERATOR && ((int)this.args[0] & OPERATOR_ALLOWS_NULL) != 0) {
                MethodHandle handler = MethodHandles.dropArguments(getGeneric(flavor, name).asType(type()), 0, NullPointerException.class);
                guard = MethodHandles.catchException(guard, NullPointerException.class, handler);
            }
            
            depth++;

            setTarget(guard);
            return target.invokeWithArguments(args);
        }

        private static final MethodHandle CHECK_CLASS;
        private static final MethodHandle CHECK_BINARY;
        private static final MethodHandle CHECK_BINARY_ARG;
        private static final MethodHandle FALLBACK;
        static {
            final Lookup lookup = MethodHandles.lookup();
            try {
                CHECK_CLASS = lookup.findStatic(lookup.lookupClass(), "checkClass",
                                              MethodType.methodType(boolean.class, Class.class, Object.class));
                CHECK_BINARY = lookup.findStatic(lookup.lookupClass(), "checkBinary",
                                              MethodType.methodType(boolean.class, Class.class, Class.class, Object.class, Object.class));
                CHECK_BINARY_ARG = lookup.findStatic(lookup.lookupClass(), "checkBinaryArg",
                                              MethodType.methodType(boolean.class, Class.class, Class.class, Object.class, Object.class));
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
            case UNARY_OPERATOR:
            case SHIFT_OPERATOR:
            case BINARY_OPERATOR:
                if (args.length != 1) {
                    throw new BootstrapMethodError("Invalid number of parameters for operator call");
                }
                if (args[0] instanceof Integer == false) {
                    throw new BootstrapMethodError("Illegal parameter for reference call: " + args[0]);
                }
                int flags = (int)args[0];
                if ((flags & OPERATOR_ALLOWS_NULL) != 0 && flavor != BINARY_OPERATOR) {
                    // we just don't need it anywhere else.
                    throw new BootstrapMethodError("This parameter is only supported for BINARY_OPERATORs");
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
