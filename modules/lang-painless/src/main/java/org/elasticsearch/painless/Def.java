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

package org.elasticsearch.painless;

import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.Definition.RuntimeClass;

import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Support for dynamic type (def).
 * <p>
 * Dynamic types can invoke methods, load/store fields, and be passed as parameters to operators without
 * compile-time type information.
 * <p>
 * Dynamic methods, loads, stores, and array/list/map load/stores involve locating the appropriate field
 * or method depending on the receiver's class. For these, we emit an {@code invokedynamic} instruction that,
 * for each new type encountered will query a corresponding {@code lookupXXX} method to retrieve the appropriate
 * method. In most cases, the {@code lookupXXX} methods here will only be called once for a given call site, because
 * caching ({@link DefBootstrap}) generally works: usually all objects at any call site will be consistently
 * the same type (or just a few types).  In extreme cases, if there is type explosion, they may be called every
 * single time, but simplicity is still more valuable than performance in this code.
 */
public final class Def {

    // TODO: Once Java has a factory for those in java.lang.invoke.MethodHandles, use it:

    /** Helper class for isolating MethodHandles and methods to get the length of arrays
     * (to emulate a "arraystore" bytecode using MethodHandles).
     * See: https://bugs.openjdk.java.net/browse/JDK-8156915
     */
    @SuppressWarnings("unused") // getArrayLength() methods are are actually used, javac just does not know :)
    private static final class ArrayLengthHelper {
        private static final Lookup PRIV_LOOKUP = MethodHandles.lookup();

        private static final Map<Class<?>,MethodHandle> ARRAY_TYPE_MH_MAPPING = Collections.unmodifiableMap(
            Stream.of(boolean[].class, byte[].class, short[].class, int[].class, long[].class,
                char[].class, float[].class, double[].class, Object[].class)
                .collect(Collectors.toMap(Function.identity(), type -> {
                    try {
                        return PRIV_LOOKUP.findStatic(PRIV_LOOKUP.lookupClass(), "getArrayLength", MethodType.methodType(int.class, type));
                    } catch (ReflectiveOperationException e) {
                        throw new AssertionError(e);
                    }
                }))
        );

        private static final MethodHandle OBJECT_ARRAY_MH = ARRAY_TYPE_MH_MAPPING.get(Object[].class);

        static int getArrayLength(final boolean[] array) { return array.length; }
        static int getArrayLength(final byte[] array)    { return array.length; }
        static int getArrayLength(final short[] array)   { return array.length; }
        static int getArrayLength(final int[] array)     { return array.length; }
        static int getArrayLength(final long[] array)    { return array.length; }
        static int getArrayLength(final char[] array)    { return array.length; }
        static int getArrayLength(final float[] array)   { return array.length; }
        static int getArrayLength(final double[] array)  { return array.length; }
        static int getArrayLength(final Object[] array)  { return array.length; }

        static MethodHandle arrayLengthGetter(Class<?> arrayType) {
            if (!arrayType.isArray()) {
                throw new IllegalArgumentException("type must be an array");
            }
            return (ARRAY_TYPE_MH_MAPPING.containsKey(arrayType)) ?
                ARRAY_TYPE_MH_MAPPING.get(arrayType) :
                OBJECT_ARRAY_MH.asType(OBJECT_ARRAY_MH.type().changeParameterType(0, arrayType));
        }

        private ArrayLengthHelper() {}
    }

    /** pointer to Map.get(Object) */
    private static final MethodHandle MAP_GET;
    /** pointer to Map.put(Object,Object) */
    private static final MethodHandle MAP_PUT;
    /** pointer to List.get(int) */
    private static final MethodHandle LIST_GET;
    /** pointer to List.set(int,Object) */
    private static final MethodHandle LIST_SET;
    /** pointer to Iterable.iterator() */
    private static final MethodHandle ITERATOR;
    /** factory for arraylength MethodHandle (intrinsic) from Java 9 */
    private static final MethodHandle JAVA9_ARRAY_LENGTH_MH_FACTORY;

    static {
        final Lookup lookup = MethodHandles.publicLookup();

        try {
            MAP_GET  = lookup.findVirtual(Map.class , "get", MethodType.methodType(Object.class, Object.class));
            MAP_PUT  = lookup.findVirtual(Map.class , "put", MethodType.methodType(Object.class, Object.class, Object.class));
            LIST_GET = lookup.findVirtual(List.class, "get", MethodType.methodType(Object.class, int.class));
            LIST_SET = lookup.findVirtual(List.class, "set", MethodType.methodType(Object.class, int.class, Object.class));
            ITERATOR = lookup.findVirtual(Iterable.class, "iterator", MethodType.methodType(Iterator.class));
        } catch (final ReflectiveOperationException roe) {
            throw new AssertionError(roe);
        }
        
        // lookup up the factory for arraylength MethodHandle (intrinsic) from Java 9:
        // https://bugs.openjdk.java.net/browse/JDK-8156915
        MethodHandle arrayLengthMHFactory;
        try {
            arrayLengthMHFactory = lookup.findStatic(MethodHandles.class, "arrayLength",
                MethodType.methodType(MethodHandle.class, Class.class));
        } catch (final ReflectiveOperationException roe) {
            arrayLengthMHFactory = null;
        }
        JAVA9_ARRAY_LENGTH_MH_FACTORY = arrayLengthMHFactory;
    }

    /** Hack to rethrow unknown Exceptions from {@link MethodHandle#invokeExact}: */
    @SuppressWarnings("unchecked")
    static <T extends Throwable> void rethrow(Throwable t) throws T {
        throw (T) t;
    }
    
    /** Returns an array length getter MethodHandle for the given array type */
    static MethodHandle arrayLengthGetter(Class<?> arrayType) {
        if (JAVA9_ARRAY_LENGTH_MH_FACTORY != null) {
            try {
                return (MethodHandle) JAVA9_ARRAY_LENGTH_MH_FACTORY.invokeExact(arrayType);
            } catch (Throwable t) {
                rethrow(t);
                throw new AssertionError(t);
            }
        } else {
            return ArrayLengthHelper.arrayLengthGetter(arrayType);
        }
    }

    /**
     * Looks up method entry for a dynamic method call.
     * <p>
     * A dynamic method call for variable {@code x} of type {@code def} looks like:
     * {@code x.method(args...)}
     * <p>
     * This method traverses {@code recieverClass}'s class hierarchy (including interfaces)
     * until it finds a matching whitelisted method. If one is not found, it throws an exception.
     * Otherwise it returns the matching method.
     * <p>
     * @param receiverClass Class of the object to invoke the method on.
     * @param name Name of the method.
     * @param arity arity of method
     * @return matching method to invoke. never returns null.
     * @throws IllegalArgumentException if no matching whitelisted method was found.
     */
    static Method lookupMethodInternal(Class<?> receiverClass, String name, int arity) {
        Definition.MethodKey key = new Definition.MethodKey(name, arity);
        // check whitelist for matching method
        for (Class<?> clazz = receiverClass; clazz != null; clazz = clazz.getSuperclass()) {
            RuntimeClass struct = Definition.getRuntimeClass(clazz);

            if (struct != null) {
                Method method = struct.methods.get(key);
                if (method != null) {
                    return method;
                }
            }

            for (Class<?> iface : clazz.getInterfaces()) {
                struct = Definition.getRuntimeClass(iface);

                if (struct != null) {
                    Method method = struct.methods.get(key);
                    if (method != null) {
                        return method;
                    }
                }
            }
        }
        
        throw new IllegalArgumentException("Unable to find dynamic method [" + name + "] with [" + arity + "] arguments " +
                                           "for class [" + receiverClass.getCanonicalName() + "].");
    }

    /**
     * Looks up handle for a dynamic method call, with lambda replacement
     * <p>
     * A dynamic method call for variable {@code x} of type {@code def} looks like:
     * {@code x.method(args...)}
     * <p>
     * This method traverses {@code recieverClass}'s class hierarchy (including interfaces)
     * until it finds a matching whitelisted method. If one is not found, it throws an exception.
     * Otherwise it returns a handle to the matching method.
     * <p>
     * @param lookup caller's lookup
     * @param callSiteType callsite's type
     * @param receiverClass Class of the object to invoke the method on.
     * @param name Name of the method.
     * @param args bootstrap args passed to callsite
     * @return pointer to matching method to invoke. never returns null.
     * @throws IllegalArgumentException if no matching whitelisted method was found.
     * @throws Throwable if a method reference cannot be converted to an functional interface
     */
     static MethodHandle lookupMethod(Lookup lookup, MethodType callSiteType, 
             Class<?> receiverClass, String name, Object args[]) throws Throwable {
         long recipe = (Long) args[0];
         int numArguments = callSiteType.parameterCount();
         // simple case: no lambdas
         if (recipe == 0) {
             return lookupMethodInternal(receiverClass, name, numArguments - 1).handle;
         }
         
         // otherwise: first we have to compute the "real" arity. This is because we have extra arguments:
         // e.g. f(a, g(x), b, h(y), i()) looks like f(a, g, x, b, h, y, i). 
         int arity = callSiteType.parameterCount() - 1;
         int upTo = 1;
         for (int i = 0; i < numArguments; i++) {
             if ((recipe & (1L << (i - 1))) != 0) {
                 String signature = (String) args[upTo++];
                 int numCaptures = Integer.parseInt(signature.substring(signature.indexOf(',')+1));
                 arity -= numCaptures;
             }
         }
         
         // lookup the method with the proper arity, then we know everything (e.g. interface types of parameters).
         // based on these we can finally link any remaining lambdas that were deferred.
         Method method = lookupMethodInternal(receiverClass, name, arity);
         MethodHandle handle = method.handle;

         int replaced = 0;
         upTo = 1;
         for (int i = 1; i < numArguments; i++) {
             // its a functional reference, replace the argument with an impl
             if ((recipe & (1L << (i - 1))) != 0) {
                 // decode signature of form 'type.call,2' 
                 String signature = (String) args[upTo++];
                 int separator = signature.indexOf('.');
                 int separator2 = signature.indexOf(',');
                 String type = signature.substring(1, separator);
                 String call = signature.substring(separator+1, separator2);
                 int numCaptures = Integer.parseInt(signature.substring(separator2+1));
                 Class<?> captures[] = new Class<?>[numCaptures];
                 for (int capture = 0; capture < captures.length; capture++) {
                     captures[capture] = callSiteType.parameterType(i + 1 + capture);
                 }
                 MethodHandle filter;
                 Definition.Type interfaceType = method.arguments.get(i - 1 - replaced);
                 if (signature.charAt(0) == 'S') {
                     // the implementation is strongly typed, now that we know the interface type,
                     // we have everything.
                     filter = lookupReferenceInternal(lookup,
                                                      interfaceType,
                                                      type,
                                                      call,
                                                      captures);
                 } else if (signature.charAt(0) == 'D') {
                     // the interface type is now known, but we need to get the implementation.
                     // this is dynamically based on the receiver type (and cached separately, underneath
                     // this cache). It won't blow up since we never nest here (just references)
                     MethodType nestedType = MethodType.methodType(interfaceType.clazz, captures);
                     CallSite nested = DefBootstrap.bootstrap(lookup, 
                                                              call,
                                                              nestedType, 
                                                              DefBootstrap.REFERENCE,
                                                              interfaceType.name);
                     filter = nested.dynamicInvoker();
                 } else {
                     throw new AssertionError();
                 }
                 // the filter now ignores the signature (placeholder) on the stack
                 filter = MethodHandles.dropArguments(filter, 0, String.class);
                 handle = MethodHandles.collectArguments(handle, i, filter);
                 i += numCaptures;
                 replaced += numCaptures;
             }
         }
         
         return handle;
     }
     
     /**
      * Returns an implementation of interfaceClass that calls receiverClass.name
      * <p>
      * This is just like LambdaMetaFactory, only with a dynamic type. The interface type is known,
      * so we simply need to lookup the matching implementation method based on receiver type.
      */
     static MethodHandle lookupReference(Lookup lookup, String interfaceClass, 
                                         Class<?> receiverClass, String name) throws Throwable {
         Definition.Type interfaceType = Definition.getType(interfaceClass);
         Method interfaceMethod = interfaceType.struct.getFunctionalMethod();
         if (interfaceMethod == null) {
             throw new IllegalArgumentException("Class [" + interfaceClass + "] is not a functional interface");
         }
         int arity = interfaceMethod.arguments.size();
         Method implMethod = lookupMethodInternal(receiverClass, name, arity);
         return lookupReferenceInternal(lookup, interfaceType, implMethod.owner.name, implMethod.name, receiverClass);
     }
     
     /** Returns a method handle to an implementation of clazz, given method reference signature. */
     private static MethodHandle lookupReferenceInternal(Lookup lookup, Definition.Type clazz, String type,
                                                         String call, Class<?>... captures) throws Throwable {
         final FunctionRef ref;
         if ("this".equals(type)) {
             // user written method
             Method interfaceMethod = clazz.struct.getFunctionalMethod();
             if (interfaceMethod == null) {
                 throw new IllegalArgumentException("Cannot convert function reference [" + type + "::" + call + "] " +
                                                    "to [" + clazz.name + "], not a functional interface");
             }
             int arity = interfaceMethod.arguments.size() + captures.length;
             final MethodHandle handle;
             try {
                 MethodHandle accessor = lookup.findStaticGetter(lookup.lookupClass(), 
                                                                 getUserFunctionHandleFieldName(call, arity), 
                                                                 MethodHandle.class);
                 handle = (MethodHandle) accessor.invokeExact();
             } catch (NoSuchFieldException | IllegalAccessException e) {
                 throw new IllegalArgumentException("Unknown call [" + call + "] with [" + arity + "] arguments.");
             }
             ref = new FunctionRef(clazz, interfaceMethod, handle, captures);
         } else {
             // whitelist lookup
             ref = new FunctionRef(clazz, type, call, captures);
         }
         final CallSite callSite;
         if (ref.needsBridges()) {
             callSite = LambdaMetafactory.altMetafactory(lookup, 
                     ref.invokedName, 
                     ref.invokedType,
                     ref.samMethodType,
                     ref.implMethod,
                     ref.samMethodType,
                     LambdaMetafactory.FLAG_BRIDGES,
                     1,
                     ref.interfaceMethodType);
         } else {
             callSite = LambdaMetafactory.altMetafactory(lookup, 
                     ref.invokedName, 
                     ref.invokedType,
                     ref.samMethodType,
                     ref.implMethod,
                     ref.samMethodType,
                     0);
         }
         return callSite.dynamicInvoker().asType(MethodType.methodType(clazz.clazz, captures));
     }
     
     /** gets the field name used to lookup up the MethodHandle for a function. */
     public static String getUserFunctionHandleFieldName(String name, int arity) {
         return "handle$" + name + "$" + arity;
     }

    /**
     * Looks up handle for a dynamic field getter (field load)
     * <p>
     * A dynamic field load for variable {@code x} of type {@code def} looks like:
     * {@code y = x.field}
     * <p>
     * The following field loads are allowed:
     * <ul>
     *   <li>Whitelisted {@code field} from receiver's class or any superclasses.
     *   <li>Whitelisted method named {@code getField()} from receiver's class/superclasses/interfaces.
     *   <li>Whitelisted method named {@code isField()} from receiver's class/superclasses/interfaces.
     *   <li>The {@code length} field of an array.
     *   <li>The value corresponding to a map key named {@code field} when the receiver is a Map.
     *   <li>The value in a list at element {@code field} (integer) when the receiver is a List.
     * </ul>
     * <p>
     * This method traverses {@code recieverClass}'s class hierarchy (including interfaces)
     * until it finds a matching whitelisted getter. If one is not found, it throws an exception.
     * Otherwise it returns a handle to the matching getter.
     * <p>
     * @param receiverClass Class of the object to retrieve the field from.
     * @param name Name of the field.
     * @return pointer to matching field. never returns null.
     * @throws IllegalArgumentException if no matching whitelisted field was found.
     */
    static MethodHandle lookupGetter(Class<?> receiverClass, String name) {
        // first try whitelist
        for (Class<?> clazz = receiverClass; clazz != null; clazz = clazz.getSuperclass()) {
            RuntimeClass struct = Definition.getRuntimeClass(clazz);

            if (struct != null) {
                MethodHandle handle = struct.getters.get(name);
                if (handle != null) {
                    return handle;
                }
            }

            for (final Class<?> iface : clazz.getInterfaces()) {
                struct = Definition.getRuntimeClass(iface);

                if (struct != null) {
                    MethodHandle handle = struct.getters.get(name);
                    if (handle != null) {
                        return handle;
                    }
                }
            }
        }
        // special case: arrays, maps, and lists
        if (receiverClass.isArray() && "length".equals(name)) {
            // arrays expose .length as a read-only getter
            return arrayLengthGetter(receiverClass);
        } else if (Map.class.isAssignableFrom(receiverClass)) {
            // maps allow access like mymap.key
            // wire 'key' as a parameter, its a constant in painless
            return MethodHandles.insertArguments(MAP_GET, 1, name);
        } else if (List.class.isAssignableFrom(receiverClass)) {
            // lists allow access like mylist.0
            // wire '0' (index) as a parameter, its a constant. this also avoids
            // parsing the same integer millions of times!
            try {
                int index = Integer.parseInt(name);
                return MethodHandles.insertArguments(LIST_GET, 1, index);
            } catch (NumberFormatException exception) {
                throw new IllegalArgumentException( "Illegal list shortcut value [" + name + "].");
            }
        }

        throw new IllegalArgumentException("Unable to find dynamic field [" + name + "] " +
                                           "for class [" + receiverClass.getCanonicalName() + "].");
    }

    /**
     * Looks up handle for a dynamic field setter (field store)
     * <p>
     * A dynamic field store for variable {@code x} of type {@code def} looks like:
     * {@code x.field = y}
     * <p>
     * The following field stores are allowed:
     * <ul>
     *   <li>Whitelisted {@code field} from receiver's class or any superclasses.
     *   <li>Whitelisted method named {@code setField()} from receiver's class/superclasses/interfaces.
     *   <li>The value corresponding to a map key named {@code field} when the receiver is a Map.
     *   <li>The value in a list at element {@code field} (integer) when the receiver is a List.
     * </ul>
     * <p>
     * This method traverses {@code recieverClass}'s class hierarchy (including interfaces)
     * until it finds a matching whitelisted setter. If one is not found, it throws an exception.
     * Otherwise it returns a handle to the matching setter.
     * <p>
     * @param receiverClass Class of the object to retrieve the field from.
     * @param name Name of the field.
     * @return pointer to matching field. never returns null.
     * @throws IllegalArgumentException if no matching whitelisted field was found.
     */
    static MethodHandle lookupSetter(Class<?> receiverClass, String name) {
        // first try whitelist
        for (Class<?> clazz = receiverClass; clazz != null; clazz = clazz.getSuperclass()) {
            RuntimeClass struct = Definition.getRuntimeClass(clazz);

            if (struct != null) {
                MethodHandle handle = struct.setters.get(name);
                if (handle != null) {
                    return handle;
                }
            }

            for (final Class<?> iface : clazz.getInterfaces()) {
                struct = Definition.getRuntimeClass(iface);

                if (struct != null) {
                    MethodHandle handle = struct.setters.get(name);
                    if (handle != null) {
                        return handle;
                    }
                }
            }
        }
        // special case: maps, and lists
        if (Map.class.isAssignableFrom(receiverClass)) {
            // maps allow access like mymap.key
            // wire 'key' as a parameter, its a constant in painless
            return MethodHandles.insertArguments(MAP_PUT, 1, name);
        } else if (List.class.isAssignableFrom(receiverClass)) {
            // lists allow access like mylist.0
            // wire '0' (index) as a parameter, its a constant. this also avoids
            // parsing the same integer millions of times!
            try {
                int index = Integer.parseInt(name);
                return MethodHandles.insertArguments(LIST_SET, 1, index);
            } catch (final NumberFormatException exception) {
                throw new IllegalArgumentException( "Illegal list shortcut value [" + name + "].");
            }
        }

        throw new IllegalArgumentException("Unable to find dynamic field [" + name + "] " +
                                           "for class [" + receiverClass.getCanonicalName() + "].");
    }

    /**
     * Returns a method handle to do an array store.
     * @param receiverClass Class of the array to store the value in
     * @return a MethodHandle that accepts the receiver as first argument, the index as second argument,
     *   and the value to set as 3rd argument. Return value is undefined and should be ignored.
     */
    static MethodHandle lookupArrayStore(Class<?> receiverClass) {
        if (receiverClass.isArray()) {
            return MethodHandles.arrayElementSetter(receiverClass);
        } else if (Map.class.isAssignableFrom(receiverClass)) {
            // maps allow access like mymap[key]
            return MAP_PUT;
        } else if (List.class.isAssignableFrom(receiverClass)) {
            return LIST_SET;
        }
        throw new IllegalArgumentException("Attempting to address a non-array type " +
                                           "[" + receiverClass.getCanonicalName() + "] as an array.");
    }

    /**
     * Returns a method handle to do an array load.
     * @param receiverClass Class of the array to load the value from
     * @return a MethodHandle that accepts the receiver as first argument, the index as second argument.
     *   It returns the loaded value.
     */
    static MethodHandle lookupArrayLoad(Class<?> receiverClass) {
        if (receiverClass.isArray()) {
            return MethodHandles.arrayElementGetter(receiverClass);
        } else if (Map.class.isAssignableFrom(receiverClass)) {
            // maps allow access like mymap[key]
            return MAP_GET;
        } else if (List.class.isAssignableFrom(receiverClass)) {
            return LIST_GET;
        }
        throw new IllegalArgumentException("Attempting to address a non-array type " +
                                           "[" + receiverClass.getCanonicalName() + "] as an array.");
    }
    
    /** Helper class for isolating MethodHandles and methods to get iterators over arrays
     * (to emulate "enhanced for loop" using MethodHandles). These cause boxing, and are not as efficient
     * as they could be, but works.
     */
    @SuppressWarnings("unused") // iterator() methods are are actually used, javac just does not know :)
    private static final class ArrayIteratorHelper {
        private static final Lookup PRIV_LOOKUP = MethodHandles.lookup();

        private static final Map<Class<?>,MethodHandle> ARRAY_TYPE_MH_MAPPING = Collections.unmodifiableMap(
            Stream.of(boolean[].class, byte[].class, short[].class, int[].class, long[].class,
                char[].class, float[].class, double[].class, Object[].class)
                .collect(Collectors.toMap(Function.identity(), type -> {
                    try {
                        return PRIV_LOOKUP.findStatic(PRIV_LOOKUP.lookupClass(), "iterator", MethodType.methodType(Iterator.class, type));
                    } catch (ReflectiveOperationException e) {
                        throw new AssertionError(e);
                    }
                }))
        );

        private static final MethodHandle OBJECT_ARRAY_MH = ARRAY_TYPE_MH_MAPPING.get(Object[].class);

        static Iterator<Boolean> iterator(final boolean[] array) {
            return new Iterator<Boolean>() {
                int index = 0;
                @Override public boolean hasNext() { return index < array.length; }
                @Override public Boolean next() { return array[index++]; }
            };
        }
        static Iterator<Byte> iterator(final byte[] array) {
            return new Iterator<Byte>() {
                int index = 0;
                @Override public boolean hasNext() { return index < array.length; }
                @Override public Byte next() { return array[index++]; }
            };
        }
        static Iterator<Short> iterator(final short[] array) {
            return new Iterator<Short>() {
                int index = 0;
                @Override public boolean hasNext() { return index < array.length; }
                @Override public Short next() { return array[index++]; }
            };
        }
        static Iterator<Integer> iterator(final int[] array) {
            return new Iterator<Integer>() {
                int index = 0;
                @Override public boolean hasNext() { return index < array.length; }
                @Override public Integer next() { return array[index++]; }
            };
        }
        static Iterator<Long> iterator(final long[] array) {
            return new Iterator<Long>() {
                int index = 0;
                @Override public boolean hasNext() { return index < array.length; }
                @Override public Long next() { return array[index++]; }
            };
        }
        static Iterator<Character> iterator(final char[] array) {
            return new Iterator<Character>() {
                int index = 0;
                @Override public boolean hasNext() { return index < array.length; }
                @Override public Character next() { return array[index++]; }
            };
        }
        static Iterator<Float> iterator(final float[] array) {
            return new Iterator<Float>() {
                int index = 0;
                @Override public boolean hasNext() { return index < array.length; }
                @Override public Float next() { return array[index++]; }
            };
        }
        static Iterator<Double> iterator(final double[] array) {
            return new Iterator<Double>() {
                int index = 0;
                @Override public boolean hasNext() { return index < array.length; }
                @Override public Double next() { return array[index++]; }
            };
        }
        static Iterator<Object> iterator(final Object[] array) {
            return new Iterator<Object>() {
                int index = 0;
                @Override public boolean hasNext() { return index < array.length; }
                @Override public Object next() { return array[index++]; }
            };
        }

        static MethodHandle newIterator(Class<?> arrayType) {
            if (!arrayType.isArray()) {
                throw new IllegalArgumentException("type must be an array");
            }
            return (ARRAY_TYPE_MH_MAPPING.containsKey(arrayType)) ?
                ARRAY_TYPE_MH_MAPPING.get(arrayType) :
                OBJECT_ARRAY_MH.asType(OBJECT_ARRAY_MH.type().changeParameterType(0, arrayType));
        }

        private ArrayIteratorHelper() {}
    }
    /**
     * Returns a method handle to do iteration (for enhanced for loop)
     * @param receiverClass Class of the array to load the value from
     * @return a MethodHandle that accepts the receiver as first argument, returns iterator
     */
    static MethodHandle lookupIterator(Class<?> receiverClass) {
        if (Iterable.class.isAssignableFrom(receiverClass)) {
            return ITERATOR;
        } else if (receiverClass.isArray()) {
            return ArrayIteratorHelper.newIterator(receiverClass);
        } else {
            throw new IllegalArgumentException("Cannot iterate over [" + receiverClass.getCanonicalName() + "]");
        }
    }


    // Conversion methods for Def to primitive types.

    public static boolean DefToboolean(final Object value) {
        return (boolean)value;
    }

    public static byte DefTobyteImplicit(final Object value) {
        return (byte)value;
    }

    public static short DefToshortImplicit(final Object value) {
        if (value instanceof Byte) {
            return (byte)value;
        } else {
            return (short)value;
        }
    }

    public static char DefTocharImplicit(final Object value) {
        if (value instanceof Byte) {
            return (char)(byte)value;
        } else {
            return (char)value;
        }
    }

    public static int DefTointImplicit(final Object value) {
        if (value instanceof Byte) {
            return (byte)value;
        } else if (value instanceof Short) {
            return (short)value;
        } else if (value instanceof Character) {
            return (char)value;
        } else {
            return (int)value;
        }
    }

    public static long DefTolongImplicit(final Object value) {
        if (value instanceof Byte) {
            return (byte)value;
        } else if (value instanceof Short) {
            return (short)value;
        } else if (value instanceof Character) {
            return (char)value;
        } else if (value instanceof Integer) {
            return (int)value;
        } else {
            return (long)value;
        }
    }

    public static float DefTofloatImplicit(final Object value) {
        if (value instanceof Byte) {
            return (byte)value;
        } else if (value instanceof Short) {
            return (short)value;
        } else if (value instanceof Character) {
            return (char)value;
        } else if (value instanceof Integer) {
            return (int)value;
        } else if (value instanceof Long) {
            return (long)value;
        } else {
            return (float)value;
        }
    }

    public static double DefTodoubleImplicit(final Object value) {
        if (value instanceof Byte) {
            return (byte)value;
        } else if (value instanceof Short) {
            return (short)value;
        } else if (value instanceof Character) {
            return (char)value;
        } else if (value instanceof Integer) {
            return (int)value;
        } else if (value instanceof Long) {
            return (long)value;
        } else if (value instanceof Float) {
            return (float)value;
        } else {
            return (double)value;
        }
    }

    public static byte DefTobyteExplicit(final Object value) {
        if (value instanceof Character) {
            return (byte)(char)value;
        } else {
            return ((Number)value).byteValue();
        }
    }

    public static short DefToshortExplicit(final Object value) {
        if (value instanceof Character) {
            return (short)(char)value;
        } else {
            return ((Number)value).shortValue();
        }
    }

    public static char DefTocharExplicit(final Object value) {
        if (value instanceof Character) {
            return ((Character)value);
        } else {
            return (char)((Number)value).intValue();
        }
    }

    public static int DefTointExplicit(final Object value) {
        if (value instanceof Character) {
            return (char)value;
        } else {
            return ((Number)value).intValue();
        }
    }

    public static long DefTolongExplicit(final Object value) {
        if (value instanceof Character) {
            return (char)value;
        } else {
            return ((Number)value).longValue();
        }
    }

    public static float DefTofloatExplicit(final Object value) {
        if (value instanceof Character) {
            return (char)value;
        } else {
            return ((Number)value).floatValue();
        }
    }

    public static double DefTodoubleExplicit(final Object value) {
        if (value instanceof Character) {
            return (char)value;
        } else {
            return ((Number)value).doubleValue();
        }
    }
}
