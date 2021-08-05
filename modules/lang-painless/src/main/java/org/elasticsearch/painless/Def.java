/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.symbol.FunctionTable;
import org.elasticsearch.script.JodaCompatibleZonedDateTime;

import java.lang.invoke.CallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.time.ZonedDateTime;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;

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
        private static final MethodHandles.Lookup PRIVATE_METHOD_HANDLES_LOOKUP = MethodHandles.lookup();

        private static final Map<Class<?>,MethodHandle> ARRAY_TYPE_MH_MAPPING = Collections.unmodifiableMap(
            Stream.of(boolean[].class, byte[].class, short[].class, int[].class, long[].class,
                char[].class, float[].class, double[].class, Object[].class)
                .collect(Collectors.toMap(Function.identity(), type -> {
                    try {
                        return PRIVATE_METHOD_HANDLES_LOOKUP.findStatic(
                                PRIVATE_METHOD_HANDLES_LOOKUP.lookupClass(), "getArrayLength", MethodType.methodType(int.class, type));
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
            if (arrayType.isArray() == false) {
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
    /** pointer to {@link Def#mapIndexNormalize}. */
    private static final MethodHandle MAP_INDEX_NORMALIZE;
    /** pointer to {@link Def#listIndexNormalize}. */
    private static final MethodHandle LIST_INDEX_NORMALIZE;
    /** factory for arraylength MethodHandle (intrinsic) from Java 9 (pkg-private for tests) */
    static final MethodHandle JAVA9_ARRAY_LENGTH_MH_FACTORY;

    static {
        final MethodHandles.Lookup methodHandlesLookup = MethodHandles.publicLookup();

        try {
            MAP_GET  = methodHandlesLookup.findVirtual(Map.class , "get", MethodType.methodType(Object.class, Object.class));
            MAP_PUT  = methodHandlesLookup.findVirtual(Map.class , "put", MethodType.methodType(Object.class, Object.class, Object.class));
            LIST_GET = methodHandlesLookup.findVirtual(List.class, "get", MethodType.methodType(Object.class, int.class));
            LIST_SET = methodHandlesLookup.findVirtual(List.class, "set", MethodType.methodType(Object.class, int.class, Object.class));
            ITERATOR = methodHandlesLookup.findVirtual(Iterable.class, "iterator", MethodType.methodType(Iterator.class));
            MAP_INDEX_NORMALIZE = methodHandlesLookup.findStatic(Def.class, "mapIndexNormalize",
                    MethodType.methodType(Object.class, Map.class, Object.class));
            LIST_INDEX_NORMALIZE = methodHandlesLookup.findStatic(Def.class, "listIndexNormalize",
                    MethodType.methodType(int.class, List.class, int.class));
        } catch (final ReflectiveOperationException roe) {
            throw new AssertionError(roe);
        }

        // lookup up the factory for arraylength MethodHandle (intrinsic) from Java 9:
        // https://bugs.openjdk.java.net/browse/JDK-8156915
        MethodHandle arrayLengthMHFactory;
        try {
            arrayLengthMHFactory = methodHandlesLookup.findStatic(MethodHandles.class, "arrayLength",
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
     * Looks up handle for a dynamic method call, with lambda replacement
     * <p>
     * A dynamic method call for variable {@code x} of type {@code def} looks like:
     * {@code x.method(args...)}
     * <p>
     * This method traverses {@code recieverClass}'s class hierarchy (including interfaces)
     * until it finds a matching whitelisted method. If one is not found, it throws an exception.
     * Otherwise it returns a handle to the matching method.
     * <p>
     * @param painlessLookup the whitelist
     * @param functions user defined functions and lambdas
     * @param constants available constants to be used if the method has the {@code InjectConstantAnnotation}
     * @param methodHandlesLookup caller's lookup
     * @param callSiteType callsite's type
     * @param receiverClass Class of the object to invoke the method on.
     * @param name Name of the method.
     * @param args bootstrap args passed to callsite
     * @return pointer to matching method to invoke. never returns null.
     * @throws IllegalArgumentException if no matching whitelisted method was found.
     * @throws Throwable if a method reference cannot be converted to an functional interface
     */
    static MethodHandle lookupMethod(PainlessLookup painlessLookup, FunctionTable functions, Map<String, Object> constants,
            MethodHandles.Lookup methodHandlesLookup, MethodType callSiteType, Class<?> receiverClass, String name, Object[] args)
            throws Throwable {

         String recipeString = (String) args[0];
         int numArguments = callSiteType.parameterCount();
         // simple case: no lambdas
         if (recipeString.isEmpty()) {
             PainlessMethod painlessMethod = painlessLookup.lookupRuntimePainlessMethod(receiverClass, name, numArguments - 1);

             if (painlessMethod == null) {
                 throw new IllegalArgumentException("dynamic method " +
                         "[" + typeToCanonicalTypeName(receiverClass) + ", " + name + "/" + (numArguments - 1) + "] not found");
             }

             MethodHandle handle = painlessMethod.methodHandle;
             Object[] injections = PainlessLookupUtility.buildInjections(painlessMethod, constants);

             if (injections.length > 0) {
                 // method handle contains the "this" pointer so start injections at 1
                 handle = MethodHandles.insertArguments(handle, 1, injections);
             }

             return handle;
         }

         // convert recipe string to a bitset for convenience (the code below should be refactored...)
         BitSet lambdaArgs = new BitSet(recipeString.length());
         for (int i = 0; i < recipeString.length(); i++) {
             lambdaArgs.set(recipeString.charAt(i));
         }

         // otherwise: first we have to compute the "real" arity. This is because we have extra arguments:
         // e.g. f(a, g(x), b, h(y), i()) looks like f(a, g, x, b, h, y, i).
         int arity = callSiteType.parameterCount() - 1;
         int upTo = 1;
         for (int i = 1; i < numArguments; i++) {
             if (lambdaArgs.get(i - 1)) {
                 Def.Encoding signature = new Def.Encoding((String) args[upTo++]);
                 arity -= signature.numCaptures;
                 // arity in painlessLookup does not include 'this' reference
                 if (signature.needsInstance) {
                     arity--;
                 }
             }
         }

         // lookup the method with the proper arity, then we know everything (e.g. interface types of parameters).
         // based on these we can finally link any remaining lambdas that were deferred.
         PainlessMethod method = painlessLookup.lookupRuntimePainlessMethod(receiverClass, name, arity);

        if (method == null) {
            throw new IllegalArgumentException(
                    "dynamic method [" + typeToCanonicalTypeName(receiverClass) + ", " + name + "/" + arity + "] not found");
        }

        MethodHandle handle = method.methodHandle;
        Object[] injections = PainlessLookupUtility.buildInjections(method, constants);

        if (injections.length > 0) {
            // method handle contains the "this" pointer so start injections at 1
            handle = MethodHandles.insertArguments(handle, 1, injections);
        }

         int replaced = 0;
         upTo = 1;
         for (int i = 1; i < numArguments; i++) {
             // its a functional reference, replace the argument with an impl
             if (lambdaArgs.get(i - 1)) {
                 Def.Encoding defEncoding = new Encoding((String) args[upTo++]);
                 MethodHandle filter;
                 Class<?> interfaceType = method.typeParameters.get(i - 1 - replaced - (defEncoding.needsInstance ? 1 : 0));
                 if (defEncoding.isStatic) {
                     // the implementation is strongly typed, now that we know the interface type,
                     // we have everything.
                     filter = lookupReferenceInternal(painlessLookup,
                                                      functions,
                                                      constants,
                                                      methodHandlesLookup,
                                                      interfaceType,
                                                      defEncoding.symbol,
                                                      defEncoding.methodName,
                                                      defEncoding.numCaptures,
                                                      defEncoding.needsInstance
                     );
                } else {
                     // the interface type is now known, but we need to get the implementation.
                     // this is dynamically based on the receiver type (and cached separately, underneath
                     // this cache). It won't blow up since we never nest here (just references)
                     Class<?>[] captures = new Class<?>[defEncoding.numCaptures];
                     for (int capture = 0; capture < captures.length; capture++) {
                         captures[capture] = callSiteType.parameterType(i + 1 + capture);
                     }
                     MethodType nestedType = MethodType.methodType(interfaceType, captures);
                     CallSite nested = DefBootstrap.bootstrap(painlessLookup,
                                                              functions,
                                                              constants,
                                                              methodHandlesLookup,
                                                              defEncoding.methodName,
                                                              nestedType,
                                                              0,
                                                              DefBootstrap.REFERENCE,
                                                              PainlessLookupUtility.typeToCanonicalTypeName(interfaceType));
                     filter = nested.dynamicInvoker();
                }
                 // the filter now ignores the signature (placeholder) on the stack
                 filter = MethodHandles.dropArguments(filter, 0, String.class);
                 handle = MethodHandles.collectArguments(handle, i - (defEncoding.needsInstance ? 1 : 0), filter);
                 i += defEncoding.numCaptures;
                 replaced += defEncoding.numCaptures;
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
    static MethodHandle lookupReference(PainlessLookup painlessLookup, FunctionTable functions, Map<String, Object> constants,
            MethodHandles.Lookup methodHandlesLookup, String interfaceClass, Class<?> receiverClass, String name)
            throws Throwable {

        Class<?> interfaceType = painlessLookup.canonicalTypeNameToType(interfaceClass);
        if (interfaceType == null) {
            throw new IllegalArgumentException("type [" + interfaceClass + "] not found");
        }
        PainlessMethod interfaceMethod = painlessLookup.lookupFunctionalInterfacePainlessMethod(interfaceType);
        if (interfaceMethod == null) {
            throw new IllegalArgumentException("Class [" + interfaceClass + "] is not a functional interface");
        }
        int arity = interfaceMethod.typeParameters.size();
        PainlessMethod implMethod = painlessLookup.lookupRuntimePainlessMethod(receiverClass, name, arity);
        if (implMethod == null) {
            throw new IllegalArgumentException(
                    "dynamic method [" + typeToCanonicalTypeName(receiverClass) + ", " + name + "/" + arity + "] not found");
        }

        return lookupReferenceInternal(painlessLookup, functions, constants,
                methodHandlesLookup, interfaceType, PainlessLookupUtility.typeToCanonicalTypeName(implMethod.targetClass),
                implMethod.javaMethod.getName(), 1, false);
     }

     /** Returns a method handle to an implementation of clazz, given method reference signature. */
    private static MethodHandle lookupReferenceInternal(
            PainlessLookup painlessLookup, FunctionTable functions, Map<String, Object> constants,
            MethodHandles.Lookup methodHandlesLookup, Class<?> clazz, String type, String call, int captures,
            boolean needsScriptInstance) throws Throwable {

        final FunctionRef ref =
                FunctionRef.create(painlessLookup, functions, null, clazz, type, call, captures, constants, needsScriptInstance);
        Class<?>[] parameters = ref.factoryMethodParameters(needsScriptInstance ? methodHandlesLookup.lookupClass() : null);
        MethodType factoryMethodType = MethodType.methodType(clazz, parameters);
        final CallSite callSite = LambdaBootstrap.lambdaBootstrap(
                methodHandlesLookup,
                ref.interfaceMethodName,
                factoryMethodType,
                ref.interfaceMethodType,
                ref.delegateClassName,
                ref.delegateInvokeType,
                ref.delegateMethodName,
                ref.delegateMethodType,
                ref.isDelegateInterface ? 1 : 0,
                ref.isDelegateAugmented ? 1 : 0,
                ref.delegateInjections
        );
        return callSite.dynamicInvoker().asType(MethodType.methodType(clazz, parameters));
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
     * @param painlessLookup the whitelist
     * @param receiverClass Class of the object to retrieve the field from.
     * @param name Name of the field.
     * @return pointer to matching field. never returns null.
     * @throws IllegalArgumentException if no matching whitelisted field was found.
     */
    static MethodHandle lookupGetter(PainlessLookup painlessLookup, Class<?> receiverClass, String name) {
        // first try whitelist
        MethodHandle getter = painlessLookup.lookupRuntimeGetterMethodHandle(receiverClass, name);

        if (getter != null) {
            return getter;
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
                throw new IllegalArgumentException("Illegal list shortcut value [" + name + "].");
            }
        }

        throw new IllegalArgumentException(
                "dynamic getter [" + typeToCanonicalTypeName(receiverClass) + ", " + name + "] not found");
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
     * @param painlessLookup the whitelist
     * @param receiverClass Class of the object to retrieve the field from.
     * @param name Name of the field.
     * @return pointer to matching field. never returns null.
     * @throws IllegalArgumentException if no matching whitelisted field was found.
     */
    static MethodHandle lookupSetter(PainlessLookup painlessLookup, Class<?> receiverClass, String name) {
        // first try whitelist
        MethodHandle setter = painlessLookup.lookupRuntimeSetterMethodHandle(receiverClass, name);

        if (setter != null) {
            return setter;
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
                throw new IllegalArgumentException("Illegal list shortcut value [" + name + "].");
            }
        }

        throw new IllegalArgumentException(
                "dynamic setter [" + typeToCanonicalTypeName(receiverClass) + ", " + name + "] not found");
    }

    /**
     * Returns a method handle to normalize the index into an array. This is what makes lists and arrays stored in {@code def} support
     * negative offsets.
     * @param receiverClass Class of the array to store the value in
     * @return a MethodHandle that accepts the receiver as first argument, the index as second argument, and returns the normalized index
     *   to use with array loads and array stores
     */
    static MethodHandle lookupIndexNormalize(Class<?> receiverClass) {
        if (receiverClass.isArray()) {
            return ArrayIndexNormalizeHelper.arrayIndexNormalizer(receiverClass);
        } else if (Map.class.isAssignableFrom(receiverClass)) {
            // noop so that mymap[key] doesn't do funny things with negative keys
            return MAP_INDEX_NORMALIZE;
        } else if (List.class.isAssignableFrom(receiverClass)) {
            return LIST_INDEX_NORMALIZE;
        }
        throw new IllegalArgumentException("Attempting to address a non-array-like type " +
                                           "[" + receiverClass.getCanonicalName() + "] as an array.");
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
        private static final MethodHandles.Lookup PRIVATE_METHOD_HANDLES_LOOKUP = MethodHandles.lookup();

        private static final Map<Class<?>,MethodHandle> ARRAY_TYPE_MH_MAPPING = Collections.unmodifiableMap(
            Stream.of(boolean[].class, byte[].class, short[].class, int[].class, long[].class,
                char[].class, float[].class, double[].class, Object[].class)
                .collect(Collectors.toMap(Function.identity(), type -> {
                    try {
                        return PRIVATE_METHOD_HANDLES_LOOKUP.findStatic(
                                PRIVATE_METHOD_HANDLES_LOOKUP.lookupClass(), "iterator", MethodType.methodType(Iterator.class, type));
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
            if (arrayType.isArray() == false) {
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

    // Conversion methods for def to primitive types.

    public static boolean defToboolean(final Object value) {
        if (value instanceof Boolean) {
            return (boolean)value;
        } else {
            throw new ClassCastException("cannot cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    boolean.class.getCanonicalName());
        }
    }

    public static byte defTobyteImplicit(final Object value) {
        if (value instanceof Byte) {
            return (byte)value;
        } else {
            throw new ClassCastException("cannot implicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    byte.class.getCanonicalName());
        }
    }

    public static short defToshortImplicit(final Object value) {
        if (value instanceof Byte) {
            return (byte)value;
        } else if (value instanceof Short) {
            return (short)value;
        } else {
            throw new ClassCastException("cannot implicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    short.class.getCanonicalName());
        }
    }

    public static char defTocharImplicit(final Object value) {
        if (value instanceof Character) {
            return (char)value;
        } else {
            throw new ClassCastException("cannot implicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    char.class.getCanonicalName());
        }
    }

    public static int defTointImplicit(final Object value) {
        if (value instanceof Byte) {
            return (byte)value;
        } else if (value instanceof Short) {
            return (short)value;
        } else if (value instanceof Character) {
            return (char)value;
        } else if (value instanceof Integer) {
            return (int)value;
        } else {
            throw new ClassCastException("cannot implicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    int.class.getCanonicalName());
        }
    }

    public static long defTolongImplicit(final Object value) {
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
            throw new ClassCastException("cannot implicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    long.class.getCanonicalName());
        }
    }

    public static float defTofloatImplicit(final Object value) {
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
            throw new ClassCastException("cannot implicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    float.class.getCanonicalName());
        }
    }

    public static double defTodoubleImplicit(final Object value) {
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
        } else if (value instanceof Double) {
            return (double)value;
        } else {
            throw new ClassCastException("cannot implicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    double.class.getCanonicalName());
        }
    }

    public static byte defTobyteExplicit(final Object value) {
        if (value instanceof Character) {
            return (byte)(char)value;
        } else if (
                value instanceof Byte    ||
                value instanceof Short   ||
                value instanceof Integer ||
                value instanceof Long    ||
                value instanceof Float   ||
                value instanceof Double
        ) {
            return ((Number)value).byteValue();
        } else {
            throw new ClassCastException("cannot explicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    byte.class.getCanonicalName());
        }
    }

    public static short defToshortExplicit(final Object value) {
        if (value instanceof Character) {
            return (short)(char)value;
        } else if (
                value instanceof Byte    ||
                value instanceof Short   ||
                value instanceof Integer ||
                value instanceof Long    ||
                value instanceof Float   ||
                value instanceof Double
        ) {
            return ((Number)value).shortValue();
        } else {
            throw new ClassCastException("cannot explicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    short.class.getCanonicalName());
        }
    }

    public static char defTocharExplicit(final Object value) {
        if (value instanceof String) {
            return Utility.StringTochar((String)value);
        } else if (value instanceof Character) {
            return (char)value;
        } else if (
                value instanceof Byte    ||
                value instanceof Short   ||
                value instanceof Integer ||
                value instanceof Long    ||
                value instanceof Float   ||
                value instanceof Double
        ) {
            return (char)((Number)value).intValue();
        } else {
            throw new ClassCastException("cannot explicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    char.class.getCanonicalName());
        }
    }

    public static int defTointExplicit(final Object value) {
        if (value instanceof Character) {
            return (char)value;
        } else if (
                value instanceof Byte    ||
                value instanceof Short   ||
                value instanceof Integer ||
                value instanceof Long    ||
                value instanceof Float   ||
                value instanceof Double
        ) {
            return ((Number)value).intValue();
        } else {
            throw new ClassCastException("cannot explicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    int.class.getCanonicalName());
        }
    }

    public static long defTolongExplicit(final Object value) {
        if (value instanceof Character) {
            return (char)value;
        } else if (
                value instanceof Byte    ||
                value instanceof Short   ||
                value instanceof Integer ||
                value instanceof Long    ||
                value instanceof Float   ||
                value instanceof Double
        ) {
            return ((Number)value).longValue();
        } else {
            throw new ClassCastException("cannot explicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    long.class.getCanonicalName());
        }
    }

    public static float defTofloatExplicit(final Object value) {
        if (value instanceof Character) {
            return (char)value;
        } else if (
                value instanceof Byte    ||
                value instanceof Short   ||
                value instanceof Integer ||
                value instanceof Long    ||
                value instanceof Float   ||
                value instanceof Double
        ) {
            return ((Number)value).floatValue();
        } else {
            throw new ClassCastException("cannot explicitly cast " +
                    "float [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    byte.class.getCanonicalName());
        }
    }

    public static double defTodoubleExplicit(final Object value) {
        if (value instanceof Character) {
            return (char)value;
        } else if (
                value instanceof Byte    ||
                value instanceof Short   ||
                value instanceof Integer ||
                value instanceof Long    ||
                value instanceof Float   ||
                value instanceof Double
        ) {
            return ((Number)value).doubleValue();
        } else {
            throw new ClassCastException("cannot explicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    byte.class.getCanonicalName());
        }
    }

    // Conversion methods for def to boxed types.

    public static Boolean defToBoolean(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Boolean) {
            return (Boolean)value;
        } else {
            throw new ClassCastException("cannot implicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    Boolean.class.getCanonicalName());
        }
    }

    public static Byte defToByteImplicit(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Byte) {
            return (Byte)value;
        } else {
            throw new ClassCastException("cannot implicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    Byte.class.getCanonicalName());
        }
    }

    public static Short defToShortImplicit(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Byte) {
            return (short)(byte)value;
        } else if (value instanceof Short) {
            return (Short)value;
        } else {
            throw new ClassCastException("cannot implicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    Short.class.getCanonicalName());
        }
    }

    public static Character defToCharacterImplicit(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Character) {
            return (Character)value;
        } else {
            throw new ClassCastException("cannot implicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    Character.class.getCanonicalName());
        }
    }

    public static Integer defToIntegerImplicit(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Byte) {
            return (int)(byte)value;
        } else if (value instanceof Short) {
            return (int)(short)value;
        } else if (value instanceof Character) {
            return (int)(char)value;
        } else if (value instanceof Integer) {
            return (Integer)value;
        } else {
            throw new ClassCastException("cannot implicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    Integer.class.getCanonicalName());
        }
    }

    public static Long defToLongImplicit(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Byte) {
            return (long)(byte)value;
        } else if (value instanceof Short) {
            return (long)(short)value;
        } else if (value instanceof Character) {
            return (long)(char)value;
        } else if (value instanceof Integer) {
            return (long)(int)value;
        } else if (value instanceof Long) {
            return (Long)value;
        } else {
            throw new ClassCastException("cannot implicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    Long.class.getCanonicalName());
        }
    }

    public static Float defToFloatImplicit(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Byte) {
            return (float)(byte)value;
        } else if (value instanceof Short) {
            return (float)(short)value;
        } else if (value instanceof Character) {
            return (float)(char)value;
        } else if (value instanceof Integer) {
            return (float)(int)value;
        } else if (value instanceof Long) {
            return (float)(long)value;
        } else if (value instanceof Float) {
            return (Float)value;
        } else {
            throw new ClassCastException("cannot implicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    Float.class.getCanonicalName());
        }
    }

    public static Double defToDoubleImplicit(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Byte) {
            return (double)(byte)value;
        } else if (value instanceof Short) {
            return (double)(short)value;
        } else if (value instanceof Character) {
            return (double)(char)value;
        } else if (value instanceof Integer) {
            return (double)(int)value;
        } else if (value instanceof Long) {
            return (double)(long)value;
        } else if (value instanceof Float) {
            return (double)(float)value;
        } else if (value instanceof Double) {
            return (Double) value;
        } else {
            throw new ClassCastException("cannot implicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    Double.class.getCanonicalName());
        }
    }

    public static Byte defToByteExplicit(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Character) {
            return (byte)(char)value;
        } else if (
            value instanceof Byte    ||
            value instanceof Short   ||
            value instanceof Integer ||
            value instanceof Long    ||
            value instanceof Float   ||
            value instanceof Double
        ) {
            return ((Number)value).byteValue();
        } else {
            throw new ClassCastException("cannot explicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    Byte.class.getCanonicalName());
        }
    }

    public static Short defToShortExplicit(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Character) {
            return (short)(char)value;
        } else if (
                value instanceof Byte    ||
                value instanceof Short   ||
                value instanceof Integer ||
                value instanceof Long    ||
                value instanceof Float   ||
                value instanceof Double
        ) {
            return ((Number)value).shortValue();
        } else {
            throw new ClassCastException("cannot explicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    Short.class.getCanonicalName());
        }
    }

    public static Character defToCharacterExplicit(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof String) {
            return Utility.StringTochar((String)value);
        } else if (value instanceof Character) {
            return (Character)value;
        } else if (
                value instanceof Byte    ||
                value instanceof Short   ||
                value instanceof Integer ||
                value instanceof Long    ||
                value instanceof Float   ||
                value instanceof Double
        ) {
            return (char)((Number)value).intValue();
        } else {
            throw new ClassCastException("cannot explicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    Character.class.getCanonicalName());
        }
    }

    public static Integer defToIntegerExplicit(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Character) {
            return (int)(char)value;
        } else if (
                value instanceof Byte    ||
                value instanceof Short   ||
                value instanceof Integer ||
                value instanceof Long    ||
                value instanceof Float   ||
                value instanceof Double
        ) {
            return ((Number)value).intValue();
        } else {
            throw new ClassCastException("cannot explicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    Integer.class.getCanonicalName());
        }
    }

    public static Long defToLongExplicit(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Character) {
            return (long)(char)value;
        } else if (
                value instanceof Byte    ||
                value instanceof Short   ||
                value instanceof Integer ||
                value instanceof Long    ||
                value instanceof Float   ||
                value instanceof Double
        ) {
            return ((Number)value).longValue();
        } else {
            throw new ClassCastException("cannot explicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    Long.class.getCanonicalName());
        }
    }

    public static Float defToFloatExplicit(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Character) {
            return (float)(char)value;
        } else if (
                value instanceof Byte    ||
                value instanceof Short   ||
                value instanceof Integer ||
                value instanceof Long    ||
                value instanceof Float   ||
                value instanceof Double
        ) {
            return ((Number)value).floatValue();
        } else {
            throw new ClassCastException("cannot explicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    Float.class.getCanonicalName());
        }
    }

    public static Double defToDoubleExplicit(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Character) {
            return (double)(char)value;
        } else if (
                value instanceof Byte    ||
                value instanceof Short   ||
                value instanceof Integer ||
                value instanceof Long    ||
                value instanceof Float   ||
                value instanceof Double
        ) {
            return ((Number)value).doubleValue();
        } else {
            throw new ClassCastException("cannot explicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    Double.class.getCanonicalName());
        }
    }

    public static String defToStringImplicit(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof String) {
            return (String)value;
        } else {
            throw new ClassCastException("cannot implicitly cast " +
                    "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                    String.class.getCanonicalName());
        }
    }

    public static String defToStringExplicit(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Character) {
            return Utility.charToString((char)value);
        } else if (value instanceof String) {
            return (String)value;
        } else {
             throw new ClassCastException("cannot explicitly cast " +
                     "def [" + PainlessLookupUtility.typeToUnboxedType(value.getClass()).getCanonicalName() + "] to " +
                     String.class.getCanonicalName());
        }
    }

    // TODO: remove this when the transition from Joda to Java datetimes is completed
    public static ZonedDateTime defToZonedDateTime(final Object value) {
        if (value instanceof JodaCompatibleZonedDateTime) {
            return ((JodaCompatibleZonedDateTime)value).getZonedDateTime();
        }

        return (ZonedDateTime)value;
    }

    /**
     * "Normalizes" the index into a {@code Map} by making no change to the index.
     */
    public static Object mapIndexNormalize(final Map<?, ?> value, Object index) {
        return index;
    }

    /**
     * "Normalizes" the idnex into a {@code List} by flipping negative indexes around so they are "from the end" of the list.
     */
    public static int listIndexNormalize(final List<?> value, int index) {
        return index >= 0 ? index : value.size() + index;
    }

    /**
     * Methods to normalize array indices to support negative indices into arrays stored in {@code def}s.
     */
    @SuppressWarnings("unused") // normalizeIndex() methods are are actually used, javac just does not know :)
    private static final class ArrayIndexNormalizeHelper {
        private static final MethodHandles.Lookup PRIVATE_METHOD_HANDLES_LOOKUP = MethodHandles.lookup();

        private static final Map<Class<?>,MethodHandle> ARRAY_TYPE_MH_MAPPING = Collections.unmodifiableMap(
            Stream.of(boolean[].class, byte[].class, short[].class, int[].class, long[].class,
                char[].class, float[].class, double[].class, Object[].class)
                .collect(Collectors.toMap(Function.identity(), type -> {
                    try {
                        return PRIVATE_METHOD_HANDLES_LOOKUP.findStatic(PRIVATE_METHOD_HANDLES_LOOKUP.lookupClass(), "normalizeIndex",
                                MethodType.methodType(int.class, type, int.class));
                    } catch (ReflectiveOperationException e) {
                        throw new AssertionError(e);
                    }
                }))
        );

        private static final MethodHandle OBJECT_ARRAY_MH = ARRAY_TYPE_MH_MAPPING.get(Object[].class);

        static int normalizeIndex(final boolean[] array, final int index) { return index >= 0 ? index : index + array.length; }
        static int normalizeIndex(final byte[] array, final int index) { return index >= 0 ? index : index + array.length; }
        static int normalizeIndex(final short[] array, final int index) { return index >= 0 ? index : index + array.length; }
        static int normalizeIndex(final int[] array, final int index) { return index >= 0 ? index : index + array.length; }
        static int normalizeIndex(final long[] array, final int index) { return index >= 0 ? index : index + array.length; }
        static int normalizeIndex(final char[] array, final int index) { return index >= 0 ? index : index + array.length; }
        static int normalizeIndex(final float[] array, final int index) { return index >= 0 ? index : index + array.length; }
        static int normalizeIndex(final double[] array, final int index) { return index >= 0 ? index : index + array.length; }
        static int normalizeIndex(final Object[] array, final int index) { return index >= 0 ? index : index + array.length; }

        static MethodHandle arrayIndexNormalizer(Class<?> arrayType) {
            if (arrayType.isArray() == false) {
                throw new IllegalArgumentException("type must be an array");
            }
            return (ARRAY_TYPE_MH_MAPPING.containsKey(arrayType)) ?
                ARRAY_TYPE_MH_MAPPING.get(arrayType) :
                OBJECT_ARRAY_MH.asType(OBJECT_ARRAY_MH.type().changeParameterType(0, arrayType));
        }

        private ArrayIndexNormalizeHelper() {}
    }


    public static class Encoding {
        public final boolean isStatic;
        public final boolean needsInstance;
        public final String symbol;
        public final String methodName;
        public final int numCaptures;

        /**
         * Encoding is passed to invokedynamic to help DefBootstrap find the method.  invokedynamic can only take
         * "Class, java.lang.invoke.MethodHandle, java.lang.invoke.MethodType, String, int, long, float, or double" types to
         * help find the callsite, which is why this object is encoded as a String for indy.
         * See: https://docs.oracle.com/javase/specs/jvms/se7/html/jvms-6.html#jvms-6.5.invokedynamic
         * */
        public final String encoding;

        private static final String FORMAT = "[SD][tf]symbol.methodName,numCaptures";

        public Encoding(boolean isStatic, boolean needsInstance, String symbol, String methodName, int numCaptures) {
            this.isStatic = isStatic;
            this.needsInstance = needsInstance;
            this.symbol = Objects.requireNonNull(symbol);
            this.methodName = Objects.requireNonNull(methodName);
            this.numCaptures = numCaptures;
            this.encoding = (isStatic ? "S" : "D") + (needsInstance ? "t" : "f") +
                    symbol + "." +
                    methodName + "," +
                    numCaptures;


            if ("this".equals(symbol)) {
                if (isStatic == false) {
                    throw new IllegalArgumentException("Def.Encoding must be static if symbol is 'this', encoding [" + encoding + "]");
                }
            } else {
                if (needsInstance) {
                    throw new IllegalArgumentException("Def.Encoding symbol must be 'this', not [" + symbol + "] if needsInstance," +
                        " encoding [" + encoding + "]");
                }
            }

            if (methodName.isEmpty()) {
                throw new IllegalArgumentException("methodName must be non-empty, encoding [" + encoding + "]");
            }
            if (numCaptures < 0) {
                throw new IllegalArgumentException("numCaptures must be non-negative, not [" + numCaptures + "]," +
                    " encoding: [" + encoding + "]");
            }
        }

        // Parsing constructor, does minimal validation to avoid extra work during runtime
        public Encoding(String encoding) {
            this.encoding = Objects.requireNonNull(encoding);
            if (encoding.length() < 6) {
                throw new IllegalArgumentException("Encoding too short. Minimum 6, given [" + encoding.length() + "]," +
                    " encoding: [" + encoding + "], format: " + FORMAT + "");
            }

            // 'S' or 'D'
            this.isStatic = encoding.charAt(0) == 'S';

            // 't' or 'f'
            this.needsInstance = encoding.charAt(1) == 't';

            int dotIndex = encoding.lastIndexOf('.');
            if (dotIndex < 2) {
                throw new IllegalArgumentException("Invalid symbol, could not find '.' at expected position after index 1, instead found" +
                    " index [" + dotIndex + "], encoding: [" + encoding + "], format: " + FORMAT);
            }

            this.symbol = encoding.substring(2, dotIndex);

            int commaIndex = encoding.indexOf(',');
            if (commaIndex <= dotIndex) {
                throw new IllegalArgumentException("Invalid symbol, could not find ',' at expected position after '.' at" +
                    " [" + dotIndex + "], instead found index [" + commaIndex + "], encoding: [" + encoding + "], format: " + FORMAT);
            }

            this.methodName = encoding.substring(dotIndex + 1, commaIndex);

            if (commaIndex == encoding.length() - 1) {
                throw new IllegalArgumentException("Invalid symbol, could not find ',' at expected position, instead found" +
                    " index [" + commaIndex + "], encoding: [" + encoding + "], format: " + FORMAT);
            }

            this.numCaptures = Integer.parseUnsignedInt(encoding.substring(commaIndex + 1));
        }

        @Override
        public String toString() {
            return encoding;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if ((o instanceof Encoding) == false) return false;
            Encoding encoding1 = (Encoding) o;
            return isStatic == encoding1.isStatic && needsInstance == encoding1.needsInstance && numCaptures == encoding1.numCaptures
                && Objects.equals(symbol, encoding1.symbol) && Objects.equals(methodName, encoding1.methodName)
                && Objects.equals(encoding, encoding1.encoding);
        }

        @Override
        public int hashCode() {
            return Objects.hash(isStatic, needsInstance, symbol, methodName, numCaptures, encoding);
        }
    }
}
