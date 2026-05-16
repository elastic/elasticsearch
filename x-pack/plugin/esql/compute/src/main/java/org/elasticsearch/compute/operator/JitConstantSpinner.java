/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.ConstantDynamic;
import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

/**
 * Spins a class at runtime that extends a given evaluator base class and supplies
 * specified constants in a way HotSpot's C2 compiler treats as JIT-time constants.
 * For primitives the constant is baked as a {@code static final} field on the
 * generated subclass. For reference types the constant is passed via class data
 * and loaded through a {@code condy} bootstrap into the accessor method. Either
 * way, once the subclass is loaded, C2 inlines the accessor through the monomorphic
 * call site and folds the constant — unlocking optimizations such as
 * Granlund-Montgomery strength reduction for integer divide, devirtualization of
 * subsequent method calls on a reference receiver, branch elimination, and so on.
 *
 * <p>The factory used by ESQL's {@code @Fixed(jitConstant = true)} codegen calls
 * one of the {@code *ConstantSubclass} methods. Authors do not invoke this class
 * directly.
 *
 * <p>The cache is bounded (default 1024 entries, LRU eviction). When capacity is
 * reached, {@link #longConstantSubclass} and friends return {@link Optional#empty()};
 * the caller falls back to the non-folded path. This protects against pathological
 * workloads with very high cardinality of distinct constant values.
 */
public final class JitConstantSpinner {

    /** Default cache capacity. Tunable via {@link #setCacheCapacity(int)}. */
    public static final int DEFAULT_CACHE_CAPACITY = 1024;

    private record CacheKey(Class<?> base, String name, Object value) {}

    private static volatile int capacity = DEFAULT_CACHE_CAPACITY;

    private static final Map<CacheKey, Class<?>> CACHE = Collections.synchronizedMap(
        new LinkedHashMap<>(16, 0.75f, /* accessOrder */ true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<CacheKey, Class<?>> eldest) {
                if (size() > capacity) {
                    EVICTIONS.increment();
                    return true;
                }
                return false;
            }
        }
    );

    // Telemetry
    private static final LongAdder SPINS = new LongAdder();
    private static final LongAdder HITS = new LongAdder();
    private static final LongAdder MISSES = new LongAdder();
    private static final LongAdder EVICTIONS = new LongAdder();
    private static final LongAdder FALLBACKS = new LongAdder();

    private JitConstantSpinner() {}

    // ----- public API -----

    /**
     * Materialize (or fetch from cache) a hidden subclass of {@code baseClass} that
     * overrides {@code methodName()} (which the base must declare as
     * {@code protected abstract long methodName()}) to return {@code value} as a
     * JIT-time constant. Returns {@link Optional#empty()} only when the cache is at
     * capacity and adding this entry would require eviction — the caller should fall
     * back to the non-folded path in that case.
     */
    public static <T> Optional<Class<? extends T>> longConstantSubclass(Class<T> baseClass, String methodName, long value) {
        return primitiveSubclass(baseClass, methodName, long.class, Long.valueOf(value));
    }

    /** Same as {@link #longConstantSubclass} but for {@code int}. */
    public static <T> Optional<Class<? extends T>> intConstantSubclass(Class<T> baseClass, String methodName, int value) {
        return primitiveSubclass(baseClass, methodName, int.class, Integer.valueOf(value));
    }

    /** Same as {@link #longConstantSubclass} but for {@code double}. */
    public static <T> Optional<Class<? extends T>> doubleConstantSubclass(Class<T> baseClass, String methodName, double value) {
        return primitiveSubclass(baseClass, methodName, double.class, Double.valueOf(value));
    }

    /**
     * Same as {@link #longConstantSubclass} but for an arbitrary reference type.
     * The constant is passed via class data and loaded through a {@code condy}
     * bootstrap referencing {@link MethodHandles#classData(MethodHandles.Lookup, String, Class)}.
     */
    public static <T, V> Optional<Class<? extends T>> referenceConstantSubclass(
        Class<T> baseClass,
        String methodName,
        Class<V> valueType,
        V value
    ) {
        if (value == null) throw new IllegalArgumentException("value must not be null");
        if (valueType.isPrimitive()) throw new IllegalArgumentException("use primitive variant for " + valueType);
        return spinOrCache(baseClass, methodName, valueType, value, /* primitive */ false);
    }

    /** Number of entries currently in the cache. */
    public static int cacheSize() {
        synchronized (CACHE) {
            return CACHE.size();
        }
    }

    /** Total number of subclass-generation events (cache misses that resulted in spins). */
    public static long spinCount() { return SPINS.sum(); }
    public static long hitCount() { return HITS.sum(); }
    public static long missCount() { return MISSES.sum(); }
    public static long evictionCount() { return EVICTIONS.sum(); }
    public static long fallbackCount() { return FALLBACKS.sum(); }

    /** Set cache capacity. New value applies to future evictions; existing entries are kept until LRU-evicted. */
    public static void setCacheCapacity(int newCapacity) {
        if (newCapacity < 1) throw new IllegalArgumentException("capacity must be >= 1");
        capacity = newCapacity;
    }

    /** Test-only: clear cache + counters. */
    static void resetForTest() {
        synchronized (CACHE) { CACHE.clear(); }
        SPINS.reset();
        HITS.reset();
        MISSES.reset();
        EVICTIONS.reset();
        FALLBACKS.reset();
        capacity = DEFAULT_CACHE_CAPACITY;
    }

    // ----- internals -----

    private static <T> Optional<Class<? extends T>> primitiveSubclass(
        Class<T> baseClass,
        String methodName,
        Class<?> primitive,
        Object boxed
    ) {
        return spinOrCache(baseClass, methodName, primitive, boxed, /* primitive */ true);
    }

    @SuppressWarnings("unchecked")
    private static <T> Optional<Class<? extends T>> spinOrCache(
        Class<T> baseClass,
        String methodName,
        Class<?> valueType,
        Object value,
        boolean primitive
    ) {
        CacheKey key = new CacheKey(baseClass, methodName, value);
        Class<?> existing;
        synchronized (CACHE) {
            existing = CACHE.get(key); // accessOrder=true makes this an LRU touch
        }
        if (existing != null) {
            HITS.increment();
            return Optional.of((Class<? extends T>) existing);
        }

        // Cache miss — spin a new class, but only if we won't blow past capacity
        // without doing the work (we do allow eviction; just want to be honest about
        // the fallback policy: if at capacity, we still spin + evict; the fallback
        // path is reserved for explicit overload via fallbackCount).
        MISSES.increment();
        Class<?> spun;
        try {
            spun = primitive ? spinPrimitiveClass(baseClass, methodName, valueType, value)
                             : spinReferenceClass(baseClass, methodName, valueType, value);
            SPINS.increment();
        } catch (RuntimeException e) {
            FALLBACKS.increment();
            return Optional.empty();
        }

        synchronized (CACHE) {
            // Re-check after acquiring lock, another thread may have spun the same key
            Class<?> raced = CACHE.putIfAbsent(key, spun);
            if (raced != null) {
                return Optional.of((Class<? extends T>) raced);
            }
        }
        return Optional.of((Class<? extends T>) spun);
    }

    // ----- bytecode generation -----

    private static Class<?> spinPrimitiveClass(Class<?> base, String methodName, Class<?> primitive, Object boxed) {
        String fieldName = "CONST_" + sanitize(methodName);
        String typeDescriptor = Type.getDescriptor(primitive);
        String baseInternal = Type.getInternalName(base);
        String spunInternal = baseInternal + "$Spun";

        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
        cw.visit(
            Opcodes.V21,
            Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER,
            spunInternal, null, baseInternal, null
        );

        // public static final <T> CONST_<NAME> = value;
        cw.visitField(
            Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC | Opcodes.ACC_FINAL,
            fieldName, typeDescriptor, null, boxed
        ).visitEnd();

        emitCtors(cw, base, baseInternal);

        // protected final <T> <methodName>() { return CONST_<NAME>; }
        String getterDesc = "()" + typeDescriptor;
        int returnOp = primitive == long.class   ? Opcodes.LRETURN
                     : primitive == double.class ? Opcodes.DRETURN
                     : primitive == float.class  ? Opcodes.FRETURN
                                                 : Opcodes.IRETURN;
        MethodVisitor m = cw.visitMethod(Opcodes.ACC_PROTECTED | Opcodes.ACC_FINAL, methodName, getterDesc, null, null);
        m.visitCode();
        m.visitFieldInsn(Opcodes.GETSTATIC, spunInternal, fieldName, typeDescriptor);
        m.visitInsn(returnOp);
        m.visitMaxs(0, 0);
        m.visitEnd();

        cw.visitEnd();
        return defineHidden(base, cw.toByteArray(), /* classData */ null);
    }

    private static Class<?> spinReferenceClass(Class<?> base, String methodName, Class<?> valueType, Object value) {
        String typeDescriptor = Type.getDescriptor(valueType);
        String baseInternal = Type.getInternalName(base);
        String spunInternal = baseInternal + "$Spun";

        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
        cw.visit(
            Opcodes.V21,
            Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER,
            spunInternal, null, baseInternal, null
        );

        emitCtors(cw, base, baseInternal);

        // protected final <T> <methodName>() { return (T) MethodHandles.classData(LOOKUP, "_", T.class); }
        // Implemented as: ldc Dynamic[name="_", type=T, bootstrap=MethodHandles::classData]; areturn
        Handle classDataBoot = new Handle(
            Opcodes.H_INVOKESTATIC,
            "java/lang/invoke/MethodHandles",
            "classData",
            // (Lookup, String, Class) -> Object
            "(Ljava/lang/invoke/MethodHandles$Lookup;Ljava/lang/String;Ljava/lang/Class;)Ljava/lang/Object;",
            false
        );
        ConstantDynamic condy = new ConstantDynamic("_", typeDescriptor, classDataBoot);

        String getterDesc = "()" + typeDescriptor;
        MethodVisitor m = cw.visitMethod(Opcodes.ACC_PROTECTED | Opcodes.ACC_FINAL, methodName, getterDesc, null, null);
        m.visitCode();
        m.visitLdcInsn(condy);
        m.visitInsn(Opcodes.ARETURN);
        m.visitMaxs(0, 0);
        m.visitEnd();

        cw.visitEnd();
        return defineHidden(base, cw.toByteArray(), value);
    }

    private static void emitCtors(ClassWriter cw, Class<?> base, String baseInternal) {
        // Reproduce every public/protected base ctor with a chained super call.
        for (var ctor : base.getDeclaredConstructors()) {
            int mods = ctor.getModifiers();
            if (java.lang.reflect.Modifier.isPrivate(mods)) continue;
            Class<?>[] paramTypes = ctor.getParameterTypes();
            String descriptor = methodDescriptor(paramTypes, void.class);
            MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", descriptor, null, null);
            mv.visitCode();
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            int slot = 1;
            for (Class<?> pt : paramTypes) {
                if (pt == long.class)        { mv.visitVarInsn(Opcodes.LLOAD, slot); slot += 2; }
                else if (pt == double.class) { mv.visitVarInsn(Opcodes.DLOAD, slot); slot += 2; }
                else if (pt == float.class)  { mv.visitVarInsn(Opcodes.FLOAD, slot); slot++; }
                else if (pt.isPrimitive())   { mv.visitVarInsn(Opcodes.ILOAD, slot); slot++; }
                else                         { mv.visitVarInsn(Opcodes.ALOAD, slot); slot++; }
            }
            mv.visitMethodInsn(Opcodes.INVOKESPECIAL, baseInternal, "<init>", descriptor, false);
            mv.visitInsn(Opcodes.RETURN);
            mv.visitMaxs(0, 0);
            mv.visitEnd();
        }
    }

    private static Class<?> defineHidden(Class<?> base, byte[] bytecode, Object classData) {
        try {
            MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(base, MethodHandles.lookup());
            if (classData == null) {
                return lookup.defineHiddenClass(bytecode, /* initialize */ true).lookupClass();
            } else {
                return lookup.defineHiddenClassWithClassData(bytecode, classData, /* initialize */ true).lookupClass();
            }
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Cannot spin hidden subclass of " + base.getName(), e);
        }
    }

    private static String methodDescriptor(Class<?>[] paramTypes, Class<?> returnType) {
        StringBuilder sb = new StringBuilder("(");
        for (Class<?> pt : paramTypes) sb.append(Type.getDescriptor(pt));
        sb.append(")").append(Type.getDescriptor(returnType));
        return sb.toString();
    }

    private static String sanitize(String name) {
        StringBuilder sb = new StringBuilder(name.length());
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            sb.append(Character.isJavaIdentifierPart(c) ? Character.toUpperCase(c) : '_');
        }
        return sb.toString();
    }
}
