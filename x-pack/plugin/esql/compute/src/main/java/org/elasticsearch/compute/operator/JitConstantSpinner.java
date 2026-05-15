/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Spins a class at runtime that extends a given evaluator base class and supplies
 * specified primitive constants as {@code static final} fields. The resulting class
 * is loaded as a hidden class via {@link MethodHandles.Lookup#defineHiddenClass} so
 * HotSpot's C2 compiler treats the field accesses as JIT-time constants — unlocking
 * constant-folding optimizations like Granlund-Montgomery strength reduction for
 * integer divide and modulo by a folded literal.
 *
 * <p>The intended use: an {@code @Evaluator}-generated abstract base class declares
 * one or more {@code protected abstract <primitive> name()} methods; the spinner
 * generates a final subclass whose methods return the specific constant value via
 * {@code GETSTATIC} of a {@code static final} field. The hidden-class subclass is
 * cached per (base, name → boxed value) tuple.
 *
 * <p>Currently supports {@code long}, {@code int}, and {@code double} constants.
 */
public final class JitConstantSpinner {

    private record CacheKey(Class<?> base, String name, Object boxedValue) {}

    private static final ConcurrentHashMap<CacheKey, Class<?>> CACHE = new ConcurrentHashMap<>();

    private JitConstantSpinner() {}

    /**
     * Materialize a hidden subclass of {@code baseClass} that overrides
     * {@code protected abstract long methodName()} to return the supplied constant
     * via a {@code static final long} field.
     */
    public static <T> Class<? extends T> longConstantSubclass(Class<T> baseClass, String methodName, long value) {
        return primitiveConstantSubclass(baseClass, methodName, long.class, Long.valueOf(value));
    }

    /** Same as {@link #longConstantSubclass} but for {@code int}. */
    public static <T> Class<? extends T> intConstantSubclass(Class<T> baseClass, String methodName, int value) {
        return primitiveConstantSubclass(baseClass, methodName, int.class, Integer.valueOf(value));
    }

    /** Same as {@link #longConstantSubclass} but for {@code double}. */
    public static <T> Class<? extends T> doubleConstantSubclass(Class<T> baseClass, String methodName, double value) {
        return primitiveConstantSubclass(baseClass, methodName, double.class, Double.valueOf(value));
    }

    @SuppressWarnings("unchecked")
    private static <T> Class<? extends T> primitiveConstantSubclass(
        Class<T> baseClass,
        String methodName,
        Class<?> primitive,
        Object boxed
    ) {
        CacheKey key = new CacheKey(baseClass, methodName, boxed);
        Class<?> existing = CACHE.get(key);
        if (existing != null) {
            return (Class<? extends T>) existing;
        }
        return (Class<? extends T>) CACHE.computeIfAbsent(key, k -> spin(k, primitive));
    }

    private static Class<?> spin(CacheKey key, Class<?> primitive) {
        Class<?> base = key.base();
        String fieldName = "CONST_" + sanitize(key.name());
        Object boxed = key.boxedValue();
        String typeDescriptor = Type.getDescriptor(primitive);
        String baseInternal = Type.getInternalName(base);
        String spunInternal = baseInternal + "$Spun";

        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
        cw.visit(Opcodes.V21, Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER, spunInternal, null, baseInternal, null);
        // public static final <T> CONST_<NAME> = value;
        cw.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC | Opcodes.ACC_FINAL, fieldName, typeDescriptor, null, boxed).visitEnd();

        // Reproduce every public/protected ctor with a super-call.
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
                if (pt == long.class) {
                    mv.visitVarInsn(Opcodes.LLOAD, slot);
                    slot += 2;
                } else if (pt == double.class) {
                    mv.visitVarInsn(Opcodes.DLOAD, slot);
                    slot += 2;
                } else if (pt == float.class) {
                    mv.visitVarInsn(Opcodes.FLOAD, slot);
                    slot++;
                } else if (pt.isPrimitive()) {
                    mv.visitVarInsn(Opcodes.ILOAD, slot);
                    slot++;
                } else {
                    mv.visitVarInsn(Opcodes.ALOAD, slot);
                    slot++;
                }
            }
            mv.visitMethodInsn(Opcodes.INVOKESPECIAL, baseInternal, "<init>", descriptor, false);
            mv.visitInsn(Opcodes.RETURN);
            mv.visitMaxs(0, 0);
            mv.visitEnd();
        }

        // protected final <T> <methodName>() { return CONST_<NAME>; }
        String getterDescriptor = "()" + typeDescriptor;
        int returnOp;
        if (primitive == long.class) returnOp = Opcodes.LRETURN;
        else if (primitive == double.class) returnOp = Opcodes.DRETURN;
        else if (primitive == float.class) returnOp = Opcodes.FRETURN;
        else returnOp = Opcodes.IRETURN;

        MethodVisitor m = cw.visitMethod(Opcodes.ACC_PROTECTED | Opcodes.ACC_FINAL, key.name(), getterDescriptor, null, null);
        m.visitCode();
        m.visitFieldInsn(Opcodes.GETSTATIC, spunInternal, fieldName, typeDescriptor);
        m.visitInsn(returnOp);
        m.visitMaxs(0, 0);
        m.visitEnd();

        cw.visitEnd();
        byte[] bytecode = cw.toByteArray();

        try {
            MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(base, MethodHandles.lookup());
            return lookup.defineHiddenClass(bytecode, true).lookupClass();
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Cannot spin hidden subclass of " + base.getName(), e);
        }
    }

    private static String methodDescriptor(Class<?>[] paramTypes, Class<?> returnType) {
        StringBuilder sb = new StringBuilder("(");
        for (Class<?> pt : paramTypes)
            sb.append(Type.getDescriptor(pt));
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

    /** Test-only: get current cache size. */
    static int cacheSize() {
        return CACHE.size();
    }

    /** Test-only: clear the cache. Hidden classes will be GC'd once unreferenced. */
    static void clearCacheForTest() {
        CACHE.clear();
    }
}
