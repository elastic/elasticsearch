/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.ConstantDynamic;
import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.lang.invoke.MethodHandles;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
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
 * <h2>Cache design — weak refs + admission filter</h2>
 *
 * Two structures handle the workload-shape tradeoffs:
 *
 * <ol>
 *   <li><b>Class cache</b>: {@code ConcurrentHashMap<Key, WeakReference<Class>>}. Unbounded;
 *       JVM manages size via reachability. A spun class is kept alive by any live evaluator
 *       instance referencing it. Once nothing references the class, GC reclaims it and the
 *       weak ref clears — the next access starts fresh.</li>
 *   <li><b>Admission tracker</b>: bounded LRU counters keyed by spinner key (default 4096
 *       entries, ~128 KB). Spin triggers only when a key's count reaches
 *       {@code admissionThreshold} (default 2). Single-occurrence keys never trigger a
 *       spin — the caller falls back to the non-folded path. This eliminates the spin
 *       tax for one-off cold keys.</li>
 * </ol>
 *
 * <p>Properties this gives:
 * <ul>
 *   <li><b>High-cardinality one-off workload</b> (e.g. 50 K distinct constants none repeating):
 *       zero spins. Per-access overhead = one CHM lookup + one counter increment (~50 ns).</li>
 *   <li><b>Bursty workload</b>: hot keys spin once on second access; stay alive while bursts
 *       run (live instances pin them); GC after a quiet period.</li>
 *   <li><b>Steady-state hot</b>: spin once per key, then constant hit rate forever.</li>
 *   <li><b>Stampede</b>: concurrent threads requesting the same missing key all funnel through
 *       a single {@code computeIfAbsent} — only one spin happens.</li>
 * </ul>
 *
 * <h2>Cost calibration</h2>
 *
 * Measured on Apple aarch64 JDK 21 after JIT warmup:
 * <ul>
 *   <li>Spin cost: mean 13 μs, p99 52 μs, max 1.5 ms (cold-JIT outlier)</li>
 *   <li>Per-class metaspace: ~1.4-2 KB</li>
 *   <li>Cache hit lookup: ~50 ns</li>
 *   <li>Admission-rejected path: ~50 ns (one CHM lookup + counter inc)</li>
 * </ul>
 *
 * <h2>Measurement is mandatory for adoption</h2>
 *
 * Adoption decisions are not predictable from code reading alone — both wins and
 * regressions surface that the static rules don't catch. Every flag adoption
 * must come with a JMH bench case (const-folded + variable baseline) measured
 * on at least three microarchitectures. See {@link org.elasticsearch.compute.ann.Fixed#jitConstant()}
 * for the decision framework and the calibration table from PR #148678 covering
 * 11 attempted adoptions, of which 4 shipped, 1 was kept noise-band-neutral,
 * and 6 were reverted with measurement evidence.
 */
public final class JitConstantSpinner {

    /**
     * Default admission-tracker capacity. The tracker holds recently-seen keys (those
     * sighted once but not yet promoted) with an access counter each. Tunable via
     * {@link #setAdmissionCapacity(int)}.
     *
     * <p>Sized for the realistic working set of "first-sight candidates currently in
     * flight" — typically hundreds at most for dashboard-style workloads. Each entry
     * is ~120-160 bytes (LinkedHashMap.Entry + CacheKey + AtomicLong + boxed value);
     * 512 entries ≈ ~60-80 KB per JVM. Per-class metaspace for spun classes is
     * managed separately via weak refs in the unbounded class cache.
     *
     * <p>Stress test (PR #148678, 450 measurements) showed capacity between 256 and
     * 16384 is essentially flat across our scenarios. 512 picked as the smallest
     * defensible default — large enough to handle realistic concurrent dashboard
     * workloads without thrashing, small enough that the memory cost is negligible.
     */
    public static final int DEFAULT_ADMISSION_CAPACITY = 512;

    /**
     * Default admission threshold. {@code 2} means a key must be seen at least twice
     * before the spinner emits a class for it. First-time keys go through the codegen
     * Factory's fallback path (regular non-JIT-folded evaluator). This protects against
     * pathological high-cardinality workloads (many distinct one-off constants) at the
     * cost of slightly slower first execution for queries with novel constants.
     *
     * <p>Set to {@code 1} to disable admission filtering — every miss spins immediately.
     * This is the prior behavior; faster for queries with unique-but-large constants
     * that benefit from JIT folding even on first invocation, but vulnerable to
     * pathological cardinality.
     */
    public static final int DEFAULT_ADMISSION_THRESHOLD = 2;

    /** @deprecated kept for compatibility with {@link #setCacheCapacity}; admission tracker is the new bound. */
    @Deprecated
    public static final int DEFAULT_CACHE_CAPACITY = DEFAULT_ADMISSION_CAPACITY;

    private record CacheKey(Class<?> base, String name, Object value) {}

    private static volatile int admissionCapacity = DEFAULT_ADMISSION_CAPACITY;
    private static volatile int admissionThreshold = DEFAULT_ADMISSION_THRESHOLD;

    /**
     * Cache-entry reachability. {@code SoftReference} keeps a spun class alive until
     * the JVM is under genuine heap pressure (cleared only to avoid OOM), so the
     * JIT-folded class survives ordinary GC and stays a cache hit. {@code WeakReference}
     * is cleared at the next GC regardless of free heap, which under a memory-stressed
     * node degrades the cache to pure re-spin churn. Soft is the default; weak is kept
     * as an opt-out for workloads that prefer the most aggressive reclamation.
     */
    private static volatile boolean useSoftReferences = true;

    private static Reference<Class<?>> newRef(Class<?> cls) {
        return useSoftReferences ? new SoftReference<>(cls) : new WeakReference<>(cls);
    }

    /**
     * Spun class cache. The reference strength is governed by {@link #useSoftReferences}:
     * the JVM reclaims classes when no live evaluator instances reference them (weak) or
     * when under heap pressure (soft). Either way the cache is a transparent index, never
     * an artificial retention root.
     */
    private static final ConcurrentHashMap<CacheKey, Reference<Class<?>>> CLASSES = new ConcurrentHashMap<>();

    /**
     * Admission tracker. Counts recently-seen keys; spin only triggers when a
     * key's count reaches {@link #admissionThreshold}. Single-occurrence keys
     * never cost a spin — the caller falls back to the non-folded path.
     * Bounded LRU so memory is capped even on pathological cardinality.
     */
    private static final Map<CacheKey, AtomicLong> ADMISSION = Collections.synchronizedMap(
        new LinkedHashMap<>(16, 0.75f, /* accessOrder */ true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<CacheKey, AtomicLong> eldest) {
                if (size() > admissionCapacity) {
                    ADMISSION_EVICTIONS.increment();
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
    private static final LongAdder ADMISSION_REJECTED = new LongAdder();   // returned empty because count < threshold
    private static final LongAdder WEAK_REF_CLEARED = new LongAdder();     // cache had entry but ref was GC'd
    private static final LongAdder ADMISSION_EVICTIONS = new LongAdder();  // counters evicted by LRU
    private static final LongAdder FALLBACKS = new LongAdder();            // spin threw — gave up

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

    /** Number of entries currently in the spun-class cache (some may have cleared weak refs awaiting prune). */
    public static int cacheSize() {
        return CLASSES.size();
    }

    /** Number of entries in the admission tracker. */
    public static int admissionTrackerSize() {
        synchronized (ADMISSION) {
            return ADMISSION.size();
        }
    }

    /** Total number of subclass-generation events (cache misses that resulted in spins). */
    public static long spinCount() {
        return SPINS.sum();
    }

    public static long hitCount() {
        return HITS.sum();
    }

    public static long missCount() {
        return MISSES.sum();
    }

    public static long evictionCount() {
        return ADMISSION_EVICTIONS.sum();
    }

    public static long admissionRejectedCount() {
        return ADMISSION_REJECTED.sum();
    }

    public static long weakRefClearedCount() {
        return WEAK_REF_CLEARED.sum();
    }

    public static long fallbackCount() {
        return FALLBACKS.sum();
    }

    /**
     * Set admission tracker capacity. Counters above this are LRU-evicted. New value applies
     * to future evictions; existing entries are kept until LRU-evicted.
     */
    public static void setAdmissionCapacity(int newCapacity) {
        if (newCapacity < 1) throw new IllegalArgumentException("capacity must be >= 1");
        admissionCapacity = newCapacity;
    }

    /**
     * Set admission threshold. A key must be seen this many times before a class is spun
     * for it. Default = 2 (skip the first-time access — usually a one-off). Set to 1 to
     * disable admission filtering (every miss spins immediately, like the prior behavior).
     */
    public static void setAdmissionThreshold(int newThreshold) {
        if (newThreshold < 1) throw new IllegalArgumentException("threshold must be >= 1");
        admissionThreshold = newThreshold;
    }

    /**
     * Choose cache-entry reachability. {@code true} (default) uses {@code SoftReference}
     * — spun classes survive ordinary GC and are reclaimed only under heap pressure.
     * {@code false} uses {@code WeakReference} — cleared at the next GC regardless of
     * free heap. See {@link #useSoftReferences} for the rationale.
     */
    public static void setUseSoftReferences(boolean soft) {
        useSoftReferences = soft;
    }

    /** @deprecated use {@link #setAdmissionCapacity(int)}; this maps to the admission tracker. */
    @Deprecated
    public static void setCacheCapacity(int newCapacity) {
        setAdmissionCapacity(newCapacity);
    }

    /**
     * Stress-test / tooling support: clear all caches and counters and reset config.
     * Not for production use. Marked public so external benchmark/stress harnesses
     * can reset state between scenarios; production code should never call this.
     */
    public static void resetForTest() {
        CLASSES.clear();
        synchronized (ADMISSION) {
            ADMISSION.clear();
        }
        SPINS.reset();
        HITS.reset();
        MISSES.reset();
        ADMISSION_REJECTED.reset();
        WEAK_REF_CLEARED.reset();
        ADMISSION_EVICTIONS.reset();
        FALLBACKS.reset();
        admissionCapacity = DEFAULT_ADMISSION_CAPACITY;
        admissionThreshold = DEFAULT_ADMISSION_THRESHOLD;
        useSoftReferences = true;
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

    /**
     * Core lookup. Returns an existing spun class (cache hit on a still-alive weak ref) OR
     * spins a new one (if the key has been seen enough times) OR returns empty (first-time
     * key — caller uses the non-jit-folded path).
     *
     * <p>Three layers:
     * <ol>
     *   <li>Class cache: {@code CHM<Key, WeakReference<Class>>}. A class is alive as long as
     *       any code references it (typically live evaluator instances). Once nothing references
     *       it, GC reclaims the class and the weak ref clears — next access starts fresh from
     *       layer 2.</li>
     *   <li>Admission tracker: bounded LRU counters per key. Only on count ≥ threshold do we
     *       spin. This eliminates the spin tax for one-off cold keys.</li>
     *   <li>Spin: ASM emit + {@code defineHiddenClass}. {@code computeIfAbsent} on the class
     *       cache prevents concurrent threads from racing into redundant spinning for the same
     *       key.</li>
     * </ol>
     */
    @SuppressWarnings("unchecked")
    private static <T> Optional<Class<? extends T>> spinOrCache(
        Class<T> baseClass,
        String methodName,
        Class<?> valueType,
        Object value,
        boolean primitive
    ) {
        CacheKey key = new CacheKey(baseClass, methodName, value);

        // Layer 1: class cache hit (weak ref still alive)
        Reference<Class<?>> ref = CLASSES.get(key);
        if (ref != null) {
            Class<?> cls = ref.get();
            if (cls != null) {
                HITS.increment();
                return Optional.of((Class<? extends T>) cls);
            }
            // Weak ref cleared by GC since last access — class had no live instances
            // and was unloaded. Drop the dead entry and fall through to admission.
            CLASSES.remove(key, ref);
            WEAK_REF_CLEARED.increment();
        }

        // Layer 2: admission tracker. Only spin when we've seen this key enough times.
        long count = incrementAdmission(key);
        if (count < admissionThreshold) {
            ADMISSION_REJECTED.increment();
            return Optional.empty();   // caller falls back to non-jit-folded path
        }

        // Layer 3: spin. computeIfAbsent prevents stampede — only one thread spins per key.
        MISSES.increment();
        Reference<Class<?>> spunRef;
        try {
            spunRef = CLASSES.computeIfAbsent(key, k -> {
                Class<?> spun = primitive
                    ? spinPrimitiveClass(baseClass, methodName, valueType, value)
                    : spinReferenceClass(baseClass, methodName, valueType, value);
                SPINS.increment();
                // Counter no longer needed; drop it so the tracker stays focused on candidates.
                removeAdmission(key);
                return newRef(spun);
            });
        } catch (RuntimeException e) {
            FALLBACKS.increment();
            return Optional.empty();
        }
        Class<?> spunClass = spunRef.get();
        if (spunClass == null) {
            // Race: weak ref cleared between insertion and our read. Recurse — admission will
            // re-trigger via counter, or fallback. Bounded by GC frequency so this is rare.
            CLASSES.remove(key, spunRef);
            WEAK_REF_CLEARED.increment();
            return spinOrCache(baseClass, methodName, valueType, value, primitive);
        }
        return Optional.of((Class<? extends T>) spunClass);
    }

    private static long incrementAdmission(CacheKey key) {
        AtomicLong counter;
        synchronized (ADMISSION) {
            counter = ADMISSION.computeIfAbsent(key, k -> new AtomicLong(0));
        }
        return counter.incrementAndGet();
    }

    private static void removeAdmission(CacheKey key) {
        synchronized (ADMISSION) {
            ADMISSION.remove(key);
        }
    }

    // ----- bytecode generation -----

    private static Class<?> spinPrimitiveClass(Class<?> base, String methodName, Class<?> primitive, Object boxed) {
        String fieldName = "CONST_" + sanitize(methodName);
        String typeDescriptor = Type.getDescriptor(primitive);
        String baseInternal = Type.getInternalName(base);
        String spunInternal = baseInternal + "$Spun";

        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
        cw.visit(Opcodes.V21, Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER, spunInternal, null, baseInternal, null);

        // public static final <T> CONST_<NAME> = value;
        cw.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC | Opcodes.ACC_FINAL, fieldName, typeDescriptor, null, boxed).visitEnd();

        emitCtors(cw, base, baseInternal);

        // protected final <T> <methodName>() { return CONST_<NAME>; }
        String getterDesc = "()" + typeDescriptor;
        int returnOp = primitive == long.class ? Opcodes.LRETURN
            : primitive == double.class ? Opcodes.DRETURN
            : primitive == float.class ? Opcodes.FRETURN
            : Opcodes.IRETURN;
        MethodVisitor m = guard(
            cw.visitMethod(Opcodes.ACC_PROTECTED | Opcodes.ACC_FINAL, methodName, getterDesc, null, null),
            methodName,
            spunInternal,
            baseInternal
        );
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
        String fieldName = "CONST_" + sanitize(methodName);

        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
        cw.visit(Opcodes.V21, Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER, spunInternal, null, baseInternal, null);

        // private static final <T> CONST_<NAME>; — populated by <clinit> from class data.
        // C2 trusts static final reference fields as JIT-time constants when the field is
        // read through getstatic, much more aggressively than it inlines an LDC ldc-condy.
        cw.visitField(Opcodes.ACC_PRIVATE | Opcodes.ACC_STATIC | Opcodes.ACC_FINAL, fieldName, typeDescriptor, null, null).visitEnd();

        // static { CONST_<NAME> = (T) MethodHandles.classData(LOOKUP, "_", T.class); }
        // Implemented via: ldc Dynamic[name="_", type=T, bootstrap=MethodHandles::classData] ; putstatic
        Handle classDataBoot = new Handle(
            Opcodes.H_INVOKESTATIC,
            "java/lang/invoke/MethodHandles",
            "classData",
            "(Ljava/lang/invoke/MethodHandles$Lookup;Ljava/lang/String;Ljava/lang/Class;)Ljava/lang/Object;",
            false
        );
        ConstantDynamic condy = new ConstantDynamic("_", typeDescriptor, classDataBoot);

        MethodVisitor clinit = guard(
            cw.visitMethod(Opcodes.ACC_STATIC, "<clinit>", "()V", null, null),
            "<clinit>",
            spunInternal,
            baseInternal
        );
        clinit.visitCode();
        clinit.visitLdcInsn(condy);
        clinit.visitFieldInsn(Opcodes.PUTSTATIC, spunInternal, fieldName, typeDescriptor);
        clinit.visitInsn(Opcodes.RETURN);
        clinit.visitMaxs(0, 0);
        clinit.visitEnd();

        emitCtors(cw, base, baseInternal);

        // protected final <T> <methodName>() { return CONST_<NAME>; }
        String getterDesc = "()" + typeDescriptor;
        MethodVisitor m = guard(
            cw.visitMethod(Opcodes.ACC_PROTECTED | Opcodes.ACC_FINAL, methodName, getterDesc, null, null),
            methodName,
            spunInternal,
            baseInternal
        );
        m.visitCode();
        m.visitFieldInsn(Opcodes.GETSTATIC, spunInternal, fieldName, typeDescriptor);
        m.visitInsn(Opcodes.ARETURN);
        m.visitMaxs(0, 0);
        m.visitEnd();

        cw.visitEnd();
        return defineHidden(base, cw.toByteArray(), value);
    }

    private static void emitCtors(ClassWriter cw, Class<?> base, String baseInternal) {
        String spunInternal = baseInternal + "$Spun";
        // Reproduce every public base ctor with a chained super call. The base classes
        // we spin against are always codegen-emitted abstract evaluators with public ctors.
        for (var ctor : base.getConstructors()) {
            int mods = ctor.getModifiers();
            if (java.lang.reflect.Modifier.isPrivate(mods)) continue;
            Class<?>[] paramTypes = ctor.getParameterTypes();
            String descriptor = methodDescriptor(paramTypes, void.class);
            MethodVisitor mv = guard(
                cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", descriptor, null, null),
                "<init>",
                spunInternal,
                baseInternal
            );
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
    }

    private static Class<?> defineHidden(Class<?> base, byte[] bytecode, Object classData) {
        // Note: bytecode-shape drift is caught at *emission* time by the GuardingMethodVisitor
        // wrap (see {@link #guard}); each visit* call asserts the opcode is in the narrow allowlist
        // before it is written. No re-parse of the byte[] is needed at this point. The static
        // {@link #verifyEmittedShape(byte[], Class)} method exists for tests that hand-craft
        // a malformed byte[] outside the emit path; production never calls it.
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

    /**
     * Wrap an emit-time {@link MethodVisitor} so every {@code visit*} call is checked against
     * the narrow opcode allowlist before being passed through. The spinner emits, per spun class,
     * exactly: one mirror of each public base ctor (ALOAD this, *LOAD params, INVOKESPECIAL super.&lt;init&gt;,
     * RETURN); one constant-returning accessor (GETSTATIC field, *RETURN); and for reference constants
     * one &lt;clinit&gt; (LDC condy, PUTSTATIC, RETURN). Anything else is emitter drift — caught here at the
     * moment the offending {@code visit*} is called, without re-parsing the byte[]. The exception
     * propagates out of {@code spinOrCache}'s {@code computeIfAbsent} into the existing
     * {@code RuntimeException} catch, which routes the caller to the codegen Fallback. No crash,
     * no wrong answer, just no folding for that constant.
     */
    private static MethodVisitor guard(MethodVisitor delegate, String methodName, String spunInternal, String expectedSuperInternal) {
        return new GuardingMethodVisitor(delegate, methodName, spunInternal, expectedSuperInternal);
    }

    /**
     * Test-only entry point that runs the same allowlist over a hand-crafted byte[]. Production
     * never calls this — emit-time wrapping via {@link #guard} is the active check.
     */
    static void verifyEmittedShape(byte[] bytecode, Class<?> declaredSuper) {
        final String expectedSuperInternal = Type.getInternalName(declaredSuper);
        final String[] spunInternalRef = new String[1];
        new ClassReader(bytecode).accept(new ClassVisitor(Opcodes.ASM9) {
            @Override
            public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
                if (expectedSuperInternal.equals(superName) == false) {
                    throw new IllegalStateException(
                        "emitter drift: spun class super [" + superName + "] != expected [" + expectedSuperInternal + "]"
                    );
                }
                spunInternalRef[0] = name;
            }

            @Override
            public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
                // Feed the same guard, but with a no-op delegate (we're just checking, not re-emitting).
                return new GuardingMethodVisitor(new MethodVisitor(Opcodes.ASM9) {}, name, spunInternalRef[0], expectedSuperInternal);
            }
        }, ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
    }

    /** Inline opcode-allowlist verifier; wraps a real {@link MethodVisitor} or a no-op for testing. */
    private static final class GuardingMethodVisitor extends MethodVisitor {
        private final String mName;
        private final String spunInternal;
        private final String expectedSuperInternal;

        GuardingMethodVisitor(MethodVisitor delegate, String methodName, String spunInternal, String expectedSuperInternal) {
            super(Opcodes.ASM9, delegate);
            this.mName = methodName;
            this.spunInternal = spunInternal;
            this.expectedSuperInternal = expectedSuperInternal;
        }

        @Override
        public void visitInsn(int opcode) {
            switch (opcode) {
                case Opcodes.RETURN, Opcodes.IRETURN, Opcodes.LRETURN, Opcodes.FRETURN, Opcodes.DRETURN, Opcodes.ARETURN -> super.visitInsn(
                    opcode
                );
                default -> throw drift("unexpected opcode " + opcode);
            }
        }

        @Override
        public void visitVarInsn(int opcode, int var) {
            switch (opcode) {
                case Opcodes.ALOAD, Opcodes.ILOAD, Opcodes.LLOAD, Opcodes.FLOAD, Opcodes.DLOAD -> super.visitVarInsn(opcode, var);
                default -> throw drift("unexpected var-insn opcode " + opcode);
            }
        }

        @Override
        public void visitFieldInsn(int opcode, String owner, String fName, String fDesc) {
            if (owner.equals(spunInternal) == false) {
                throw drift("field op on foreign owner [" + owner + "]");
            }
            if (opcode == Opcodes.GETSTATIC || (opcode == Opcodes.PUTSTATIC && "<clinit>".equals(mName))) {
                super.visitFieldInsn(opcode, owner, fName, fDesc);
                return;
            }
            throw drift("unexpected field-insn opcode " + opcode);
        }

        @Override
        public void visitMethodInsn(int opcode, String owner, String mn, String md, boolean iface) {
            if (opcode == Opcodes.INVOKESPECIAL && expectedSuperInternal.equals(owner) && "<init>".equals(mn)) {
                super.visitMethodInsn(opcode, owner, mn, md, iface);
                return;
            }
            throw drift("unexpected method-insn opcode=" + opcode + " owner=" + owner + " name=" + mn);
        }

        @Override
        public void visitLdcInsn(Object cst) {
            if ("<clinit>".equals(mName) && cst instanceof ConstantDynamic) {
                super.visitLdcInsn(cst);
                return;
            }
            throw drift("unexpected LDC of " + cst.getClass().getSimpleName());
        }

        @Override
        public void visitIntInsn(int opcode, int operand) {
            throw drift("unexpected int-insn opcode " + opcode);
        }

        @Override
        public void visitTypeInsn(int opcode, String type) {
            throw drift("unexpected type-insn opcode " + opcode);
        }

        @Override
        public void visitJumpInsn(int opcode, org.objectweb.asm.Label l) {
            throw drift("unexpected jump-insn opcode " + opcode);
        }

        @Override
        public void visitIincInsn(int var, int incr) {
            throw drift("unexpected IINC");
        }

        @Override
        public void visitInvokeDynamicInsn(String n, String d, Handle b, Object... a) {
            throw drift("unexpected INVOKEDYNAMIC");
        }

        @Override
        public void visitMultiANewArrayInsn(String d, int dims) {
            throw drift("unexpected MULTIANEWARRAY");
        }

        @Override
        public void visitTableSwitchInsn(int min, int max, org.objectweb.asm.Label dflt, org.objectweb.asm.Label... labels) {
            throw drift("unexpected TABLESWITCH");
        }

        @Override
        public void visitLookupSwitchInsn(org.objectweb.asm.Label dflt, int[] keys, org.objectweb.asm.Label[] labels) {
            throw drift("unexpected LOOKUPSWITCH");
        }

        private IllegalStateException drift(String what) {
            return new IllegalStateException(
                "JitConstantSpinner emitter drift in spun [" + spunInternal + "] method [" + mName + "]: " + what
            );
        }
    }

    private static String methodDescriptor(Class<?>[] paramTypes, Class<?> returnType) {
        StringBuilder sb = new StringBuilder("(");
        for (Class<?> pt : paramTypes) {
            sb.append(Type.getDescriptor(pt));
        }
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
