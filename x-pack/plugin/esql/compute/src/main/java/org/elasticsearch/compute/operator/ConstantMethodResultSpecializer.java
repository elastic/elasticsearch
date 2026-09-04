/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

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
 * Specializes a class at runtime that extends a given evaluator base class and supplies
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
 *       JVM manages size via reachability. A constant-specialized class is kept alive by any live evaluator
 *       instance referencing it. Once nothing references the class, GC reclaims it and the
 *       weak ref clears — the next access starts fresh.</li>
 *   <li><b>Admission tracker</b>: bounded LRU counters keyed by specializer key (default 4096
 *       entries, ~128 KB). Specialization triggers only when a key's count reaches
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
 *   <li>Specialization cost: mean 13 μs, p99 52 μs, max 1.5 ms (cold-JIT outlier)</li>
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
public final class ConstantMethodResultSpecializer {

    /**
     * Default admission-tracker capacity. The tracker holds recently-seen keys (those
     * sighted once but not yet promoted) with an access counter each. Tunable via
     * {@link #setAdmissionCapacity(int)}.
     *
     * <p>Sized for the realistic working set of "first-sight candidates currently in
     * flight" — typically hundreds at most for dashboard-style workloads. Each entry
     * is ~120-160 bytes (LinkedHashMap.Entry + CacheKey + AtomicLong + boxed value);
     * 512 entries ≈ ~60-80 KB per JVM. Per-class metaspace for constant-constant-specialized classes is
     * managed separately via weak refs in the unbounded class cache.
     *
     * <p>Stress test (PR #148678, 450 measurements) showed capacity between 256 and
     * 16384 is essentially flat across our scenarios. 512 picked as the smallest
     * defensible default — large enough to handle realistic concurrent dashboard
     * workloads without thrashing, small enough that the memory cost is negligible.
     */
    static final int DEFAULT_ADMISSION_CAPACITY = 512;

    /**
     * Default admission threshold. {@code 2} means a key must be seen at least twice
     * before the specializer emits a class for it. First-time keys go through the codegen
     * Factory's standard path (regular non-JIT-folded evaluator). This protects against
     * pathological high-cardinality workloads (many distinct one-off constants) at the
     * cost of slightly slower first execution for queries with novel constants.
     *
     * <p>Set to {@code 1} to disable admission filtering — every miss spins immediately.
     * This is the prior behavior; faster for queries with unique-but-large constants
     * that benefit from JIT folding even on first invocation, but vulnerable to
     * pathological cardinality.
     */
    static final int DEFAULT_ADMISSION_THRESHOLD = 2;

    /** @deprecated kept for compatibility with {@link #setCacheCapacity}; admission tracker is the new bound. */
    @Deprecated
    static final int DEFAULT_CACHE_CAPACITY = DEFAULT_ADMISSION_CAPACITY;

    /**
     * JVM-wide default specializer instance. Generated evaluator factories call
     * {@code ConstantMethodResultSpecializer.SHARED.specializeLong(...)}; tests construct their
     * own instance via {@code new ConstantMethodResultSpecializer()} for isolation (no shared state).
     *
     * <p>The cache + admission tracker are intentionally process-global by default: two
     * instances would each spin a hidden class for the same {@code (base, accessor, value)}
     * triple, duplicating JIT compilations and defeating the cache. Use {@code new}
     * only when you want isolated state.
     */
    public static final ConstantMethodResultSpecializer SHARED = new ConstantMethodResultSpecializer();

    private record CacheKey(Class<?> base, String name, Object value) {}

    private volatile int admissionCapacity = DEFAULT_ADMISSION_CAPACITY;
    private volatile int admissionThreshold = DEFAULT_ADMISSION_THRESHOLD;

    /**
     * Cache-entry reachability. {@code SoftReference} keeps a constant-specialized class alive until
     * the JVM is under genuine heap pressure (cleared only to avoid OOM), so the
     * JIT-folded class survives ordinary GC and stays a cache hit. {@code WeakReference}
     * is cleared at the next GC regardless of free heap, which under a memory-stressed
     * node degrades the cache to pure re-spin churn. Soft is the default; weak is kept
     * as an opt-out for workloads that prefer the most aggressive reclamation.
     */
    private volatile boolean useSoftReferences = true;

    private Reference<Class<?>> newRef(Class<?> cls) {
        return useSoftReferences ? new SoftReference<>(cls) : new WeakReference<>(cls);
    }

    /**
     * Spun class cache. The reference strength is governed by {@link #useSoftReferences}:
     * the JVM reclaims classes when no live evaluator instances reference them (weak) or
     * when under heap pressure (soft). Either way the cache is a transparent index, never
     * an artificial retention root.
     */
    private final ConcurrentHashMap<CacheKey, Reference<Class<?>>> CLASSES = new ConcurrentHashMap<>();

    /**
     * Admission tracker. Counts recently-seen keys; spin only triggers when a
     * key's count reaches {@link #admissionThreshold}. Single-occurrence keys
     * never cost a spin — the caller falls back to the non-folded path.
     * Bounded LRU so memory is capped even on pathological cardinality.
     */
    private final Map<CacheKey, AtomicLong> ADMISSION = Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, /* accessOrder */ true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<CacheKey, AtomicLong> eldest) {
            if (size() > admissionCapacity) {
                ADMISSION_EVICTIONS.increment();
                return true;
            }
            return false;
        }
    });

    // Telemetry
    private final LongAdder SPINS = new LongAdder();
    private final LongAdder HITS = new LongAdder();
    private final LongAdder MISSES = new LongAdder();
    private final LongAdder ADMISSION_REJECTED = new LongAdder();   // returned empty because count < threshold
    private final LongAdder WEAK_REF_CLEARED = new LongAdder();     // cache had entry but ref was GC'd
    private final LongAdder ADMISSION_EVICTIONS = new LongAdder();  // counters evicted by LRU
    private final LongAdder STANDARDS = new LongAdder();            // spin threw — gave up

    /**
     * Package-private — production code uses {@link #SHARED}. Tests in the same
     * package may construct an isolated instance to avoid cross-test counter/cache
     * contamination.
     */
    ConstantMethodResultSpecializer() {}

    // ----- public API -----

    /**
     * Materialize (or fetch from cache) a hidden subclass of {@code baseClass} that
     * overrides {@code methodName()} (which the base must declare as
     * {@code protected abstract long methodName()}) to return {@code value} as a
     * JIT-time constant. Returns {@link Optional#empty()} only when the cache is at
     * capacity and adding this entry would require eviction — the caller should fall
     * back to the non-folded path in that case.
     */
    public <T> Optional<Class<? extends T>> specializeLong(Class<T> baseClass, String methodName, long value) {
        return primitiveSubclass(baseClass, methodName, long.class, Long.valueOf(value));
    }

    /** Same as {@link #specializeLong} but for {@code int}. */
    public <T> Optional<Class<? extends T>> specializeInt(Class<T> baseClass, String methodName, int value) {
        return primitiveSubclass(baseClass, methodName, int.class, Integer.valueOf(value));
    }

    /** Same as {@link #specializeLong} but for {@code double}. */
    public <T> Optional<Class<? extends T>> specializeDouble(Class<T> baseClass, String methodName, double value) {
        return primitiveSubclass(baseClass, methodName, double.class, Double.valueOf(value));
    }

    /**
     * Same as {@link #specializeLong} but for an arbitrary reference type.
     * The constant is passed via class data and loaded through a {@code condy}
     * bootstrap referencing {@link MethodHandles#classData(MethodHandles.Lookup, String, Class)}.
     */
    public <T, V> Optional<Class<? extends T>> specializeReference(Class<T> baseClass, String methodName, Class<V> valueType, V value) {
        if (value == null) throw new IllegalArgumentException("value must not be null");
        if (valueType.isPrimitive()) throw new IllegalArgumentException("use primitive variant for " + valueType);
        return spinOrCache(baseClass, methodName, valueType, value, /* primitive */ false);
    }

    /** Number of entries currently in the specialized-class cache (some may have cleared weak refs awaiting prune). */
    int cacheSize() {
        return CLASSES.size();
    }

    /** Number of entries in the admission tracker. */
    int admissionTrackerSize() {
        synchronized (ADMISSION) {
            return ADMISSION.size();
        }
    }

    /** Total number of subclass-generation events (cache misses that resulted in spins). */
    long spinCount() {
        return SPINS.sum();
    }

    long hitCount() {
        return HITS.sum();
    }

    long missCount() {
        return MISSES.sum();
    }

    long evictionCount() {
        return ADMISSION_EVICTIONS.sum();
    }

    long admissionRejectedCount() {
        return ADMISSION_REJECTED.sum();
    }

    long weakRefClearedCount() {
        return WEAK_REF_CLEARED.sum();
    }

    long standardCount() {
        return STANDARDS.sum();
    }

    /**
     * Set admission tracker capacity. Counters above this are LRU-evicted. New value applies
     * to future evictions; existing entries are kept until LRU-evicted.
     */
    void setAdmissionCapacity(int newCapacity) {
        if (newCapacity < 1) throw new IllegalArgumentException("capacity must be >= 1");
        admissionCapacity = newCapacity;
    }

    /**
     * Set admission threshold. A key must be seen this many times before a class is specialized
     * for it. Default = 2 (skip the first-time access — usually a one-off). Set to 1 to
     * disable admission filtering (every miss spins immediately, like the prior behavior).
     *
     * <p>Public so cross-package tests in esql can force the Standard / constant-specialized paths by
     * setting the threshold extreme. No production caller; if a tuning API becomes
     * useful, route it through {@link #SHARED} explicitly.
     */
    public void setAdmissionThreshold(int newThreshold) {
        if (newThreshold < 1) throw new IllegalArgumentException("threshold must be >= 1");
        admissionThreshold = newThreshold;
    }

    /**
     * Choose cache-entry reachability. {@code true} (default) uses {@code SoftReference}
     * — constant-constant-specialized classes survive ordinary GC and are reclaimed only under heap pressure.
     * {@code false} uses {@code WeakReference} — cleared at the next GC regardless of
     * free heap. See {@link #useSoftReferences} for the rationale.
     */
    void setUseSoftReferences(boolean soft) {
        useSoftReferences = soft;
    }

    /** @deprecated use {@link #setAdmissionCapacity(int)}; this maps to the admission tracker. */
    @Deprecated
    void setCacheCapacity(int newCapacity) {
        setAdmissionCapacity(newCapacity);
    }

    /**
     * Stress-test / tooling support: clear all caches and counters and reset config.
     * Not for production use. Marked public so external benchmark/stress harnesses
     * can reset state between scenarios; production code should never call this.
     */
    public void resetForTest() {
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
        STANDARDS.reset();
        admissionCapacity = DEFAULT_ADMISSION_CAPACITY;
        admissionThreshold = DEFAULT_ADMISSION_THRESHOLD;
        useSoftReferences = true;
    }

    // ----- internals -----

    private <T> Optional<Class<? extends T>> primitiveSubclass(Class<T> baseClass, String methodName, Class<?> primitive, Object boxed) {
        return spinOrCache(baseClass, methodName, primitive, boxed, /* primitive */ true);
    }

    /**
     * Core lookup. Returns an existing constant-specialized class (cache hit on a still-alive weak ref) OR
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
     *       spin. This eliminates the specialization tax for one-off cold keys.</li>
     *   <li>Spin: ASM emit + {@code defineHiddenClass}. {@code computeIfAbsent} on the class
     *       cache prevents concurrent threads from racing into redundant spinning for the same
     *       key.</li>
     * </ol>
     */
    @SuppressWarnings("unchecked")
    private <T> Optional<Class<? extends T>> spinOrCache(
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
        Reference<Class<?>> constantSpecializedRef;
        try {
            constantSpecializedRef = CLASSES.computeIfAbsent(key, k -> {
                Class<?> spun = primitive
                    ? emitSpecializedPrimitiveClass(baseClass, methodName, valueType, value)
                    : emitSpecializedReferenceClass(baseClass, methodName, valueType, value);
                SPINS.increment();
                // Counter no longer needed; drop it so the tracker stays focused on candidates.
                removeAdmission(key);
                return newRef(spun);
            });
        } catch (RuntimeException e) {
            STANDARDS.increment();
            return Optional.empty();
        }
        Class<?> constantSpecializedClass = constantSpecializedRef.get();
        if (constantSpecializedClass == null) {
            // Race: weak ref cleared between insertion and our read. Recurse — admission will
            // re-trigger via counter, or standard. Bounded by GC frequency so this is rare.
            CLASSES.remove(key, constantSpecializedRef);
            WEAK_REF_CLEARED.increment();
            return spinOrCache(baseClass, methodName, valueType, value, primitive);
        }
        return Optional.of((Class<? extends T>) constantSpecializedClass);
    }

    private long incrementAdmission(CacheKey key) {
        AtomicLong counter;
        synchronized (ADMISSION) {
            counter = ADMISSION.computeIfAbsent(key, k -> new AtomicLong(0));
        }
        return counter.incrementAndGet();
    }

    private void removeAdmission(CacheKey key) {
        synchronized (ADMISSION) {
            ADMISSION.remove(key);
        }
    }

    // ----- bytecode generation -----

    // ===================================================================
    // Bytecode emission. Each emitX method emits exactly one artifact and
    // carries Javadoc showing the equivalent Java code being produced. The
    // two top-level orchestrators (primitive vs reference) compose the same
    // helpers in different orders / with different field-access decisions.
    // ===================================================================

    /**
     * Emit a constant-specialized subclass whose accessor returns a primitive constant.
     *
     * <p>Equivalent Java for {@code base = ModLongsByConstantEvaluator}, {@code methodName = "rhs"}, {@code value = 60L}:
     * <pre>{@code
     *   public final class ModLongsByConstantEvaluator$ConstantSpecialized extends ModLongsByConstantEvaluator {
     *       public static final long CONST_RHS = 60L;
     *
     *       public ModLongsByConstantEvaluator$ConstantSpecialized(Source source, ExpressionEvaluator lhs, DriverContext ctx) {
     *           super(source, lhs, ctx);
     *       }
     *       // ...one ctor per public ctor on the base class...
     *
     *       protected final long rhs() {
     *           return CONST_RHS;
     *       }
     *   }
     * }</pre>
     *
     * <p>The field is {@code public} because that's the access flag the JIT trusts most readily for
     * primitive {@code static final}s (a value-bearing {@code ConstantValue} attribute initialises it
     * at class-load with no {@code <clinit>}).
     */
    private Class<?> emitSpecializedPrimitiveClass(Class<?> base, String methodName, Class<?> primitive, Object boxed) {
        String typeDescriptor = Type.getDescriptor(primitive);
        ConstantSpecializedClassShapeValidator.ConstantSpecializedClassSpec spec = primitiveSpec(base, methodName, typeDescriptor);

        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
        ClassVisitor cv = ConstantSpecializedClassShapeValidator.guarding(cw, spec);

        emitClassHeader(cv, spec);
        emitConstantField(cv, spec, /* initialValue */ boxed);
        emitForwardingConstructors(cv, base);
        emitPrimitiveAccessor(cv, spec, primitive);

        cv.visitEnd();
        return defineHidden(base, cw.toByteArray(), /* classData */ null);
    }

    /**
     * Emit a constant-specialized subclass whose accessor returns a reference constant.
     *
     * <p>Equivalent Java for {@code base = StartsWithConstantEvaluator}, {@code methodName = "prefix"}, {@code valueType = BytesRef.class}:
     * <pre>{@code
     *   public final class StartsWithConstantEvaluator$ConstantSpecialized extends StartsWithConstantEvaluator {
     *       private static final BytesRef CONST_PREFIX;
     *
     *       static {
     *           // Loaded from class-data slot via condy bootstrapped against MethodHandles.classData.
     *           CONST_PREFIX = (BytesRef) MethodHandles.classData(LOOKUP, "_", BytesRef.class);
     *       }
     *
     *       public StartsWithConstantEvaluator$ConstantSpecialized(Source source, ExpressionEvaluator str, DriverContext ctx) {
     *           super(source, str, ctx);
     *       }
     *
     *       protected final BytesRef prefix() {
     *           return CONST_PREFIX;
     *       }
     *   }
     * }</pre>
     *
     * <p>The field is {@code private} because C2 only treats reference {@code static final}s as JIT-time
     * constants when access is restricted (otherwise the field is mutable via reflection in principle and
     * the optimization is unsound). Initialisation goes through {@code <clinit>} because reference
     * literals can't ride a {@code ConstantValue} attribute — we use condy (LDC of a {@code ConstantDynamic})
     * to fetch the value from the class-data slot that {@link MethodHandles.Lookup#defineHiddenClassWithClassData}
     * populated.
     */
    private Class<?> emitSpecializedReferenceClass(Class<?> base, String methodName, Class<?> valueType, Object value) {
        String typeDescriptor = Type.getDescriptor(valueType);
        ConstantSpecializedClassShapeValidator.ConstantSpecializedClassSpec spec = referenceSpec(base, methodName, typeDescriptor);

        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
        ClassVisitor cv = ConstantSpecializedClassShapeValidator.guarding(cw, spec);

        emitClassHeader(cv, spec);
        emitConstantField(cv, spec, /* initialValue */ null);   // populated by <clinit>
        emitClassDataClinit(cv, spec);
        emitForwardingConstructors(cv, base);
        emitReferenceAccessor(cv, spec);

        cv.visitEnd();
        return defineHidden(base, cw.toByteArray(), value);
    }

    private ConstantSpecializedClassShapeValidator.ConstantSpecializedClassSpec primitiveSpec(
        Class<?> base,
        String methodName,
        String typeDescriptor
    ) {
        String baseInternal = Type.getInternalName(base);
        return new ConstantSpecializedClassShapeValidator.ConstantSpecializedClassSpec(
            base,
            baseInternal + "$ConstantSpecialized",
            methodName,
            "()" + typeDescriptor,
            "CONST_" + sanitize(methodName),
            typeDescriptor,
            Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC | Opcodes.ACC_FINAL,
            /* expectsClinit */ false,
            ConstantSpecializedClassShapeValidator.countPublicCtors(base)
        );
    }

    private ConstantSpecializedClassShapeValidator.ConstantSpecializedClassSpec referenceSpec(
        Class<?> base,
        String methodName,
        String typeDescriptor
    ) {
        String baseInternal = Type.getInternalName(base);
        return new ConstantSpecializedClassShapeValidator.ConstantSpecializedClassSpec(
            base,
            baseInternal + "$ConstantSpecialized",
            methodName,
            "()" + typeDescriptor,
            "CONST_" + sanitize(methodName),
            typeDescriptor,
            Opcodes.ACC_PRIVATE | Opcodes.ACC_STATIC | Opcodes.ACC_FINAL,
            /* expectsClinit */ true,
            ConstantSpecializedClassShapeValidator.countPublicCtors(base)
        );
    }

    /**
     * Class header.
     *
     * <p>Equivalent Java:
     * <pre>{@code
     *   public final class <Base>$ConstantSpecialized extends <Base> { ... }
     * }</pre>
     */
    private static void emitClassHeader(ClassVisitor cv, ConstantSpecializedClassShapeValidator.ConstantSpecializedClassSpec spec) {
        cv.visit(
            Opcodes.V21,
            Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER,
            spec.constantSpecializedInternalName(),
            null,
            spec.expectedSuperInternalName(),
            null
        );
    }

    /**
     * The single constant-holding field.
     *
     * <p>Equivalent Java (primitive case — {@code initialValue} non-null, baked as {@code ConstantValue} attribute):
     * <pre>{@code
     *   public static final long CONST_RHS = 60L;
     * }</pre>
     *
     * <p>Equivalent Java (reference case — {@code initialValue} null, populated by {@code <clinit>}):
     * <pre>{@code
     *   private static final BytesRef CONST_PREFIX;
     * }</pre>
     */
    private static void emitConstantField(
        ClassVisitor cv,
        ConstantSpecializedClassShapeValidator.ConstantSpecializedClassSpec spec,
        Object initialValue
    ) {
        cv.visitField(spec.expectedFieldAccess(), spec.fieldName(), spec.fieldDescriptor(), null, initialValue).visitEnd();
    }

    /**
     * Class initializer that fetches a reference constant from the class-data slot.
     *
     * <p>Equivalent Java:
     * <pre>{@code
     *   static {
     *       CONST_PREFIX = (BytesRef) MethodHandles.classData(LOOKUP, "_", BytesRef.class);
     *   }
     * }</pre>
     *
     * <p>Implemented at the bytecode level as a single LDC of a {@link ConstantDynamic} (condy) whose
     * bootstrap is {@code MethodHandles.classData}, followed by a {@code putstatic}.
     */
    private static void emitClassDataClinit(ClassVisitor cv, ConstantSpecializedClassShapeValidator.ConstantSpecializedClassSpec spec) {
        Handle classDataBoot = new Handle(
            Opcodes.H_INVOKESTATIC,
            "java/lang/invoke/MethodHandles",
            "classData",
            "(Ljava/lang/invoke/MethodHandles$Lookup;Ljava/lang/String;Ljava/lang/Class;)Ljava/lang/Object;",
            false
        );
        ConstantDynamic condy = new ConstantDynamic("_", spec.fieldDescriptor(), classDataBoot);

        MethodVisitor clinit = cv.visitMethod(Opcodes.ACC_STATIC, "<clinit>", "()V", null, null);
        clinit.visitCode();
        clinit.visitLdcInsn(condy);
        clinit.visitFieldInsn(Opcodes.PUTSTATIC, spec.constantSpecializedInternalName(), spec.fieldName(), spec.fieldDescriptor());
        clinit.visitInsn(Opcodes.RETURN);
        clinit.visitMaxs(0, 0);
        clinit.visitEnd();
    }

    /**
     * Mirror every public ctor of {@code base} as a forwarding ctor on the constant-specialized subclass.
     *
     * <p>Equivalent Java (one such method per public ctor on the base):
     * <pre>{@code
     *   public <Base>$ConstantSpecialized(P1 p1, P2 p2, ..., Pn pn) {
     *       super(p1, p2, ..., pn);
     *   }
     * }</pre>
     *
     * <p>The constant-specialized subclass holds no additional state — every parent ctor is mirrored verbatim
     * with a chained {@code super(...)} call. Skips private ctors (defensive — codegen-emitted
     * evaluators never have them, but the loop should be local-safe).
     */
    private void emitForwardingConstructors(ClassVisitor cv, Class<?> base) {
        String baseInternal = Type.getInternalName(base);
        for (var ctor : base.getConstructors()) {
            if (java.lang.reflect.Modifier.isPrivate(ctor.getModifiers())) continue;
            Class<?>[] paramTypes = ctor.getParameterTypes();
            String descriptor = methodDescriptor(paramTypes, void.class);
            MethodVisitor mv = cv.visitMethod(Opcodes.ACC_PUBLIC, "<init>", descriptor, null, null);
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

    /**
     * Accessor override that returns the primitive constant.
     *
     * <p>Equivalent Java:
     * <pre>{@code
     *   protected final long rhs() {
     *       return CONST_RHS;
     *   }
     * }</pre>
     *
     * <p>At the bytecode level: {@code getstatic} the field, then the type-appropriate return opcode
     * ({@code lreturn} for long/long-backed types, {@code dreturn} for double, {@code freturn} for
     * float, {@code ireturn} otherwise).
     */
    private static void emitPrimitiveAccessor(
        ClassVisitor cv,
        ConstantSpecializedClassShapeValidator.ConstantSpecializedClassSpec spec,
        Class<?> primitive
    ) {
        int returnOp = primitive == long.class ? Opcodes.LRETURN
            : primitive == double.class ? Opcodes.DRETURN
            : primitive == float.class ? Opcodes.FRETURN
            : Opcodes.IRETURN;
        emitAccessorBody(cv, spec, returnOp);
    }

    /**
     * Accessor override that returns the reference constant.
     *
     * <p>Equivalent Java:
     * <pre>{@code
     *   protected final BytesRef prefix() {
     *       return CONST_PREFIX;
     *   }
     * }</pre>
     */
    private static void emitReferenceAccessor(ClassVisitor cv, ConstantSpecializedClassShapeValidator.ConstantSpecializedClassSpec spec) {
        emitAccessorBody(cv, spec, Opcodes.ARETURN);
    }

    private static void emitAccessorBody(
        ClassVisitor cv,
        ConstantSpecializedClassShapeValidator.ConstantSpecializedClassSpec spec,
        int returnOp
    ) {
        MethodVisitor m = cv.visitMethod(
            Opcodes.ACC_PROTECTED | Opcodes.ACC_FINAL,
            spec.accessorName(),
            spec.accessorDescriptor(),
            null,
            null
        );
        m.visitCode();
        m.visitFieldInsn(Opcodes.GETSTATIC, spec.constantSpecializedInternalName(), spec.fieldName(), spec.fieldDescriptor());
        m.visitInsn(returnOp);
        m.visitMaxs(0, 0);
        m.visitEnd();
    }

    private Class<?> defineHidden(Class<?> base, byte[] bytecode, Object classData) {
        // Bytecode-shape drift is caught at *emission* time by ConstantSpecializedClassShapeValidator's
        // wrap (see emitSpecializedPrimitiveClass / emitSpecializedReferenceClass). On drift the validator throws
        // AssertionError, which deliberately bypasses spinOrCache's catch (RuntimeException)
        // and propagates uncaught — a drifted emitter is unsafe territory and we hard-fail
        // rather than silently degrade. By the time we get here, the byte[] has been
        // verified opcode-by-opcode and structurally; defineHiddenClass cannot see a class
        // shape the validator hasn't already approved.
        try {
            // defineHiddenClass is gated by the create_class_loader entitlement, which is
            // granted to ALL-UNNAMED in the ESQL plugin's entitlement-policy.yaml.
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

    private String methodDescriptor(Class<?>[] paramTypes, Class<?> returnType) {
        StringBuilder sb = new StringBuilder("(");
        for (Class<?> pt : paramTypes) {
            sb.append(Type.getDescriptor(pt));
        }
        sb.append(")").append(Type.getDescriptor(returnType));
        return sb.toString();
    }

    private String sanitize(String name) {
        StringBuilder sb = new StringBuilder(name.length());
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            sb.append(Character.isJavaIdentifierPart(c) ? Character.toUpperCase(c) : '_');
        }
        return sb.toString();
    }
}
