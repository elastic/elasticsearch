/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.ann;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used on parameters on methods annotated with {@link Evaluator} to indicate
 * parameters that are provided to the generated evaluator's constructor rather
 * than recalculated for every row.
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.SOURCE)
public @interface Fixed {
    /**
     * Should this attribute be in the Evaluator's {@code toString}?
     */
    boolean includeInToString() default true;

    /**
     * Defines the scope of the parameter.
     * - SINGLETON (default) will build a single instance and share it across all evaluators
     * - THREAD_LOCAL will build a new instance for each evaluator thread
     */
    Scope scope() default Scope.SINGLETON;

    /**
     * Adopts JIT-time constant folding for this parameter. The annotation processor
     * emits a class shape where the generated evaluator becomes abstract over this
     * parameter, and the Factory materialises a per-distinct-value subclass via
     * {@code org.elasticsearch.compute.operator.ConstantMethodResultSpecializer} with the value
     * baked in as {@code static final} (primitive) or class-data condy (reference).
     * This unlocks C2's constant-folding optimizations including Granlund-Montgomery
     * strength reduction for integer divide / modulo.
     *
     * <h4>READ THIS FIRST — three rules, in order</h4>
     *
     * <ol>
     *   <li><b>Performance testing is mandatory.</b> Never adopt this flag based on
     *       theory. Add JMH cases for both the constant-folded path and the variable
     *       baseline; measure on at least three microarchitectures (Apple M, an
     *       x86 server, an aarch64 server). Mac alone can mislead — Apple Silicon
     *       has aggressive uop fusion that masks both wins and losses present on
     *       server CPUs. See PR #148678 for the calibration table covering 11
     *       attempted adoptions; the framework's value is making this experiment
     *       cheap, not guaranteeing wins.</li>
     *
     *   <li><b>Cardinality is bounded by the admission filter + GC.</b> The specializer's
     *       admission filter (default threshold = 2) refuses to spin for first-time
     *       keys — the codegen Factory routes them to a {@code Standard} subclass
     *       (regular instance field, no JIT folding, runs slower but runs).
     *       Repeat keys spin once and the class lives only as long as some live
     *       evaluator references it (weak-referenced in the cache; GC reclaims when
     *       unused). This is great for parameters with low distinct-value counts
     *       ({@code rhs} in {@code MOD x BY 60}, {@code prefix} in {@code STARTS_WITH(s, "foo")},
     *       a fixed regex pattern). It is still <b>sub-optimal</b> for parameters
     *       that could be user-supplied literals varying per session, query, or
     *       field — the admission filter prevents the specialization tax (each spin is ~13 μs
     *       in steady state, post-JIT-warmup), but those queries pay the
     *       no-JIT-folding cost for their first execution. <b>Flag parameters
     *       whose values are bounded in practice</b> — typically ≤thousands per
     *       cluster lifetime — for best results. If in doubt, measure with the
     *       stress harness in {@code sweep/AdmissionStress.java}.</li>
     *
     *   <li><b>Only valid on parameters with SINGLETON scope.</b> {@code THREAD_LOCAL}
     *       parameters cannot use this flag.</li>
     * </ol>
     *
     * <h4>When to use {@code jitConstant = true}</h4>
     *
     * Adopt only when measurement shows a clear win in steady-state. Two tiers:
     *
     * <p><b>Tier 1 — parameter was previously *variable per row*</b> (no {@code @Fixed}
     * before). Adoption saves one {@code Block} fetch per row (~1 ns) <i>plus</i>
     * whatever inner-loop optimization the constant unlocks. Wins typically come from:
     * <ul>
     *   <li>Inner work does <b>integer divide or modulo</b> by the parameter —
     *       Granlund-Montgomery folds 5-10 cycles into 2-3 (e.g. MOD/DIV: 3-4x).</li>
     *   <li>Inner work calls a method whose <b>length/shape depends on the constant</b>,
     *       letting C2 specialize a loop bound or unroll (e.g. STARTS_WITH/ENDS_WITH
     *       on a fixed prefix/suffix: ~2x).</li>
     *   <li>Inner work <b>devirtualizes</b> through the constant receiver to a
     *       non-trivial method body, and the inner method is worth more than ~2 ns
     *       per call to inline.</li>
     * </ul>
     *
     * <p><b>Tier 2 — parameter was already {@code @Fixed}</b> (regular field, no
     * per-row {@code Block} fetch to save). Adoption costs ~2 ns of abstract-accessor
     * dispatch overhead with the <i>only</i> return being inner-loop optimization.
     * The bar is much higher. <b>Only adopt when there is a concrete multi-cycle
     * unlock</b> (deep call-chain devirtualization, hot-loop branch elimination,
     * method-internal constant folding worth &gt;2 ns). AutomataMatch (powers RLIKE)
     * was tried this way and regressed 23% — the DFA walk didn't recover the
     * dispatch tax.
     *
     * <h4>When NOT to use {@code jitConstant = true}</h4>
     *
     * <ul>
     *   <li><b>High-cardinality parameter</b> (see rule 2 above). Will thrash the
     *       cache and bloat metaspace.</li>
     *   <li>Per-row inner work is one ALU op (a CMP, an ADD, a MUL): dispatch tax
     *       will dominate. Predicted regression.</li>
     *   <li>Per-row inner work is one cheap virtual call with no GM-class optimization
     *       to recover dispatch (e.g. {@code Rounding.Prepared.round()}). DateTrunc
     *       was tried this way and went from 0.6 ns/op to 2.5 ns/op (4x slower).</li>
     *   <li>Per-row inner work is dominated by an intrinsic ({@code Math.round},
     *       {@code Math.log10}, {@code Math.exp}, {@code Math.pow}). C2 already
     *       handles these; the strength-reduction gain is in the noise.</li>
     *   <li>Per-row work is dominated by allocation, IO, or string parsing
     *       (JSON, regex). Adoption is at best neutral.</li>
     *   <li>The parameter would require {@code THREAD_LOCAL} scope (spinner is
     *       SINGLETON-only).</li>
     * </ul>
     */
    boolean jitConstant() default false;

    /**
     * Defines the parameter scope
     */
    enum Scope {
        /**
         * Should be used for immutable parameters that can be shared across different threads
         */
        SINGLETON,
        /**
         * Should be used for mutable or not thread safe parameters
         */
        THREAD_LOCAL,
    }
}
