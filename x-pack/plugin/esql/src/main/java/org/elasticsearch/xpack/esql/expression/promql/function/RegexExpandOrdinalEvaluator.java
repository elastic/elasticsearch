/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import com.google.re2j.Matcher;
import com.google.re2j.Pattern;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * Handwritten {@link ExpressionEvaluator} for {@link RegexExpand} (constant {@code regex} and {@code replacement}).
 * <p>
 * When the source column is dictionary-encoded ({@link OrdinalBytesRefBlock}), dense, and single-valued, the anchored match and
 * template expansion run once per distinct dictionary entry rather than once per row. For a column of N positions backed by a
 * dictionary of D entries this cuts the regex work - and the per-row byte copy {@link RegexExpand#toExactBytes} makes for
 * RE2/J - from N down to D, which is a large win on Lucene keyword columns where doc-value ordinals naturally deduplicate.
 * <p>
 * The fast path is correctness-equivalent to the per-row path because expansion is a pure function of one source value, so the
 * result is identical however many rows share a dictionary entry. It has two shapes because {@link RegexExpand} emits {@code null}
 * as the no-op sentinel for a non-matching value, and a dictionary {@link BytesRefVector} cannot hold {@code null}:
 * <ul>
 *   <li><b>every entry matched</b> - reuse the input ordinals with a rebuilt dictionary and return a fresh
 *       {@link OrdinalBytesRefBlock}; nothing is materialized per row (null positions are already encoded in the reused
 *       ordinals and carried over unchanged);</li>
 *   <li><b>some entry did not match</b> - materialize a plain {@link BytesRefBlock}, appending {@code null} for rows whose
 *       ordinal maps to a non-matching entry; the expensive match/expand still ran only D times, leaving only cheap per-row
 *       appends.</li>
 * </ul>
 * The dictionary path is gated by {@link OrdinalBytesRefBlock#isDense()} and a no-multi-value check; other blocks (non-ordinal,
 * sparse, or possibly multivalued) go through the per-row loop, which applies {@link RegexExpand#process} to every position so
 * nulls, the delete/no-op sentinels, and the multi-value warning are handled identically to the dictionary path's inputs.
 */
final class RegexExpandOrdinalEvaluator implements ExpressionEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(RegexExpandOrdinalEvaluator.class);

    private final Source source;
    private final ExpressionEvaluator src;
    private final Matcher matcher;
    private final BytesRef scratch;
    private final BytesRefBuilder out;
    private final BytesRef outValue;
    private final RegexExpand.Replacement template;
    private final DriverContext driverContext;
    private Warnings warnings;

    RegexExpandOrdinalEvaluator(
        Source source,
        ExpressionEvaluator src,
        Matcher matcher,
        BytesRef scratch,
        BytesRefBuilder out,
        BytesRef outValue,
        RegexExpand.Replacement template,
        DriverContext driverContext
    ) {
        this.source = source;
        this.src = src;
        this.matcher = matcher;
        this.scratch = scratch;
        this.out = out;
        this.outValue = outValue;
        this.template = template;
        this.driverContext = driverContext;
    }

    @Override
    public Block eval(Page page) {
        try (BytesRefBlock srcBlock = (BytesRefBlock) src.eval(page)) {
            OrdinalBytesRefBlock ordinals = srcBlock.asOrdinals();
            if (ordinals != null && ordinals.isDense() && ordinals.mayHaveMultivaluedFields() == false) {
                return evalDictionary(ordinals);
            }
            return evalPerRow(page.getPositionCount(), srcBlock);
        }
    }

    /**
     * Match and expand once per distinct dictionary entry, then either reuse the ordinals (all matched) or materialize a plain
     * block (some entry did not match). A {@code null} slot in {@code expanded} marks a non-matching entry (the no-op sentinel).
     */
    private Block evalDictionary(OrdinalBytesRefBlock ordinalsBlock) {
        BytesRefVector dictionary = ordinalsBlock.getDictionaryVector();
        int dictSize = dictionary.getPositionCount();
        BytesRef[] expanded = new BytesRef[dictSize];
        boolean anyNoMatch = false;
        for (int d = 0; d < dictSize; d++) {
            BytesRef entry = dictionary.getBytesRef(d, scratch);
            BytesRef result = RegexExpand.matchAndExpand(RegexExpand.toExactBytes(entry), matcher, out, outValue, template);
            if (result == null) {
                anyNoMatch = true; // expanded[d] stays null
            } else {
                // out/outValue are reused across entries, so copy the expansion before the next iteration overwrites it.
                expanded[d] = BytesRef.deepCopyOf(result);
            }
        }
        return anyNoMatch ? materializePerRow(ordinalsBlock, expanded) : reuseOrdinals(ordinalsBlock, expanded);
    }

    /**
     * Build an {@link OrdinalBytesRefBlock} that reuses the input ordinals with a rebuilt dictionary - no per-row value is
     * materialized. Valid only when every entry matched, so {@code expanded} has no {@code null} slot. Null positions live in the
     * reused ordinals {@link IntBlock} and are carried over unchanged, matching the per-row null short-circuit.
     */
    private Block reuseOrdinals(OrdinalBytesRefBlock ordinalsBlock, BytesRef[] expanded) {
        BytesRefVector newDictionary;
        try (BytesRefVector.Builder builder = driverContext.blockFactory().newBytesRefVectorBuilder(expanded.length)) {
            for (BytesRef value : expanded) {
                builder.appendBytesRef(value);
            }
            newDictionary = builder.build();
        }
        OrdinalBytesRefBlock result = null;
        try {
            IntBlock inputOrdinals = ordinalsBlock.getOrdinalsBlock();
            inputOrdinals.incRef();
            result = new OrdinalBytesRefBlock(inputOrdinals, newDictionary);
            newDictionary = null;
            return result;
        } finally {
            if (result == null) {
                Releasables.closeExpectNoException(newDictionary);
            }
        }
    }

    /**
     * Map each row's ordinal to its precomputed expansion, appending {@code null} for a non-matching entry (the no-op sentinel)
     * or a null position. Used when at least one entry did not match, since such a {@code null} cannot live in the rebuilt
     * dictionary of an {@link OrdinalBytesRefBlock}.
     */
    private Block materializePerRow(OrdinalBytesRefBlock ordinalsBlock, BytesRef[] expanded) {
        IntBlock ordinals = ordinalsBlock.getOrdinalsBlock();
        int positionCount = ordinalsBlock.getPositionCount();
        try (BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                if (ordinals.isNull(p)) {
                    result.appendNull();
                    continue;
                }
                BytesRef value = expanded[ordinals.getInt(ordinals.getFirstValueIndex(p))];
                if (value == null) {
                    result.appendNull();
                } else {
                    result.appendBytesRef(value);
                }
            }
            return result.build();
        }
    }

    /**
     * Per-row fallback for non-ordinal, sparse, or possibly-multivalued blocks: short-circuit null positions, then apply
     * {@link RegexExpand#process}, turning its multi-value {@link IllegalArgumentException} into a warning and a {@code null}
     * result so the ES|QL single-value contract is honored.
     */
    private Block evalPerRow(int positionCount, BytesRefBlock srcBlock) {
        try (BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                if (srcBlock.isNull(p)) {
                    result.appendNull();
                    continue;
                }
                try {
                    RegexExpand.process(result, p, srcBlock, matcher, scratch, out, outValue, template);
                } catch (IllegalArgumentException e) {
                    warnings().registerException(e);
                    result.appendNull();
                }
            }
            return result.build();
        }
    }

    @Override
    public long baseRamBytesUsed() {
        return BASE_RAM_BYTES_USED + src.baseRamBytesUsed();
    }

    @Override
    public String toString() {
        return "RegexExpandOrdinalEvaluator[src=" + src + ", template=" + template + "]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(src);
    }

    private Warnings warnings() {
        if (warnings == null) {
            this.warnings = driverContext.createWarnings(source);
        }
        return warnings;
    }

    static final class Factory implements ExpressionEvaluator.Factory {
        private final Source source;
        private final ExpressionEvaluator.Factory src;
        private final Pattern pattern;
        private final RegexExpand.Replacement template;

        Factory(Source source, ExpressionEvaluator.Factory src, Pattern pattern, RegexExpand.Replacement template) {
            this.source = source;
            this.src = src;
            this.pattern = pattern;
            this.template = template;
        }

        @Override
        public RegexExpandOrdinalEvaluator get(DriverContext context) {
            // The Matcher (with its capture-group index array) and the output buffers are reused per driver thread, rewound onto
            // each value via reset(); only the compiled pattern and bound template are shared across threads.
            return new RegexExpandOrdinalEvaluator(
                source,
                src.get(context),
                pattern.matcher(""),
                new BytesRef(),
                new BytesRefBuilder(),
                new BytesRef(),
                template,
                context
            );
        }

        @Override
        public String toString() {
            return "RegexExpandOrdinalEvaluator[src=" + src + ", template=" + template + "]";
        }
    }
}
