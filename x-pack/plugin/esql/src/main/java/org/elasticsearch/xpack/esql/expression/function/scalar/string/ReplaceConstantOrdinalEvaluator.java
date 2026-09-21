/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
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

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Hand-written {@link ExpressionEvaluator} for {@code REPLACE(str, regex, newStr)} when both
 * {@code regex} and {@code newStr} are foldable.
 * <p>
 * When the input column is dictionary-encoded ({@link OrdinalBytesRefBlock}) and single-valued and dense,
 * REPLACE is applied once per dictionary entry and the result is emitted as a fresh
 * {@link OrdinalBytesRefBlock} that re-uses the original ordinals. For a column with N positions backed
 * by a dictionary of size D, this reduces regex work from N calls down to D — typically a 10–50x
 * reduction on Lucene keyword columns where doc-value ordinals naturally deduplicate.
 * <p>
 * The fast-path is correctness-equivalent to the per-row path because:
 * <ul>
 *   <li>REPLACE is a pure function of its inputs, so {@code f(input)} is the same regardless of how many
 *       rows reference the same dictionary entry.</li>
 *   <li>Dictionary entries are never null (the {@link BytesRefVector} contract), so the row-level
 *       null-out logic from the per-row path doesn't apply here — nulls are already represented in
 *       the ordinals {@link IntBlock} and carried over unchanged.</li>
 *   <li>If {@link Replace#process} throws {@link IllegalArgumentException} (result-too-large) for any
 *       dictionary entry, we abandon the dictionary path for this page and fall back to per-row
 *       evaluation. The fallback emits warnings exactly as the existing per-row path does.</li>
 * </ul>
 * <p>
 * The dictionary path is gated by {@link OrdinalBytesRefBlock#isDense()} and a no-multi-value check;
 * when those don't hold, evaluation goes through the same per-row loop as
 * {@code ReplaceConstantEvaluator}.
 * <p>
 * The successful dictionary path returns an {@link OrdinalBytesRefBlock}; the per-row fallback returns
 * a materialized {@code BytesRefBlock} produced by a builder. Logical equality (values, nulls) is
 * identical between the two — see {@code BytesRefBlock.equals(BytesRefBlock, BytesRefBlock)} — but the
 * underlying block type may differ. Callers that pattern-match on block identity must not rely on
 * either shape.
 */
final class ReplaceConstantOrdinalEvaluator implements ExpressionEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ReplaceConstantOrdinalEvaluator.class);

    private final Source source;
    private final ExpressionEvaluator str;
    private final Pattern regex;
    private final byte[] literalPrefix;
    private final BytesRef newStr;
    private final DriverContext driverContext;
    private Warnings warnings;

    ReplaceConstantOrdinalEvaluator(
        Source source,
        ExpressionEvaluator str,
        Pattern regex,
        byte[] literalPrefix,
        BytesRef newStr,
        DriverContext driverContext
    ) {
        this.source = source;
        this.str = str;
        this.regex = regex;
        this.literalPrefix = literalPrefix;
        this.newStr = newStr;
        this.driverContext = driverContext;
    }

    @Override
    public Block eval(Page page) {
        try (BytesRefBlock strBlock = (BytesRefBlock) str.eval(page)) {
            OrdinalBytesRefBlock ordinals = strBlock.asOrdinals();
            if (ordinals != null && ordinals.isDense() && ordinals.mayHaveMultivaluedFields() == false) {
                Block dictResult = evalDictionary(ordinals);
                if (dictResult != null) {
                    return dictResult;
                }
                // Dictionary path bailed (an entry triggered an exception). Fall through to per-row.
            }
            return evalPerRow(page.getPositionCount(), strBlock);
        }
    }

    /**
     * Apply REPLACE once per dictionary entry and build an {@link OrdinalBytesRefBlock} that re-uses the
     * input ordinals. Returns {@code null} if any dictionary entry throws {@link IllegalArgumentException}
     * — the caller should fall back to per-row evaluation so warnings get attributed at the row level
     * exactly as the legacy path would.
     */
    private Block evalDictionary(OrdinalBytesRefBlock ordinalsBlock) {
        BytesRefVector dictionary = ordinalsBlock.getDictionaryVector();
        int dictSize = dictionary.getPositionCount();
        BytesRefVector newDictionary = null;
        try (BytesRefVector.Builder builder = driverContext.blockFactory().newBytesRefVectorBuilder(dictSize)) {
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < dictSize; i++) {
                BytesRef entry = dictionary.getBytesRef(i, scratch);
                BytesRef replaced;
                try {
                    replaced = Replace.process(entry, regex, literalPrefix, newStr);
                } catch (IllegalArgumentException e) {
                    // Bail to the per-row path so warnings are emitted from the row that triggered the failure
                    // (matching the legacy evaluator's behavior).
                    return null;
                }
                builder.appendBytesRef(replaced);
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

    // Rows to sample before giving up on a mostly-distinct page. A small sample systematically
    // overestimates the page's true distinct ratio (each new row is less likely to be "new" the more
    // rows you've already seen), so this needs to be large enough for the ratio to have converged
    // close to its page-wide value before it's trusted.
    static final int MIN_SAMPLE_BEFORE_DISABLING = 512;
    // Give up only once the sampled distinct ratio reaches this fraction (numerator/denominator), not
    // merely "more distinct than repeated". Moderately-repetitive data (e.g. ~50% distinct) still
    // benefits from caching; only near-fully-unique data (>= 80% distinct here) doesn't.
    static final int GIVEUP_DISTINCT_RATIO_NUM = 4;
    static final int GIVEUP_DISTINCT_RATIO_DEN = 5;

    /**
     * Per-row fallback: used when the input isn't a dense, single-valued {@link OrdinalBytesRefBlock}
     * (e.g. after a {@code WHERE} filter), or when the dictionary path bails on an oversized entry.
     * <p>
     * Memoizes in two tiers: a free last-success check (catches adjacent/near-adjacent duplicates) and
     * a page-scoped {@link HashMap} (catches other repeats). The map is discarded when this method
     * returns — no cross-page cache. If a page looks mostly distinct, caching is disabled for the rest
     * of that page to avoid regressing the no-repeats case.
     * <p>
     * Failures from {@link Replace#process} are never cached, so a repeated failing value still warns on
     * every occurrence.
     */
    private Block evalPerRow(int positionCount, BytesRefBlock strBlock) {
        try (BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
            // `cur`/`prev` ping-pong between two buffers so `prev` (last successful value) is never
            // clobbered by the next fetch. Updated together, only on success.
            BytesRef bufA = new BytesRef();
            BytesRef bufB = new BytesRef();
            BytesRef cur = bufA;
            BytesRef prev = null;
            BytesRef prevResult = null;

            Map<BytesRef, BytesRef> cache = null;
            boolean cacheEnabled = true;
            int rowsSeen = 0; // value-bearing rows seen so far (excludes null/multi-value rows)

            position: for (int p = 0; p < positionCount; p++) {
                switch (strBlock.getValueCount(p)) {
                    case 0:
                        result.appendNull();
                        continue position;
                    case 1:
                        break;
                    default:
                        warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
                        result.appendNull();
                        continue position;
                }
                cur = strBlock.getBytesRef(strBlock.getFirstValueIndex(p), cur);
                rowsSeen++;
                if (prev != null && prev.bytesEquals(cur)) {
                    result.appendBytesRef(prevResult);
                    continue position;
                }

                BytesRef cached = (cacheEnabled && cache != null) ? cache.get(cur) : null;
                BytesRef replaced;
                if (cached != null) {
                    replaced = cached;
                } else {
                    try {
                        replaced = Replace.process(cur, regex, literalPrefix, newStr);
                    } catch (IllegalArgumentException e) {
                        warnings().registerException(e);
                        result.appendNull();
                        continue position; // don't cache failures; a repeat retries and re-warns
                    }
                    if (cacheEnabled) {
                        if (cache == null) {
                            cache = new HashMap<>();
                        }
                        cache.put(BytesRef.deepCopyOf(cur), BytesRef.deepCopyOf(replaced));
                        if (rowsSeen >= MIN_SAMPLE_BEFORE_DISABLING
                            && cache.size() * GIVEUP_DISTINCT_RATIO_DEN >= rowsSeen * GIVEUP_DISTINCT_RATIO_NUM) {
                            cacheEnabled = false; // mostly distinct: give up for the rest of this page
                        }
                    }
                }
                result.appendBytesRef(replaced);

                // swap buffers: cur becomes prev
                BytesRef nextCur = (cur == bufA) ? bufB : bufA;
                prev = cur;
                prevResult = replaced;
                cur = nextCur;
            }
            return result.build();
        }
    }

    @Override
    public long baseRamBytesUsed() {
        // Track the constant byte arrays we hold; the regex Pattern itself is not accounted (matching the
        // convention of the generated ReplaceConstantEvaluator, which also doesn't include the Pattern).
        return BASE_RAM_BYTES_USED + str.baseRamBytesUsed() + RamUsageEstimator.sizeOf(literalPrefix) + RamUsageEstimator.sizeOf(
            newStr.bytes
        );
    }

    @Override
    public String toString() {
        return "ReplaceConstantOrdinalEvaluator[" + "str=" + str + ", regex=" + regex + ", newStr=" + newStr + "]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(str);
    }

    private Warnings warnings() {
        if (warnings == null) {
            this.warnings = driverContext.createWarnings(source);
        }
        return warnings;
    }

    static final class Factory implements ExpressionEvaluator.Factory {
        private final Source source;
        private final ExpressionEvaluator.Factory str;
        private final Pattern regex;
        private final byte[] literalPrefix;
        private final BytesRef newStr;

        Factory(Source source, ExpressionEvaluator.Factory str, Pattern regex, byte[] literalPrefix, BytesRef newStr) {
            this.source = source;
            this.str = str;
            this.regex = regex;
            this.literalPrefix = literalPrefix;
            this.newStr = newStr;
        }

        @Override
        public ReplaceConstantOrdinalEvaluator get(DriverContext context) {
            return new ReplaceConstantOrdinalEvaluator(source, str.get(context), regex, literalPrefix, newStr, context);
        }

        @Override
        public String toString() {
            return "ReplaceConstantOrdinalEvaluator[" + "str=" + str + ", regex=" + regex + ", newStr=" + newStr + "]";
        }
    }
}
