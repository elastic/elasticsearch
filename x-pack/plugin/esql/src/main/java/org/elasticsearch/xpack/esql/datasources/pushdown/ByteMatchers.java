/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.pushdown;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.lucene.queries.BinaryDocValuesContainsTermQuery;

import java.util.Arrays;

/**
 * Byte-level matching primitives shared by datasource pushdown evaluators.
 *
 * <p>All operations are <b>byte-level</b> — they treat their inputs as opaque {@code byte[]} and
 * make no assumption about character encoding or case. Callers that work with valid UTF-8 KEYWORD
 * values (the Elasticsearch contract) get codepoint-correct semantics for free thanks to UTF-8's
 * self-synchronizing property: a valid UTF-8 substring matches at byte boundaries iff it matches
 * at codepoint boundaries. For inputs that are not valid UTF-8, the helpers degrade to plain byte
 * comparison — caller's responsibility if that diverges from the desired semantics.
 *
 * <p>Each primitive routes the heavy lifting through the fastest implementation available on the
 * platform:
 * <ul>
 *   <li>{@link #equals(BytesRef, BytesRef)}, {@link #startsWith(BytesRef, BytesRef)},
 *       {@link #endsWith(BytesRef, BytesRef)} delegate to {@link Arrays#equals(byte[], int, int,
 *       byte[], int, int)}, which HotSpot intrinsifies to AVX2/AVX-512 on x86 and NEON on ARM
 *       (with partial-inlining for sizes &le; 64 bytes — no stub call).</li>
 *   <li>{@link #containsLiteral(BytesRef, BytesRef)} delegates to
 *       {@link BinaryDocValuesContainsTermQuery#contains(byte[], int, int, BytesRef)}, a thin
 *       wrapper over {@code ESVectorUtil#contains} which uses Panama Vector API first+last byte
 *       filtering for values &ge; 24 bytes.</li>
 * </ul>
 *
 * <p>The methods are deliberately small and inlinable so the JIT can fuse them into per-row
 * predicate loops without virtual-call overhead. Datasource evaluators (today: Parquet
 * late-materialization; future: any in-Java filter on byte-shaped columns) should call these
 * helpers rather than open-coding the byte loops.
 */
public final class ByteMatchers {

    private ByteMatchers() {}

    /**
     * Returns {@code true} iff the two byte sequences have identical length and content.
     * Equivalent to {@link BytesRef#bytesEquals(BytesRef)} but routed through
     * {@link Arrays#equals(byte[], int, int, byte[], int, int)} so it picks up the JDK's
     * vectorized intrinsic on hot paths.
     */
    public static boolean equals(BytesRef value, BytesRef target) {
        if (value.length != target.length) {
            return false;
        }
        return Arrays.equals(
            value.bytes,
            value.offset,
            value.offset + value.length,
            target.bytes,
            target.offset,
            target.offset + target.length
        );
    }

    /**
     * Returns {@code true} iff {@code value} begins with {@code prefix}. An empty prefix matches
     * every value (including an empty one). Returns {@code false} when {@code prefix} is longer
     * than {@code value}.
     */
    public static boolean startsWith(BytesRef value, BytesRef prefix) {
        if (prefix.length == 0) {
            return true;
        }
        if (prefix.length > value.length) {
            return false;
        }
        return Arrays.equals(
            value.bytes,
            value.offset,
            value.offset + prefix.length,
            prefix.bytes,
            prefix.offset,
            prefix.offset + prefix.length
        );
    }

    /**
     * Returns {@code true} iff {@code value} ends with {@code suffix}. An empty suffix matches
     * every value (including an empty one). Returns {@code false} when {@code suffix} is longer
     * than {@code value}.
     */
    public static boolean endsWith(BytesRef value, BytesRef suffix) {
        if (suffix.length == 0) {
            return true;
        }
        if (suffix.length > value.length) {
            return false;
        }
        int tailStart = value.offset + value.length - suffix.length;
        return Arrays.equals(value.bytes, tailStart, tailStart + suffix.length, suffix.bytes, suffix.offset, suffix.offset + suffix.length);
    }

    /**
     * Returns {@code true} iff {@code value} contains {@code literal} as a contiguous subsequence.
     * An empty literal matches every value (including an empty one). Routes through the SIMD
     * substring search exposed by {@link BinaryDocValuesContainsTermQuery#contains(byte[], int,
     * int, BytesRef)}.
     *
     * <p>The Lucene query class is used purely as a static-method shim around
     * {@code ESVectorUtil#contains}: {@code simdvec}'s module exports the SIMD utility only to
     * {@code org.elasticsearch.server}, so plugins (unnamed modules) cannot import it directly.
     * Calling it through this server-side neighbor keeps the SIMD intent without forcing every
     * consumer plugin to add a {@code simdvec} dependency it cannot legally use at runtime. If
     * the shim ever moves to a more semantically-named {@code :server} home, only this method
     * needs to change.
     */
    public static boolean containsLiteral(BytesRef value, BytesRef literal) {
        if (literal.length == 0) {
            return true;
        }
        if (literal.length > value.length) {
            return false;
        }
        return BinaryDocValuesContainsTermQuery.contains(value.bytes, value.offset, value.length, literal);
    }

    /**
     * Composite predicate: returns {@code true} iff {@code value} starts with {@code prefix},
     * ends with {@code suffix}, and contains {@code literal} as a contiguous subsequence anywhere
     * <i>between</i> the matched prefix and suffix. Any of the three components may be
     * {@code null} or empty, in which case the corresponding check is skipped.
     *
     * <p>This implements the SQL {@code LIKE 'prefix%literal%suffix'} family in a single pass:
     * <ul>
     *   <li>{@code prefix*} → {@code (prefix, null, null)}</li>
     *   <li>{@code *suffix} → {@code (null, null, suffix)}</li>
     *   <li>{@code *literal*} → {@code (null, literal, null)}</li>
     *   <li>{@code prefix*suffix} → {@code (prefix, null, suffix)}</li>
     *   <li>{@code prefix*literal*} → {@code (prefix, literal, null)}</li>
     *   <li>{@code *literal*suffix} → {@code (null, literal, suffix)}</li>
     *   <li>{@code prefix*literal*suffix} → {@code (prefix, literal, suffix)}</li>
     * </ul>
     *
     * <p>The order of checks is prefix → suffix → literal: the affix checks are short JDK-
     * intrinsified equality comparisons (typically &le; 32 bytes) and reject the bulk of
     * non-matching values cheaply, leaving the more expensive SIMD substring scan for survivors.
     * For correctness, the literal scan is restricted to the bytes <i>strictly between</i> the
     * prefix and suffix regions: this prevents the literal from straddling and double-counting
     * either affix region (e.g. {@code value="aXa"}, {@code prefix="a"}, {@code literal="aXa"},
     * {@code suffix="a"} must match iff {@code aXa} appears in the empty middle slice — which it
     * does not — not iff it appears anywhere in the full value).
     *
     * <p>A single combined-length guard rejects any value that cannot host all three components
     * end-to-end without overlap: when {@code prefix.length + literal.length + suffix.length >
     * value.length} the predicate returns {@code false} immediately. This subsumes both the
     * "affixes don't fit" case (literal length zero) and the "literal can't fit between the
     * affixes" case, and it matches the non-overlapping-concatenation semantics that
     * {@code ByteRunAutomaton} uses on the same patterns — the parity callers depend on.
     *
     * <p>When all three components are absent (every parameter {@code null} or empty) this
     * returns {@code true} unconditionally — the vacuum predicate. Callers typically partition
     * that case upstream as a {@code matchesAll} fast path; reaching this method with all-null
     * arguments is harmless but wasteful.
     */
    public static boolean affixContains(BytesRef value, @Nullable BytesRef prefix, @Nullable BytesRef literal, @Nullable BytesRef suffix) {
        int prefixLen = prefix == null ? 0 : prefix.length;
        int suffixLen = suffix == null ? 0 : suffix.length;
        int literalLen = literal == null ? 0 : literal.length;
        if (prefixLen + suffixLen + literalLen > value.length) {
            return false;
        }
        if (prefixLen > 0 && startsWith(value, prefix) == false) {
            return false;
        }
        if (suffixLen > 0 && endsWith(value, suffix) == false) {
            return false;
        }
        if (literalLen == 0) {
            return true;
        }
        // The literal must fit in the middle slice (between prefix and suffix). Building a
        // sub-BytesRef would allocate; instead, call the underlying byte-array contains directly
        // with the slice bounds.
        int sliceOffset = value.offset + prefixLen;
        int sliceLength = value.length - prefixLen - suffixLen;
        return BinaryDocValuesContainsTermQuery.contains(value.bytes, sliceOffset, sliceLength, literal);
    }
}
