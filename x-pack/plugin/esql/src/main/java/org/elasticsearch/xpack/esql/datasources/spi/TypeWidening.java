/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.type.DataType;

/**
 * The one answer to "which single type represents both of these types?" for external datasets.
 *
 * <p>It answers <b>combination</b> only. The other question a text reader asks &mdash; "which type can
 * parse this string?" &mdash; is recognition, needs the raw token, and stays in the reader. Answering
 * combination by walking a recognition ladder is how a numeric column that later meets a timestamp
 * ends up typed {@code datetime}, with its bare numbers read as epochs.
 *
 * <p>Promotions: {@code INTEGER -> LONG}, {@code INTEGER -> DOUBLE}, {@code DATETIME -> DATE_NANOS},
 * and under {@link Policy#INFERENCE} also {@code LONG -> DOUBLE}. {@code KEYWORD} is the top, so
 * {@link #join} is total and no caller needs a fallback of its own &mdash; a fallback per call site is
 * how this subsystem came to hold four different answers. There is no bottom: {@code join(NULL, X)} is
 * {@code KEYWORD}, so a fold must seed with its own first element (see {@link #join}).
 *
 * <p>Both policies are join-semilattices, which is what lets NDJSON fold an unordered set and CSV fold
 * in row order and still agree.
 *
 * <p><b>The two policies differ on {@code LONG + DOUBLE} only, and that difference is a known
 * inconsistency rather than a design</b> &mdash; the text inferrers implement a plain numeric chain
 * with no precision consideration, while {@code SchemaReconciliation} documents excluding that pair
 * above 2^53. The consequence is that a dataset's physical layout decides its column type. It is
 * preserved here so that unifying four encodings does not also change shipped behaviour, and tracked
 * as esql-planning#1809 &mdash; do not collapse the policies to "fix" it, that is #1809's call. A test
 * fails if a second difference appears.
 *
 * <p>Static reference comparisons, no allocation, no dispatch. Callers short-circuit the identity case
 * themselves so a settled column does not reach it per value.
 */
public final class TypeWidening {

    /**
     * Which of the two readings of the lattice the caller wants. They differ on {@code LONG + DOUBLE}
     * and nothing else, and that difference is a known inconsistency rather than a design &mdash; see
     * the class javadoc and esql-planning#1809 before changing either.
     */
    public enum Policy {
        /** Folding types observed within one file, whose result is cached and cannot warn. */
        INFERENCE,
        /** Merging committed types across files, recomputed per query and able to warn. */
        RECONCILIATION
    }

    private TypeWidening() {}

    /**
     * The single type that represents both, always. Returns {@code KEYWORD} when there is no closer
     * common supertype, so callers never have to invent a fallback of their own &mdash; inventing one
     * per call site is how the answers drifted apart in the first place.
     *
     * <p><b>There is a top but no bottom.</b> No type is an identity element: {@code join(NULL, X)} is
     * {@code KEYWORD}, not {@code X}, because a column that is null in one file and an integer in
     * another is not thereby an integer column. So a caller folding a collection must seed the fold
     * with the collection's own first element and handle emptiness itself &mdash; seeding with
     * {@code NULL} (or any other type as a stand-in for "nothing yet") collapses every fold to
     * {@code KEYWORD}.
     */
    public static DataType join(DataType a, DataType b, Policy policy) {
        if (a == b) {
            return a;
        }
        DataType widened = widenOrdered(a, b, policy);
        if (widened != null) {
            return widened;
        }
        widened = widenOrdered(b, a, policy);
        return widened != null ? widened : DataType.KEYWORD;
    }

    /**
     * The strict form: the common supertype when one exists without loss, else {@code null}. Callers
     * that need to distinguish "no lossless supertype" from "the answer is keyword" use this;
     * everyone else wants {@link #join}.
     */
    @Nullable
    public static DataType widenLossless(DataType a, DataType b) {
        if (a == b) {
            return a;
        }
        DataType widened = widenOrdered(a, b, Policy.RECONCILIATION);
        return widened != null ? widened : widenOrdered(b, a, Policy.RECONCILIATION);
    }

    /**
     * The promotions, stated once in one direction; {@link #join} tries both orderings so this need
     * only name each pair once.
     */
    @Nullable
    private static DataType widenOrdered(DataType from, DataType to, Policy policy) {
        if (from == DataType.INTEGER && (to == DataType.LONG || to == DataType.DOUBLE)) {
            return to;
        }
        if (from == DataType.LONG && to == DataType.DOUBLE) {
            // The one edge the two policies disagree on — see the class javadoc.
            return policy == Policy.INFERENCE ? DataType.DOUBLE : null;
        }
        if (from == DataType.DATETIME && to == DataType.DATE_NANOS) {
            return DataType.DATE_NANOS;
        }
        return null;
    }
}
