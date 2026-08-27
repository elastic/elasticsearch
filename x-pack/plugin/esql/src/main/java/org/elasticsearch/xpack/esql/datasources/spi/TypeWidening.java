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
 * <h2>The question this does and does not answer</h2>
 * Two questions look alike and are not the same:
 * <ol>
 *   <li><b>Recognition</b> &mdash; "which type can parse this string?" Only the text readers ask it,
 *       because only they are handed raw tokens; {@code "42"} might be an integer, a long, a double
 *       or a string. That knowledge is a reader's own, and stays there.</li>
 *   <li><b>Combination</b> &mdash; "which single type represents these two types?" Everyone asks it:
 *       an inferrer folding the types it observed in one column, a reader committing a column to a
 *       wider type, reconciliation merging two files. That is this class, and only this class.</li>
 * </ol>
 * Answering (2) by walking a recognition ladder is how a numeric column that later meets a timestamp
 * ends up typed {@code datetime} rather than {@code keyword}, with its bare numbers read as epochs.
 * Ask the right question and the answer follows without a special case.
 *
 * <h2>Two policies, and why they exist</h2>
 * The two are identical but for a single edge, {@code LONG + DOUBLE}: {@link Policy#INFERENCE}
 * promotes to {@code DOUBLE}, {@link Policy#RECONCILIATION} refuses and lands on {@code KEYWORD}.
 *
 * <p><b>That difference is a known inconsistency, not a design.</b> It is preserved here so that
 * moving four scattered encodings behind one authority does not also change behaviour users depend
 * on; it is not evidence that answering the same question two ways is correct. What the code shows
 * is two authors at two different times: the text inferrers implement a plain
 * {@code INTEGER -> LONG -> DOUBLE} numeric chain with no precision consideration, while
 * {@code SchemaReconciliation} was written with one, and documents excluding {@code LONG + DOUBLE}
 * for precision loss above 2^53.
 *
 * <p>The consequence is that a dataset's physical layout decides its column type: the same long and
 * double in one file give {@code DOUBLE}, and split across two files give {@code KEYWORD}. Nothing
 * justifies that; it is tracked as esql-planning#1809, kept out of this class's change because
 * resolving it either drops a deliberate precision guard or changes inference results, which is a
 * product decision rather than a refactor.
 *
 * <p>Two things follow for anyone editing this class. Do not "fix" the inconsistency by quietly
 * collapsing the policies; that is esql-planning#1809's decision to make, not a tidy-up. And do not add a second
 * difference &mdash; {@code TypeWideningTests#testPoliciesDifferOnExactlyOneEdge} fails if you do,
 * which is the point of it.
 *
 * <h2>The lattice</h2>
 * {@code KEYWORD} is the top: every pair with no closer common supertype joins there, which is what
 * makes {@link #join} total. Lossless promotions are {@code INTEGER -> LONG}, {@code INTEGER -> DOUBLE},
 * {@code DATETIME -> DATE_NANOS}, and under {@link Policy#INFERENCE} also {@code LONG -> DOUBLE}.
 * Everything else &mdash; a boolean against a number, a number against a timestamp, anything against
 * {@code IP}, {@code VERSION}, {@code UNSIGNED_LONG} or {@code NULL} &mdash; is {@code KEYWORD}.
 *
 * <p>Both policies are join-semilattices: commutative, associative and idempotent, asserted
 * exhaustively rather than assumed. That is what lets a caller fold a set of observed types in any
 * order and get the same answer, which is the property the text inferrers depend on.
 *
 * <h2>Cost</h2>
 * Static reference comparisons, no allocation and no dispatch. Callers consult it when a column's
 * type may move, and are expected to short-circuit the identity case themselves so that a settled
 * column does not reach it on every value.
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
