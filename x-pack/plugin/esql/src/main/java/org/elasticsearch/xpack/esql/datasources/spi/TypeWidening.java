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
 * <p>It answers <b>combination</b> only. The other question a text reader asks (which type can
 * parse this string?) is recognition, needs the raw token, and stays in the reader. Answering
 * combination by walking a recognition ladder is how a numeric column that later meets a timestamp
 * ends up typed {@code datetime}, with its bare numbers read as epochs.
 *
 * <p>Promotions: {@code INTEGER -> LONG}, {@code INTEGER -> DOUBLE}, {@code LONG -> DOUBLE},
 * {@code DATETIME -> DATE_NANOS}. {@code KEYWORD} is the top, so {@link #join} is total and no
 * caller needs a fallback of its own. A fallback per call site is how this subsystem came to hold
 * four different answers. There is no bottom: {@code join(NULL, X)} is {@code KEYWORD}, so a fold
 * must seed with its own first element (see {@link #join}).
 *
 * <p>{@code join} is a join-semilattice, which is what lets NDJSON fold an unordered set and CSV
 * fold in row order and still agree. {@link #widenLossless} is the strict form: it answers the same
 * promotions except {@code LONG -> DOUBLE}, which is not exact above {@code 2^53}.
 *
 * <p>Static reference comparisons, no allocation, no dispatch. Callers short-circuit the identity case
 * themselves so a settled column does not reach it per value.
 */
public final class TypeWidening {

    private TypeWidening() {}

    /**
     * The single type that represents both, always. Returns {@code KEYWORD} when there is no closer
     * common supertype, so callers never have to invent a fallback of their own. Inventing one per
     * call site is how the answers drifted apart in the first place.
     *
     * <p><b>There is a top but no bottom.</b> No type is an identity element: {@code join(NULL, X)} is
     * {@code KEYWORD}, not {@code X}, because a column that is null in one file and an integer in
     * another is not thereby an integer column. So a caller folding a collection must seed the fold
     * with the collection's own first element and handle emptiness itself. Seeding with {@code NULL}
     * (or any other type as a stand-in for "nothing yet") collapses every fold to {@code KEYWORD}.
     */
    public static DataType join(DataType a, DataType b) {
        if (a == b) {
            return a;
        }
        DataType widened = widenOrdered(a, b);
        if (widened != null) {
            return widened;
        }
        widened = widenOrdered(b, a);
        return widened != null ? widened : DataType.KEYWORD;
    }

    /**
     * The strict form: the common supertype when one exists without loss, else {@code null}. Callers
     * that need to distinguish "no lossless supertype" from "the answer is keyword" use this;
     * everyone else wants {@link #join}. {@code LONG + DOUBLE} is the one {@link #join} promotion
     * this rejects, because integers above {@code 2^53} are not exact doubles.
     */
    @Nullable
    public static DataType widenLossless(DataType a, DataType b) {
        if (a == b) {
            return a;
        }
        if (isLongDoublePair(a, b)) {
            return null;
        }
        DataType widened = widenOrdered(a, b);
        return widened != null ? widened : widenOrdered(b, a);
    }

    /**
     * The promotions, stated once in one direction; {@link #join} tries both orderings so this need
     * only name each pair once.
     */
    @Nullable
    private static DataType widenOrdered(DataType from, DataType to) {
        if (from == DataType.INTEGER && (to == DataType.LONG || to == DataType.DOUBLE)) {
            return to;
        }
        if (from == DataType.LONG && to == DataType.DOUBLE) {
            return DataType.DOUBLE;
        }
        if (from == DataType.DATETIME && to == DataType.DATE_NANOS) {
            return DataType.DATE_NANOS;
        }
        return null;
    }

    private static boolean isLongDoublePair(DataType a, DataType b) {
        return (a == DataType.LONG && b == DataType.DOUBLE) || (a == DataType.DOUBLE && b == DataType.LONG);
    }
}
