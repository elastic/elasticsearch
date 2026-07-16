/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.operator.topn.SharedMinCompetitive;
import org.elasticsearch.compute.operator.topn.SharedNumericThreshold;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.Objects;

/**
 * Per-query descriptor for the live TopN threshold exposed to format readers.
 * <p>
 * A descriptor is backed by exactly one side-channel, selected by {@link #elementType()}:
 * <ul>
 *     <li>fixed-width numeric sort keys (LONG, INT, DOUBLE, BOOLEAN) publish a raw {@code long}
 *         bound through a {@link SharedNumericThreshold} fed by the {@code NumericTopNOperator};</li>
 *     <li>{@code BYTES_REF} sort keys (keyword/text) publish an encoded competitive bound through a
 *         {@link SharedMinCompetitive} fed by the generic {@code TopNOperator}. The encoded bound is
 *         decoded back to a raw {@link BytesRef} on demand so readers can compare it directly against
 *         a column's min/max string statistics.</li>
 * </ul>
 * Readers branch on {@link #elementType()} and call the matching {@code dominates} overload.
 */
public final class DynamicThreshold implements Releasable {
    private final String columnName;
    private final ElementType elementType;
    private final boolean ascending;
    private final boolean nullsFirst;
    @Nullable
    private final SharedNumericThreshold numericChannel;
    @Nullable
    private final SharedMinCompetitive bytesChannel;

    public DynamicThreshold(
        String columnName,
        ElementType elementType,
        boolean ascending,
        boolean nullsFirst,
        SharedNumericThreshold channel
    ) {
        this(columnName, elementType, ascending, nullsFirst, channel, null);
    }

    /**
     * Build a {@code BYTES_REF} threshold backed by the generic {@code TopNOperator}'s
     * {@link SharedMinCompetitive} side-channel.
     */
    public DynamicThreshold(String columnName, boolean ascending, boolean nullsFirst, SharedMinCompetitive channel) {
        this(columnName, ElementType.BYTES_REF, ascending, nullsFirst, null, channel);
    }

    private DynamicThreshold(
        String columnName,
        ElementType elementType,
        boolean ascending,
        boolean nullsFirst,
        @Nullable SharedNumericThreshold numericChannel,
        @Nullable SharedMinCompetitive bytesChannel
    ) {
        // Enforced at runtime (not just via assert) so the dual-mode API stays safe with assertions
        // disabled: setNumericThresholdSupplier accepts any ElementType, so a mismatched type/channel
        // pairing must fail loudly at construction rather than NPE later in a reader's hot path.
        Objects.requireNonNull(elementType, "elementType must not be null");
        if ((numericChannel == null) == (bytesChannel == null)) {
            throw new IllegalArgumentException("exactly one backing channel must be set");
        }
        if ((elementType == ElementType.BYTES_REF) != (bytesChannel != null)) {
            throw new IllegalArgumentException(
                "BYTES_REF thresholds must use the BytesRef channel and numeric thresholds the numeric channel, got [" + elementType + "]"
            );
        }
        this.columnName = columnName;
        this.elementType = elementType;
        this.ascending = ascending;
        this.nullsFirst = nullsFirst;
        this.numericChannel = numericChannel;
        this.bytesChannel = bytesChannel;
    }

    public String columnName() {
        return columnName;
    }

    public ElementType elementType() {
        return elementType;
    }

    public boolean nullsFirst() {
        return nullsFirst;
    }

    public boolean dominates(long rangeMin, long rangeMax) {
        requireNumeric();
        if (numericChannel.noFurtherCandidates()) {
            return true;
        }
        return numericChannel.dominates(rangeMin, rangeMax);
    }

    public boolean dominates(long rangeMin, long rangeMax, long nullCount) {
        requireNumeric();
        if (numericChannel.noFurtherCandidates()) {
            return true;
        }
        if (nullCount > 0 && nullsFirst) {
            return false;
        }
        return numericChannel.dominates(rangeMin, rangeMax);
    }

    /**
     * Whether the {@code [rangeMin, rangeMax]} string statistics of a row group/stripe are strictly
     * dominated by the current {@code BYTES_REF} competitive bound. Mirrors
     * {@link #dominates(long, long)}: equality is deliberately not skipped because row-position
     * tiebreakers can still matter. Returns {@code false} when no usable bound has been published
     * yet (or the bound is a null key).
     */
    public boolean dominates(BytesRef rangeMin, BytesRef rangeMax) {
        requireBytes();
        if (bytesChannel.noFurtherCandidates()) {
            return true;
        }
        BytesRef bound = bytesRefBound();
        if (bound == null) {
            return false;
        }
        return ascending ? rangeMin.compareTo(bound) > 0 : rangeMax.compareTo(bound) < 0;
    }

    public boolean dominates(BytesRef rangeMin, BytesRef rangeMax, long nullCount) {
        requireBytes();
        if (bytesChannel.noFurtherCandidates()) {
            return true;
        }
        if (nullCount > 0 && nullsFirst) {
            return false;
        }
        // Inlined rather than delegating to dominates(rangeMin, rangeMax) to avoid re-running the
        // guard and side-channel reads on the per-row-group hot path, mirroring the numeric 3-arg.
        BytesRef bound = bytesRefBound();
        if (bound == null) {
            return false;
        }
        return ascending ? rangeMin.compareTo(bound) > 0 : rangeMax.compareTo(bound) < 0;
    }

    public boolean dominatesNulls(long nullCount) {
        if (noFurtherCandidates()) {
            return true;
        }
        return nullCount > 0 && nullsFirst == false && hasBound();
    }

    public boolean noFurtherCandidates() {
        return elementType == ElementType.BYTES_REF ? bytesChannel.noFurtherCandidates() : numericChannel.noFurtherCandidates();
    }

    public long current() {
        requireNumeric();
        return numericChannel.current();
    }

    @Nullable
    private BytesRef bytesRefBound() {
        return bytesChannel == null ? null : bytesChannel.minCompetitiveValue();
    }

    /**
     * Guards the numeric ({@code long}) overloads against being called on a {@code BYTES_REF}
     * threshold, where {@code numericChannel} is {@code null}. The two modes are mutually exclusive
     * by construction, so a mismatch is a wiring bug rather than a runtime data condition.
     */
    private void requireNumeric() {
        if (numericChannel == null) {
            throw new IllegalStateException(
                "numeric threshold operation invoked on a [" + elementType + "] DynamicThreshold backed by a BytesRef channel"
            );
        }
    }

    /**
     * Guards the {@link BytesRef} overloads against being called on a numeric threshold, where
     * {@code bytesChannel} is {@code null}.
     */
    private void requireBytes() {
        if (bytesChannel == null) {
            throw new IllegalStateException(
                "BytesRef threshold operation invoked on a [" + elementType + "] DynamicThreshold backed by a numeric channel"
            );
        }
    }

    public boolean hasBound() {
        if (elementType == ElementType.BYTES_REF) {
            return bytesRefBound() != null;
        }
        return ascending ? current() != Long.MAX_VALUE : current() != Long.MIN_VALUE;
    }

    @Override
    public void close() {
        Releasables.close(numericChannel, bytesChannel);
    }
}
