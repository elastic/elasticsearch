/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.type.DataType;

/**
 * The single source of truth for how a column's ESQL {@link DataType} participates in the external-source
 * per-column statistics pipeline (harvest, cache classification, warm-path serving, and block construction).
 * Five DataType-keyed switches used to encode this mapping independently and drifted against one another;
 * they now all dispatch on {@link #of(DataType)}.
 * <p>
 * Three orthogonal facts per type:
 * <ul>
 *   <li>{@link #blockKind} — the block flavour a served MIN/MAX extremum is materialized into. {@code null}
 *   means the type is NOT servable (no {@code buildBlock} arm exists); {@link #servable()} is exactly
 *   {@code blockKind != null}.</li>
 *   <li>{@link #harvestable} — whether the cold-scan {@code ColumnStatsAccumulator} tracks min/max for this
 *   type (i.e. the type is "text-pushable"). Text-format warm stats exist for a column ONLY if the
 *   accumulator harvested them, so a non-harvestable type can never produce a warm extremum even though it
 *   may still be servable (see the WARNING below).</li>
 *   <li>{@link #coercion} — how a stat {@link Number} is normalized to the column's resolved type before it
 *   is folded/served.</li>
 * </ul>
 * <p>
 * <b>WARNING — the three flags are ORTHOGONAL; do not derive one from another.</b>
 * They carry genuinely independent facts:
 * <ul>
 *   <li>The counters ({@code COUNTER_LONG}/{@code COUNTER_DOUBLE}) ARE servable (they carry a
 *   {@code blockKind}) yet are NOT coercible ({@code coercion == NONE}) and NOT harvestable — so servability
 *   implies neither coercibility nor harvestability.</li>
 *   <li>{@code UNSIGNED_LONG} is servable ({@code blockKind == LONG}: Parquet sign-flip-encodes its stat into
 *   ESQL's wire form via {@code ParquetColumnDecoding#encodeUnsignedLong}, exactly as the scan does, so the LONG
 *   arm serves the encoded value verbatim — byte-identical to the scan) AND coercible
 *   ({@code coercion == EXACT_LONG}, which still neutralizes a stale committed extremum that cannot be
 *   represented in the resolved type) yet NOT harvestable (the text accumulator never tracks it).</li>
 * </ul>
 * Deriving coercibility from servability (or vice versa) would break the stale-extremum neutralization that
 * {@code UNSIGNED_LONG} depends on.
 */
public record ColumnStatTypeSupport(@Nullable StatBlockKind blockKind, boolean harvestable, StatCoercion coercion) {

    public ColumnStatTypeSupport {
        // A harvestable type is always servable. ColumnStatsAccumulator.classify relies on this: it maps every
        // harvestable type to a tracked block kind, so a harvestable-but-not-servable entry would have no arm.
        assert harvestable == false || blockKind != null
            : "harvestable type must have a blockKind (be servable): " + blockKind + "/" + harvestable + "/" + coercion;
    }

    /** The block flavour a served MIN/MAX extremum is materialized into. */
    public enum StatBlockKind {
        INT,
        LONG,
        DOUBLE,
        BOOLEAN,
        BYTES_REF
    }

    /** How a stat {@link Number} is normalized to the column's resolved type before it is folded/served. */
    public enum StatCoercion {
        NONE,
        EXACT_INT,
        EXACT_LONG,
        WIDEN_DOUBLE
    }

    /** True iff a served MIN/MAX extremum can be materialized into a block for this type (i.e. {@code blockKind != null}). */
    public boolean servable() {
        return blockKind != null;
    }

    /**
     * The support record for {@code type}, or {@code null} if the type does not participate in the external-source
     * per-column statistics pipeline at all (neither servable, harvestable, nor coercible).
     */
    @Nullable
    public static ColumnStatTypeSupport of(DataType type) {
        return switch (type) {
            case INTEGER -> new ColumnStatTypeSupport(StatBlockKind.INT, true, StatCoercion.EXACT_INT);
            case LONG, DATETIME, DATE_NANOS -> new ColumnStatTypeSupport(StatBlockKind.LONG, true, StatCoercion.EXACT_LONG);
            case COUNTER_LONG -> new ColumnStatTypeSupport(StatBlockKind.LONG, false, StatCoercion.NONE);
            case DOUBLE -> new ColumnStatTypeSupport(StatBlockKind.DOUBLE, true, StatCoercion.WIDEN_DOUBLE);
            case COUNTER_DOUBLE -> new ColumnStatTypeSupport(StatBlockKind.DOUBLE, false, StatCoercion.NONE);
            case BOOLEAN -> new ColumnStatTypeSupport(StatBlockKind.BOOLEAN, true, StatCoercion.NONE);
            case KEYWORD, TEXT, IP -> new ColumnStatTypeSupport(StatBlockKind.BYTES_REF, true, StatCoercion.NONE);
            // Servable: Parquet sign-flip-encodes the uint64 stat into ESQL's wire form (encodeUnsignedLong),
            // exactly as the scan does, so the LONG arm serves the encoded value byte-identically to a scan
            // (elastic/elasticsearch#152858). Not harvestable (text never tracks it); still EXACT_LONG-coercible.
            case UNSIGNED_LONG -> new ColumnStatTypeSupport(StatBlockKind.LONG, false, StatCoercion.EXACT_LONG);
            default -> null;
        };
    }
}
