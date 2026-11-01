/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.ColumnStatTypeSupport.StatBlockKind;
import org.elasticsearch.xpack.esql.datasources.ColumnStatTypeSupport.StatCoercion;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Equivalence guard for {@link ColumnStatTypeSupport}, the single source of truth that replaced five
 * independent DataType-keyed switches (servableExtremum / buildBlock in the optimizer, the
 * TextAggregatePushdownSupport MIN/MAX gate, ColumnStatsAccumulator.classify, and the cache's
 * isNumericStatType / coerceNumberToType). The table below is the verbatim mapping every one of those
 * switches used to carry; this test pins it so a drift in any consumer is a red test rather than a silent
 * behavior change.
 */
public class ColumnStatTypeSupportTests extends ESTestCase {

    /** The exact per-type mapping the five consumers encoded before consolidation. */
    private static final Map<DataType, ColumnStatTypeSupport> EXPECTED = new EnumMap<>(DataType.class);
    static {
        EXPECTED.put(DataType.INTEGER, new ColumnStatTypeSupport(StatBlockKind.INT, true, StatCoercion.EXACT_INT));
        EXPECTED.put(DataType.LONG, new ColumnStatTypeSupport(StatBlockKind.LONG, true, StatCoercion.EXACT_LONG));
        EXPECTED.put(DataType.DATETIME, new ColumnStatTypeSupport(StatBlockKind.LONG, true, StatCoercion.EXACT_LONG));
        EXPECTED.put(DataType.DATE_NANOS, new ColumnStatTypeSupport(StatBlockKind.LONG, true, StatCoercion.EXACT_LONG));
        EXPECTED.put(DataType.COUNTER_LONG, new ColumnStatTypeSupport(StatBlockKind.LONG, false, StatCoercion.NONE));
        EXPECTED.put(DataType.DOUBLE, new ColumnStatTypeSupport(StatBlockKind.DOUBLE, true, StatCoercion.WIDEN_DOUBLE));
        EXPECTED.put(DataType.COUNTER_DOUBLE, new ColumnStatTypeSupport(StatBlockKind.DOUBLE, false, StatCoercion.NONE));
        EXPECTED.put(DataType.BOOLEAN, new ColumnStatTypeSupport(StatBlockKind.BOOLEAN, true, StatCoercion.NONE));
        EXPECTED.put(DataType.KEYWORD, new ColumnStatTypeSupport(StatBlockKind.BYTES_REF, true, StatCoercion.NONE));
        EXPECTED.put(DataType.TEXT, new ColumnStatTypeSupport(StatBlockKind.BYTES_REF, true, StatCoercion.NONE));
        EXPECTED.put(DataType.IP, new ColumnStatTypeSupport(StatBlockKind.BYTES_REF, true, StatCoercion.NONE));
        // UNSIGNED_LONG is servable (Parquet sign-flip-encodes its stat into ESQL's wire form, like the scan, so
        // the LONG arm serves it verbatim — elastic/elasticsearch#152858), coercible (stale-extremum drop), but
        // NOT harvestable (text never tracks it).
        EXPECTED.put(DataType.UNSIGNED_LONG, new ColumnStatTypeSupport(StatBlockKind.LONG, false, StatCoercion.EXACT_LONG));
    }

    /** Every DataType maps exactly as the table expects; every other type maps to {@code null}. */
    public void testExhaustiveMapping() {
        for (DataType type : DataType.values()) {
            ColumnStatTypeSupport actual = ColumnStatTypeSupport.of(type);
            ColumnStatTypeSupport expected = EXPECTED.get(type);
            assertEquals("ColumnStatTypeSupport.of(" + type + ")", expected, actual);
        }
    }

    /** {@code servable()} is exactly {@code blockKind != null} — the old servableExtremum / buildBlock arm set. */
    public void testServableIsBlockKindPresence() {
        for (DataType type : DataType.values()) {
            ColumnStatTypeSupport s = ColumnStatTypeSupport.of(type);
            if (s == null) {
                continue;
            }
            assertEquals("servable() for " + type, s.blockKind() != null, s.servable());
        }
    }

    /** The servable set (blockKind != null) is exactly the 11 types the old servableExtremum switch served. */
    public void testServableSet() {
        Set<DataType> servable = EnumSet.noneOf(DataType.class);
        for (DataType type : DataType.values()) {
            ColumnStatTypeSupport s = ColumnStatTypeSupport.of(type);
            if (s != null && s.servable()) {
                servable.add(type);
            }
        }
        assertEquals(
            EnumSet.of(
                DataType.INTEGER,
                DataType.LONG,
                DataType.DATETIME,
                DataType.DATE_NANOS,
                DataType.COUNTER_LONG,
                DataType.DOUBLE,
                DataType.COUNTER_DOUBLE,
                DataType.BOOLEAN,
                DataType.KEYWORD,
                DataType.TEXT,
                DataType.IP,
                DataType.UNSIGNED_LONG
            ),
            servable
        );
    }

    /** The harvestable set is exactly the 9 types the old TextAggregatePushdownSupport MIN_MAX_TYPES held (no counters). */
    public void testHarvestableSet() {
        Set<DataType> harvestable = EnumSet.noneOf(DataType.class);
        for (DataType type : DataType.values()) {
            ColumnStatTypeSupport s = ColumnStatTypeSupport.of(type);
            if (s != null && s.harvestable()) {
                harvestable.add(type);
            }
        }
        assertEquals(
            EnumSet.of(
                DataType.BOOLEAN,
                DataType.INTEGER,
                DataType.LONG,
                DataType.DOUBLE,
                DataType.DATETIME,
                DataType.DATE_NANOS,
                DataType.KEYWORD,
                DataType.TEXT,
                DataType.IP
            ),
            harvestable
        );
    }

    /** The numeric-coercible set (coercion != NONE) is exactly the old cache isNumericStatType set. */
    public void testNumericCoercibleSet() {
        Set<DataType> coercible = EnumSet.noneOf(DataType.class);
        for (DataType type : DataType.values()) {
            ColumnStatTypeSupport s = ColumnStatTypeSupport.of(type);
            if (s != null && s.coercion() != StatCoercion.NONE) {
                coercible.add(type);
            }
        }
        assertEquals(
            EnumSet.of(DataType.INTEGER, DataType.LONG, DataType.DOUBLE, DataType.DATETIME, DataType.DATE_NANOS, DataType.UNSIGNED_LONG),
            coercible
        );
    }

    /**
     * The orthogonality the record's javadoc warns about, pinned as a test: coercibility is NOT derivable from
     * servability. UNSIGNED_LONG is servable AND coercible; the counters are servable-but-not-coercible — same
     * servability, different coercibility.
     */
    public void testCoercionOrthogonalToServability() {
        ColumnStatTypeSupport unsignedLong = ColumnStatTypeSupport.of(DataType.UNSIGNED_LONG);
        assertTrue("UNSIGNED_LONG must be servable (Parquet sign-flip-encodes its stat)", unsignedLong.servable());
        assertEquals("UNSIGNED_LONG must be coercible", StatCoercion.EXACT_LONG, unsignedLong.coercion());

        for (DataType counter : EnumSet.of(DataType.COUNTER_LONG, DataType.COUNTER_DOUBLE)) {
            ColumnStatTypeSupport s = ColumnStatTypeSupport.of(counter);
            assertTrue(counter + " must be servable", s.servable());
            assertEquals(counter + " must not be coercible", StatCoercion.NONE, s.coercion());
        }
    }
}
