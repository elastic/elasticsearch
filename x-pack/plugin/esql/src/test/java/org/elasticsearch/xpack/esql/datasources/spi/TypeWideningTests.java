/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.List;

/**
 * The lattice is the specification every caller derives from, so it is asserted exhaustively rather
 * than by example: the algebraic laws hold over every pair and triple of the types these paths can
 * carry, because that is what lets callers fold a set of observed types in any order.
 */
public class TypeWideningTests extends ESTestCase {

    /**
     * Every type reachable on an external-dataset schema path: what the text inferrers produce, plus
     * what a typed CSV header can declare, plus what a columnar footer can carry, including
     * {@code UNSUPPORTED} and {@code NULL}, which the Parquet and Arrow readers put into a file schema
     * for map and depth-capped struct columns and which therefore reach reconciliation.
     */
    private static final List<DataType> UNIVERSE = List.of(
        DataType.BOOLEAN,
        DataType.INTEGER,
        DataType.LONG,
        DataType.DOUBLE,
        DataType.UNSIGNED_LONG,
        DataType.DATETIME,
        DataType.DATE_NANOS,
        DataType.IP,
        DataType.VERSION,
        DataType.NULL,
        DataType.UNSUPPORTED,
        DataType.KEYWORD
    );

    public void testJoinIsTotalOverEveryDataType() {
        // Totality over the WHOLE enum, not just the universe above: a caller that reaches this with
        // an unexpected type must get keyword, never null and never an exception.
        for (DataType a : DataType.values()) {
            for (DataType b : DataType.values()) {
                assertNotNull(a + " join " + b, TypeWidening.join(a, b));
            }
        }
    }

    /**
     * The join never invents a type: its answer is one of its inputs or the top. This is what makes
     * full-enum associativity hold without enumerating the full enum, and it would catch a future
     * promotion that routed two types to some third type nobody asked for.
     */
    public void testJoinNeverInventsAThirdType() {
        for (DataType a : DataType.values()) {
            for (DataType b : DataType.values()) {
                DataType joined = TypeWidening.join(a, b);
                assertTrue(a + " join " + b + " = " + joined, joined == a || joined == b || joined == DataType.KEYWORD);
            }
        }
    }

    /**
     * No type is an identity element, {@code NULL} least of all. Pinned because a caller folding a
     * collection is tempted to seed with {@code NULL}, which would collapse every fold to keyword.
     */
    public void testLatticeHasNoBottom() {
        for (DataType t : UNIVERSE) {
            if (t == DataType.NULL) {
                continue;
            }
            assertEquals(t.toString(), DataType.KEYWORD, TypeWidening.join(DataType.NULL, t));
        }
    }

    public void testJoinIsIdempotent() {
        for (DataType t : DataType.values()) {
            assertEquals(t.toString(), t, TypeWidening.join(t, t));
        }
    }

    public void testJoinIsCommutative() {
        for (DataType a : UNIVERSE) {
            for (DataType b : UNIVERSE) {
                assertEquals(a + " join " + b, TypeWidening.join(a, b), TypeWidening.join(b, a));
            }
        }
    }

    /**
     * The property the inferrers actually depend on: NDJSON folds an unordered type set and CSV folds
     * in row order, and both must land on the same type. Without associativity the answer would
     * depend on which value the file happened to list first.
     */
    public void testJoinIsAssociative() {
        for (DataType a : UNIVERSE) {
            for (DataType b : UNIVERSE) {
                for (DataType c : UNIVERSE) {
                    DataType left = TypeWidening.join(TypeWidening.join(a, b), c);
                    DataType right = TypeWidening.join(a, TypeWidening.join(b, c));
                    assertEquals(a + ", " + b + ", " + c, left, right);
                }
            }
        }
    }

    public void testKeywordAbsorbsEverything() {
        for (DataType t : UNIVERSE) {
            assertEquals(t.toString(), DataType.KEYWORD, TypeWidening.join(t, DataType.KEYWORD));
        }
    }

    public void testLosslessPromotions() {
        assertEquals(DataType.LONG, TypeWidening.join(DataType.INTEGER, DataType.LONG));
        assertEquals(DataType.DOUBLE, TypeWidening.join(DataType.INTEGER, DataType.DOUBLE));
        assertEquals(DataType.DATE_NANOS, TypeWidening.join(DataType.DATETIME, DataType.DATE_NANOS));
    }

    /**
     * A numeric type and a temporal type have no common supertype below keyword. Answering otherwise
     * is what let a column of numbers be typed as timestamps and read as instants in 1970.
     */
    public void testNumericAndTemporalHaveNoCommonSupertype() {
        for (DataType numeric : List.of(DataType.INTEGER, DataType.LONG, DataType.DOUBLE, DataType.UNSIGNED_LONG)) {
            for (DataType temporal : List.of(DataType.DATETIME, DataType.DATE_NANOS)) {
                assertEquals(numeric + " join " + temporal, DataType.KEYWORD, TypeWidening.join(numeric, temporal));
                assertEquals(temporal + " join " + numeric, DataType.KEYWORD, TypeWidening.join(temporal, numeric));
            }
        }
    }

    public void testBooleanJoinsNothingButItself() {
        for (DataType t : UNIVERSE) {
            if (t == DataType.BOOLEAN) {
                continue;
            }
            assertEquals(t.toString(), DataType.KEYWORD, TypeWidening.join(DataType.BOOLEAN, t));
        }
    }

    /**
     * {@code join} answers {@code DOUBLE} for {@code LONG + DOUBLE}. A single file of mixed integer
     * and fractional values and a glob that splits those values across files land on the same type.
     */
    public void testJoinPromotesLongToDouble() {
        assertEquals(DataType.DOUBLE, TypeWidening.join(DataType.LONG, DataType.DOUBLE));
        assertEquals(DataType.DOUBLE, TypeWidening.join(DataType.DOUBLE, DataType.LONG));
    }

    /**
     * {@code widenLossless} stays null for {@code LONG + DOUBLE} so strict callers can tell "no lossless
     * supertype" from "the answer is keyword". That is the only pair where {@code join} returns a
     * non-keyword type and {@code widenLossless} does not answer.
     */
    public void testWidenLosslessExcludesOnlyLongDouble() {
        List<String> excluded = new ArrayList<>();
        for (DataType a : UNIVERSE) {
            for (DataType b : UNIVERSE) {
                if (TypeWidening.widenLossless(a, b) == null && TypeWidening.join(a, b) != DataType.KEYWORD) {
                    excluded.add(a + "+" + b);
                }
            }
        }
        assertEquals("only LONG+DOUBLE is a lossy join promotion, got " + excluded, List.of("LONG+DOUBLE", "DOUBLE+LONG"), excluded);
    }

    /**
     * {@code widenLossless} is the strict form reconciliation needs, where "no lossless supertype" and
     * "the answer is keyword" are different outcomes.
     */
    public void testWidenLosslessDistinguishesNoSupertypeFromKeyword() {
        assertNull(TypeWidening.widenLossless(DataType.LONG, DataType.DOUBLE));
        assertNull(TypeWidening.widenLossless(DataType.INTEGER, DataType.DATETIME));
        assertNull(TypeWidening.widenLossless(DataType.UNSIGNED_LONG, DataType.INTEGER));
        assertEquals(DataType.LONG, TypeWidening.widenLossless(DataType.INTEGER, DataType.LONG));
        assertEquals(DataType.DATE_NANOS, TypeWidening.widenLossless(DataType.DATETIME, DataType.DATE_NANOS));
        assertEquals(DataType.KEYWORD, TypeWidening.widenLossless(DataType.KEYWORD, DataType.KEYWORD));
    }

    public void testWidenLosslessAgreesWithJoinWhereverItAnswers() {
        for (DataType a : UNIVERSE) {
            for (DataType b : UNIVERSE) {
                DataType lossless = TypeWidening.widenLossless(a, b);
                if (lossless != null) {
                    assertEquals(a + "+" + b, lossless, TypeWidening.join(a, b));
                }
            }
        }
    }
}
