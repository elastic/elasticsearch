/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.TypeWidening.Policy;

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
     * what a typed CSV header or a columnar footer can declare on top.
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
        DataType.KEYWORD
    );

    public void testJoinIsTotalOverEveryDataType() {
        // Totality over the WHOLE enum, not just the universe above: a caller that reaches this with
        // an unexpected type must get keyword, never null and never an exception.
        for (DataType a : DataType.values()) {
            for (DataType b : DataType.values()) {
                for (Policy policy : Policy.values()) {
                    assertNotNull(a + " join " + b + " [" + policy + "]", TypeWidening.join(a, b, policy));
                }
            }
        }
    }

    public void testJoinIsIdempotent() {
        for (DataType t : DataType.values()) {
            for (Policy policy : Policy.values()) {
                assertEquals(t + " [" + policy + "]", t, TypeWidening.join(t, t, policy));
            }
        }
    }

    public void testJoinIsCommutative() {
        for (DataType a : UNIVERSE) {
            for (DataType b : UNIVERSE) {
                for (Policy policy : Policy.values()) {
                    assertEquals(a + " join " + b + " [" + policy + "]", TypeWidening.join(a, b, policy), TypeWidening.join(b, a, policy));
                }
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
                    for (Policy policy : Policy.values()) {
                        DataType left = TypeWidening.join(TypeWidening.join(a, b, policy), c, policy);
                        DataType right = TypeWidening.join(a, TypeWidening.join(b, c, policy), policy);
                        assertEquals(a + ", " + b + ", " + c + " [" + policy + "]", left, right);
                    }
                }
            }
        }
    }

    public void testKeywordAbsorbsEverything() {
        for (DataType t : UNIVERSE) {
            for (Policy policy : Policy.values()) {
                assertEquals(t + " [" + policy + "]", DataType.KEYWORD, TypeWidening.join(t, DataType.KEYWORD, policy));
            }
        }
    }

    public void testLosslessPromotions() {
        for (Policy policy : Policy.values()) {
            assertEquals(DataType.LONG, TypeWidening.join(DataType.INTEGER, DataType.LONG, policy));
            assertEquals(DataType.DOUBLE, TypeWidening.join(DataType.INTEGER, DataType.DOUBLE, policy));
            assertEquals(DataType.DATE_NANOS, TypeWidening.join(DataType.DATETIME, DataType.DATE_NANOS, policy));
        }
    }

    /**
     * The bug this lattice exists to make impossible: a numeric type and a temporal type have no
     * common supertype below keyword. Answering otherwise is what let a column of numbers be typed
     * as timestamps and read as instants in 1970.
     */
    public void testNumericAndTemporalHaveNoCommonSupertype() {
        for (DataType numeric : List.of(DataType.INTEGER, DataType.LONG, DataType.DOUBLE, DataType.UNSIGNED_LONG)) {
            for (DataType temporal : List.of(DataType.DATETIME, DataType.DATE_NANOS)) {
                for (Policy policy : Policy.values()) {
                    assertEquals(
                        numeric + " join " + temporal + " [" + policy + "]",
                        DataType.KEYWORD,
                        TypeWidening.join(numeric, temporal, policy)
                    );
                    assertEquals(
                        temporal + " join " + numeric + " [" + policy + "]",
                        DataType.KEYWORD,
                        TypeWidening.join(temporal, numeric, policy)
                    );
                }
            }
        }
    }

    public void testBooleanJoinsNothingButItself() {
        for (DataType t : UNIVERSE) {
            for (Policy policy : Policy.values()) {
                if (t == DataType.BOOLEAN) {
                    continue;
                }
                assertEquals(t + " [" + policy + "]", DataType.KEYWORD, TypeWidening.join(DataType.BOOLEAN, t, policy));
            }
        }
    }

    /**
     * The whole justification for having two policies is that they differ by one edge and no more. If
     * a future promotion is added to one and not the other, this fails and forces the question.
     */
    public void testPoliciesDifferOnExactlyOneEdge() {
        List<String> differing = new ArrayList<>();
        for (DataType a : UNIVERSE) {
            for (DataType b : UNIVERSE) {
                DataType inference = TypeWidening.join(a, b, Policy.INFERENCE);
                DataType reconciliation = TypeWidening.join(a, b, Policy.RECONCILIATION);
                if (inference != reconciliation) {
                    differing.add(a + "+" + b);
                }
            }
        }
        // Both orderings of the one pair.
        assertEquals("policies must differ on LONG+DOUBLE and nothing else, got " + differing, 2, differing.size());
        assertEquals(DataType.DOUBLE, TypeWidening.join(DataType.LONG, DataType.DOUBLE, Policy.INFERENCE));
        assertEquals(DataType.KEYWORD, TypeWidening.join(DataType.LONG, DataType.DOUBLE, Policy.RECONCILIATION));
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

    public void testWidenLosslessAgreesWithReconciliationJoinWhereverItAnswers() {
        for (DataType a : UNIVERSE) {
            for (DataType b : UNIVERSE) {
                DataType lossless = TypeWidening.widenLossless(a, b);
                if (lossless != null) {
                    assertEquals(a + "+" + b, lossless, TypeWidening.join(a, b, Policy.RECONCILIATION));
                }
            }
        }
    }
}
