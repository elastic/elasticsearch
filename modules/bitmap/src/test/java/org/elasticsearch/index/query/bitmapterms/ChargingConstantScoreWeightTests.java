/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.util.DocIdSetBuilder;
import org.elasticsearch.lucene.search.cost.DocIdSetBuilderCostEstimator;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongConsumer;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ChargingConstantScoreWeightTests extends ESTestCase {

    /** After every grow, what was charged covers the bound for the total so far, and never goes past a full set's. */
    public void testChargeCoversTheBoundAfterEveryGrow() {
        int maxDoc = between(1, 1 << 20);
        long dense = denseBytes(maxDoc);
        List<Long> charges = new ArrayList<>();
        LongConsumer charger = new ChargingConstantScoreWeight.BuilderCharger(charges::add, maxDoc);
        long grown = 0;
        for (int i = between(1, 1000); i > 0; i--) {
            int numDocs = between(1, Math.max(1, maxDoc >>> between(4, 12)));
            charger.accept(numDocs);
            grown += numDocs;
            long charged = sum(charges);
            assertThat(charged, greaterThanOrEqualTo(new DocIdSetBuilderCostEstimator(grown, maxDoc).estimate()));
            assertThat(charged, lessThanOrEqualTo(dense));
        }
        assertThat(charges, everyItem(greaterThan(0L)));
    }

    /**
     * Growing one document at a time until the set is dense, the charge at least doubles each time the breaker is consulted, so it is
     * consulted a logarithmic number of times rather than once per grow.
     */
    public void testChargesInDoublingSteps() {
        int maxDoc = between(1 << 12, 1 << 24);
        long dense = denseBytes(maxDoc);
        List<Long> charges = new ArrayList<>();
        LongConsumer charger = new ChargingConstantScoreWeight.BuilderCharger(charges::add, maxDoc);
        // The builder upgrades to a bit set once it is grown past maxDoc >>> 7.
        for (int grown = 0; grown <= maxDoc >>> 7; grown++) {
            charger.accept(1);
        }
        assertThat(sum(charges), equalTo(dense));
        int doublings = 63 - Long.numberOfLeadingZeros(dense / charges.getFirst());
        assertThat(charges.size(), lessThanOrEqualTo(doublings + 2));
    }

    public void testChargesNothingMoreOnceDense() {
        int maxDoc = between(1, 1 << 20);
        List<Long> charges = new ArrayList<>();
        LongConsumer charger = new ChargingConstantScoreWeight.BuilderCharger(charges::add, maxDoc);
        charger.accept(maxDoc);
        assertThat(charges, equalTo(List.of(denseBytes(maxDoc))));
        charger.accept(between(1, maxDoc));
        assertThat(charges, equalTo(List.of(denseBytes(maxDoc))));
    }

    public void testEmptyLeafChargesNothing() {
        List<Long> charges = new ArrayList<>();
        new ChargingConstantScoreWeight.BuilderCharger(charges::add, 0).accept(between(1, 100));
        assertThat(charges, empty());
    }

    /** What a real builder holds after each grow fits in what was charged for it. */
    public void testChargeCoversARealBuilder() {
        int maxDoc = between(1, 1 << 16);
        DocIdSetBuilder builder = new DocIdSetBuilder(maxDoc);
        List<Long> charges = new ArrayList<>();
        LongConsumer charger = new ChargingConstantScoreWeight.BuilderCharger(charges::add, maxDoc);
        for (int i = between(1, 200); i > 0; i--) {
            int numDocs = between(1, Math.max(1, maxDoc >>> between(3, 10)));
            charger.accept(numDocs);
            DocIdSetBuilder.BulkAdder adder = builder.grow(numDocs);
            for (int doc = 0; doc < numDocs; doc++) {
                adder.add(randomInt(maxDoc - 1));
            }
            assertThat(RamUsageTester.ramUsed(builder), lessThanOrEqualTo(sum(charges)));
        }
    }

    private static long denseBytes(int maxDoc) {
        return new DocIdSetBuilderCostEstimator(maxDoc, maxDoc).estimate();
    }

    private static long sum(List<Long> charges) {
        return charges.stream().mapToLong(Long::longValue).sum();
    }
}
