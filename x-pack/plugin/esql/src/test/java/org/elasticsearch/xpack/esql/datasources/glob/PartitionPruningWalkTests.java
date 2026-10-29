/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.esql.datasources.glob.PartitionPruningWalk.LIST_PAGE_SIZE;
import static org.elasticsearch.xpack.esql.datasources.glob.PartitionPruningWalk.cheaperToFinish;

/**
 * Worked checks for the finish-vs-flat page-count rule. The first survivor is already listed, so remaining
 * cost is {@code (S - 1) * ceil(k / P) <= ceil(L * k / P)}.
 */
public class PartitionPruningWalkTests extends ESTestCase {

    public void testSevenOfOneHundredTwentyFourDailyLeavesFinishes() {
        assertTrue(cheaperToFinish(7, 576, 124));
    }

    public void testOneHundredSixtyEightHourlyLeavesFlatten() {
        assertFalse(cheaperToFinish(168, 5, 2880));
    }

    public void testThreeOfTenLeavesFlatten() {
        assertFalse(cheaperToFinish(3, 5, 10));
    }

    public void testFiveOfOneHundredIdsFinishWhenObjectsFillPages() {
        assertTrue("k=50: 4 remaining pages <= ceil(100*50/1000)", cheaperToFinish(5, 50, 100));
        assertTrue("k=1000: 4 pages vs 100 prefix pages", cheaperToFinish(5, 1000, 100));
    }

    public void testFiveOfOneHundredIdsFlattenWhenEachDirIsOneObject() {
        assertFalse("k=1: 4 remaining pages > ceil(100/1000)=1 prefix page", cheaperToFinish(5, 1, 100));
    }

    public void testSingleSurvivorAlwaysFinishes() {
        assertTrue(cheaperToFinish(1, 1, 10));
        assertTrue(cheaperToFinish(1, 576, 124));
        assertTrue(cheaperToFinish(1, LIST_PAGE_SIZE + 1, 2));
        assertTrue("probe already paid; remaining cost is 0", cheaperToFinish(1, LIST_PAGE_SIZE + 1, 0.5));
    }

    public void testEmptySurvivorsFinish() {
        assertTrue(cheaperToFinish(0, 100, 10));
    }

    public void testPageSizeBoundary() {
        assertEquals(1, PartitionPruningWalk.ceilDiv(1, LIST_PAGE_SIZE));
        assertEquals(1, PartitionPruningWalk.ceilDiv(LIST_PAGE_SIZE, LIST_PAGE_SIZE));
        assertEquals(2, PartitionPruningWalk.ceilDiv(LIST_PAGE_SIZE + 1, LIST_PAGE_SIZE));
        assertEquals(0, PartitionPruningWalk.ceilDiv(0, LIST_PAGE_SIZE));
    }

    public void testEqualCostFinishes() {
        // 7 of 62 days, k=100, P=1000: 6 remaining == ceil(6.2) == 7? 6 <= 7
        assertTrue(cheaperToFinish(7, 100, 62));
    }

    public void testJustOverEqualFlattens() {
        // 8 remaining pages vs ceil(62*100/1000)=7
        assertFalse(cheaperToFinish(9, 100, 62));
    }

    public void testProbeAlreadyPaidFinishesWhenExAnteWouldFlatten() {
        // S=3, k=1001, L=4.5: ex-ante 6 vs 5 flattens; remaining 4 vs 5 finishes (totals 6 vs 7).
        assertTrue(cheaperToFinish(3, LIST_PAGE_SIZE + 1, 4.5));
    }

    public void testTwoSparseSurvivorsFinishAtEqualRemainingCost() {
        assertTrue("1 remaining page == ceil(100/1000)=1", cheaperToFinish(2, 1, 100));
    }

    public void testMultiSurvivorPagesWhenObjectsExceedPageSize() {
        assertTrue("remaining 4 <= ceil(10*1001/1000)=11", cheaperToFinish(3, LIST_PAGE_SIZE + 1, 10));
        assertFalse("remaining 4 > ceil(2*1001/1000)=3", cheaperToFinish(3, LIST_PAGE_SIZE + 1, 2));
    }

    public void testEmptyFirstListingStillCostsOnePage() {
        assertTrue(cheaperToFinish(1, 0, 10));
        assertFalse("S=5 empty dirs vs 1 prefix page", cheaperToFinish(5, 0, 100));
    }

    public void testFractionalPrefixWidth() {
        assertFalse("S=2, remaining two pages vs ceil(0.5*1001/1000)=1", cheaperToFinish(2, LIST_PAGE_SIZE + 1, 0.5));
        assertTrue(cheaperToFinish(1, 1, 1.5));
    }
}
