/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class ChangePointBucketFillUtilsTests extends ESTestCase {

    public void testInteriorGapFill() {
        Rounding.Prepared rounding = Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY).build().prepareForUnknown();
        long hour0 = rounding.round(0L);
        long hour2 = rounding.nextRoundingValue(rounding.nextRoundingValue(hour0));

        TreeSet<Long> existing = new TreeSet<>(List.of(hour0, hour2));
        List<Long> filled = ChangePointBucketFillUtils.computeFilledKeys(existing, rounding, hour0, hour2 + 1);

        assertThat(filled.size(), equalTo(3));
        assertThat(filled.get(1), equalTo(rounding.nextRoundingValue(hour0)));
    }

    public void testEdgeExtensionWhenSingleBucketAtStart() {
        Rounding.Prepared rounding = Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY).build().prepareForUnknown();
        long start = rounding.round(0L);
        long end = start;
        for (int i = 0; i < ChangePointBucketFillUtils.MINIMUM_BUCKET_COUNT; i++) {
            end = rounding.nextRoundingValue(end);
        }

        TreeSet<Long> existing = new TreeSet<>(List.of(start));
        List<Long> filled = ChangePointBucketFillUtils.computeFilledKeys(existing, rounding, start, end);

        assertThat(filled.size(), equalTo(ChangePointBucketFillUtils.MINIMUM_BUCKET_COUNT));
        assertThat(filled.getFirst(), equalTo(start));
        long expectedLast = start;
        for (int i = 0; i < ChangePointBucketFillUtils.MINIMUM_BUCKET_COUNT - 1; i++) {
            expectedLast = rounding.nextRoundingValue(expectedLast);
        }
        assertThat(filled.getLast(), equalTo(expectedLast));
    }

    public void testEdgeExtensionWhenSingleBucketAtEnd() {
        Rounding.Prepared rounding = Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY).build().prepareForUnknown();
        long start = rounding.round(0L);
        long end = start;
        for (int i = 0; i < 40; i++) {
            end = rounding.nextRoundingValue(end);
        }
        long lastBucket = previousRoundingValue(rounding, end, start);

        TreeSet<Long> existing = new TreeSet<>(List.of(lastBucket));
        List<Long> filled = ChangePointBucketFillUtils.computeFilledKeys(existing, rounding, start, end);

        assertThat(filled.size(), equalTo(ChangePointBucketFillUtils.MINIMUM_BUCKET_COUNT));
        assertThat(filled, hasItem(lastBucket));
        assertThat(filled.getLast(), equalTo(lastBucket));
        assertThat(filled, not(hasItem(start)));
        long expectedFirst = lastBucket;
        for (int i = 0; i < ChangePointBucketFillUtils.MINIMUM_BUCKET_COUNT - 1; i++) {
            expectedFirst = previousRoundingValue(rounding, expectedFirst, start);
        }
        assertThat(filled.getFirst(), equalTo(expectedFirst));
    }

    public void testExtendOnlyOnOtherSideWhenOneEdgeReached() {
        Rounding.Prepared rounding = Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY).build().prepareForUnknown();
        long start = rounding.round(0L);
        long end = start;
        for (int i = 0; i < 10; i++) {
            end = rounding.nextRoundingValue(end);
        }
        long lastBucket = previousRoundingValue(rounding, end, start);

        TreeSet<Long> existing = new TreeSet<>(List.of(lastBucket));
        List<Long> filled = ChangePointBucketFillUtils.computeFilledKeys(existing, rounding, start, end);

        assertThat(filled.size(), equalTo(10));
        assertThat(filled.getLast(), equalTo(lastBucket));
        for (int i = 1; i < filled.size(); i++) {
            assertThat(filled.get(i), equalTo(rounding.nextRoundingValue(filled.get(i - 1))));
        }
    }

    public void testInteriorFillAllGapsWhenSparseEndpoints() {
        Rounding.Prepared rounding = Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY).build().prepareForUnknown();
        long start = rounding.round(0L);
        long end = start;
        for (int i = 0; i < 40; i++) {
            end = rounding.nextRoundingValue(end);
        }
        long lastBucket = previousRoundingValue(rounding, end, start);

        TreeSet<Long> existing = new TreeSet<>(List.of(start, lastBucket));
        List<Long> filled = ChangePointBucketFillUtils.computeFilledKeys(existing, rounding, start, end);

        assertThat(filled.size(), equalTo(40));
        assertThat(filled.getFirst(), equalTo(start));
        assertThat(filled.getLast(), equalTo(lastBucket));
    }

    public void testNoEdgeExtensionWhenAtLeastMinimumRealBuckets() {
        Rounding.Prepared rounding = Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY).build().prepareForUnknown();
        long start = rounding.round(0L);
        TreeSet<Long> existing = new TreeSet<>();
        long current = start;
        for (int i = 0; i < ChangePointBucketFillUtils.MINIMUM_BUCKET_COUNT; i++) {
            existing.add(current);
            current = rounding.nextRoundingValue(current);
        }
        long end = current;

        assertThat(ChangePointBucketFillUtils.needsFill(existing, rounding, start, end), equalTo(false));
        assertThat(ChangePointBucketFillUtils.computeFilledKeys(existing, rounding, start, end), equalTo(new ArrayList<>(existing)));
    }

    public void testInteriorFillWhenSparseRealBucketsAtOrAboveMinimum() {
        Rounding.Prepared rounding = Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY).build().prepareForUnknown();
        long start = rounding.round(0L);
        TreeSet<Long> existing = new TreeSet<>();
        long current = start;
        for (int i = 0; i < ChangePointBucketFillUtils.MINIMUM_BUCKET_COUNT; i++) {
            existing.add(current);
            current = rounding.nextRoundingValue(current);
            current = rounding.nextRoundingValue(current);
        }
        long end = current;
        for (int i = 0; i < 5; i++) {
            end = rounding.nextRoundingValue(end);
        }

        List<Long> filled = ChangePointBucketFillUtils.computeFilledKeys(existing, rounding, start, end);

        assertThat(ChangePointBucketFillUtils.needsFill(existing, rounding, start, end), equalTo(true));
        assertThat(filled.size(), equalTo(ChangePointBucketFillUtils.MINIMUM_BUCKET_COUNT * 2 - 1));
        for (int i = 1; i < filled.size(); i++) {
            assertThat(filled.get(i), equalTo(rounding.nextRoundingValue(filled.get(i - 1))));
        }
    }

    public void testNoFillWhenDenseAndAboveMinimum() {
        Rounding.Prepared rounding = Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY).build().prepareForUnknown();
        long start = rounding.round(0L);
        TreeSet<Long> existing = new TreeSet<>();
        long current = start;
        for (int i = 0; i < 25; i++) {
            existing.add(current);
            current = rounding.nextRoundingValue(current);
        }
        long end = current;

        assertThat(ChangePointBucketFillUtils.needsFill(existing, rounding, start, end), equalTo(false));
    }

    public void testCountBucketsInRange() {
        Rounding.Prepared rounding = Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY).build().prepareForUnknown();
        long start = rounding.round(0L);
        long end = start;
        for (int i = 0; i < 22; i++) {
            end = rounding.nextRoundingValue(end);
        }
        assertThat(ChangePointBucketFillUtils.countBucketsInRange(rounding, start, end), equalTo(22));
    }

    public void testEmptyExistingReturnsEmpty() {
        Rounding.Prepared rounding = Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY).build().prepareForUnknown();
        long start = rounding.round(0L);
        long end = start;
        for (int i = 0; i < 22; i++) {
            end = rounding.nextRoundingValue(end);
        }
        assertThat(ChangePointBucketFillUtils.computeFilledKeys(new TreeSet<>(), rounding, start, end).size(), equalTo(0));
    }

    private static long previousRoundingValue(Rounding.Prepared rounding, long key, long minDate) {
        long t = rounding.round(minDate);
        long previous = t;
        while (t < key) {
            previous = t;
            t = rounding.nextRoundingValue(t);
        }
        return previous;
    }
}
