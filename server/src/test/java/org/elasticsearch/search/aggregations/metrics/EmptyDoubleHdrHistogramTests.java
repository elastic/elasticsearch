/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.HdrHistogram.DoubleHistogram;
import org.elasticsearch.test.ESTestCase;

public class EmptyDoubleHdrHistogramTests extends ESTestCase {

    private static final DoubleHistogram singleton = new EmptyDoubleHdrHistogram();

    public void testSetAutoResize() {
        assertFalse(singleton.isAutoResize());
        singleton.setAutoResize(true);
        assertFalse(singleton.isAutoResize());
        singleton.setAutoResize(false);
        assertFalse(singleton.isAutoResize());
    }

    public void testRecordValue() {
        expectThrows(UnsupportedOperationException.class, () -> singleton.recordValue(1.0D));
    }

    public void testRecordValueWithCount() {
        expectThrows(UnsupportedOperationException.class, () -> singleton.recordValueWithCount(1.0D, 1));
    }

    public void testRecordValueWithExpectedInterval() {
        expectThrows(UnsupportedOperationException.class, () -> singleton.recordValueWithExpectedInterval(1.0D, 1.0D));
    }

    public void testReset() {
        expectThrows(UnsupportedOperationException.class, singleton::reset);
    }

    public void testCopy() {
        expectThrows(UnsupportedOperationException.class, singleton::copy);
    }

    public void testCopyCorrectedForCoordinatedOmission() {
        expectThrows(UnsupportedOperationException.class, () -> singleton.copyCorrectedForCoordinatedOmission(1.0D));
    }

    public void testCopyInto() {
        expectThrows(UnsupportedOperationException.class, () -> singleton.copyInto(new DoubleHistogram(1)));
    }

    public void testCopyIntoCorrectedForCoordinatedOmission() {
        expectThrows(
            UnsupportedOperationException.class,
            () -> singleton.copyIntoCorrectedForCoordinatedOmission(new DoubleHistogram(1), 1.0D)
        );
    }

    public void testAdd() {
        expectThrows(UnsupportedOperationException.class, () -> singleton.add(new DoubleHistogram(1)));
    }

    public void testAddWhileCorrectingForCoordinatedOmission() {
        expectThrows(
            UnsupportedOperationException.class,
            () -> singleton.addWhileCorrectingForCoordinatedOmission(new DoubleHistogram(1), 1.0D)
        );
    }

    public void testSubtract() {
        expectThrows(UnsupportedOperationException.class, () -> singleton.subtract(new DoubleHistogram(1)));
    }

    public void testLowestEquivalentValue() {
        expectThrows(UnsupportedOperationException.class, () -> singleton.lowestEquivalentValue(1.0D));
    }

    public void testHighestEquivalentValue() {
        expectThrows(UnsupportedOperationException.class, () -> singleton.highestEquivalentValue(1.0D));
    }

    public void testSizeOfEquivalentValueRange() {
        expectThrows(UnsupportedOperationException.class, () -> singleton.sizeOfEquivalentValueRange(1.0D));
    }

    public void testMedianEquivalentValue() {
        expectThrows(UnsupportedOperationException.class, () -> singleton.medianEquivalentValue(1.0D));
    }

    public void testNextNonEquivalentValue() {
        expectThrows(UnsupportedOperationException.class, () -> singleton.nextNonEquivalentValue(1.0D));
    }

    public void testValuesAreEquivalent() {
        expectThrows(UnsupportedOperationException.class, () -> singleton.valuesAreEquivalent(1.0D, 1.0D));
    }

    public void testSetStartTimeStamp() {
        expectThrows(UnsupportedOperationException.class, () -> singleton.setStartTimeStamp(1L));
    }

    public void testSetEndTimeStamp() {
        expectThrows(UnsupportedOperationException.class, () -> singleton.setEndTimeStamp(1L));
    }

    public void testSetTag() {
        expectThrows(UnsupportedOperationException.class, () -> singleton.setTag("foo_bar"));
    }

    public void testLinearBucketValues() {
        expectThrows(UnsupportedOperationException.class, () -> singleton.linearBucketValues(1.0D));
    }

    public void testLogarithmicBucketValues() {
        expectThrows(UnsupportedOperationException.class, () -> singleton.logarithmicBucketValues(1.0D, 1.0D));
    }
}
