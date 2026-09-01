/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.cluster.metadata.IndexMetadataStats;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class RamUsageEstimatesTests extends ESTestCase {

    public void testObjectHasOnlyPrimitiveFields() {
        assertTrue(RamUsageEstimates.objectHasOnlyPrimitiveFields(IndexMetadataStats.AverageShardSize.class));
        assertFalse(RamUsageEstimates.objectHasOnlyPrimitiveFields(ByteSizeValue.class));
    }

    public void testObjectHasOnlyShallowCompleteFields() {
        assertTrue(RamUsageEstimates.objectHasOnlyShallowCompleteFields(IndexMetadataStats.AverageShardSize.class));
        assertTrue(RamUsageEstimates.objectHasOnlyShallowCompleteFields(ByteSizeValue.class));
        assertTrue(RamUsageEstimates.objectHasOnlyShallowCompleteFields(TimeValue.class));
        assertFalse(RamUsageEstimates.objectHasOnlyShallowCompleteFields(String.class));
    }

    public void testShallowSizeOfPrimitiveOnlyMatchesRamUsageEstimator() {
        var averageShardSize = new IndexMetadataStats.AverageShardSize(1024L, 4);
        assertThat(
            RamUsageEstimates.shallowSizeOfPrimitiveOnly(averageShardSize),
            equalTo(RamUsageEstimator.shallowSizeOf(averageShardSize))
        );
    }

    public void testShallowSizeOfShallowCompleteMatchesRamUsageEstimator() {
        ByteSizeValue byteSizeValue = ByteSizeValue.ofMb(1);
        TimeValue timeValue = new TimeValue(1, TimeUnit.DAYS);
        assertThat(RamUsageEstimates.shallowSizeOfShallowComplete(byteSizeValue), equalTo(RamUsageEstimator.shallowSizeOf(byteSizeValue)));
        assertThat(RamUsageEstimates.shallowSizeOfShallowComplete(timeValue), equalTo(RamUsageEstimator.shallowSizeOf(timeValue)));
    }

    public void testSizeOfShallowCompleteValue() {
        assertThat(RamUsageEstimates.sizeOfShallowCompleteValue(null), is(0L));
        assertThat(RamUsageEstimates.sizeOfShallowCompleteValue(1L), equalTo(RamUsageEstimator.sizeOf(1L)));
        assertThat(
            RamUsageEstimates.sizeOfShallowCompleteValue(TimeValue.timeValueDays(1)),
            equalTo(RamUsageEstimator.shallowSizeOf(TimeValue.timeValueDays(1)))
        );
        assertThat(
            RamUsageEstimates.sizeOfShallowCompleteValue("unknown-condition-value"),
            equalTo(RamUsageEstimator.sizeOf("unknown-condition-value"))
        );
    }

    public void testReferenceFieldNamesDeclaredOn() {
        assertThat(
            RamUsageEstimates.referenceFieldNamesDeclaredOn(IndexMetadataStats.class),
            equalTo(java.util.Set.of("indexWriteLoad", "averageShardSize"))
        );
    }

    public void testShallowSizeOfShallowCompleteIsAtLeastReferenceOverhead() {
        long shallow = RamUsageEstimates.shallowSizeOfShallowComplete(ByteSizeValue.ofMb(1));
        assertThat(shallow, greaterThan(0L));
    }
}
