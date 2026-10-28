/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.plain.SortedDoublesIndexFieldData;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.field.DoubleDocValuesField;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;

/**
 * Checks that rejecting an invalid missing value does not leave the bucketed comparator's arrays charged to the breaker.
 */
public class DoubleValuesComparatorSourceTests extends ESTestCase {

    /**
     * Building a Lucene sort defers missing-value validation. The bucketed sort must reject the same value without leaking
     * arrays allocated by its superclass before the missing-value initializer runs.
     */
    public void testInvalidMissingValueReleasesBucketedSortArrays() {
        CircuitBreakerService breakers = newLimitedBreakerService(ByteSizeValue.ofMb(1));
        CircuitBreaker breaker = breakers.getBreaker(CircuitBreaker.REQUEST);
        // No recycler or aggregation preallocation, so the counter directly measures outstanding array allocations.
        BigArrays bigArrays = new BigArrays(null, breakers, CircuitBreaker.REQUEST).withCircuitBreaking();
        SortedDoublesIndexFieldData fieldData = new SortedDoublesIndexFieldData(
            "price",
            NumericType.DOUBLE,
            CoreValuesSourceType.NUMERIC,
            DoubleDocValuesField::new,
            IndexType.points(false, true)
        );

        // Successfully building the Lucene sort does not establish that the missing value can be parsed.
        assertNotNull(fieldData.sortField(false, IndexVersion.current(), "bogus", MultiValueMode.MIN, null, false));
        assertEquals(0L, breaker.getUsed());

        expectThrows(
            NumberFormatException.class,
            () -> fieldData.newBucketedSort(
                bigArrays,
                "bogus",
                MultiValueMode.MIN,
                null,
                SortOrder.ASC,
                DocValueFormat.RAW,
                3,
                BucketedSort.NOOP_EXTRA_DATA
            )
        );

        assertEquals("failed bucketed sort construction must release its arrays", 0L, breaker.getUsed());
    }
}
