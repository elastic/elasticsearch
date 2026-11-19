/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.List;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.esql.into.IntoTestUtils.stats;
import static org.hamcrest.Matchers.equalTo;

public class IntoStatsTests extends ComputeTestCase {
    public void testRoundTripThroughAPage() {
        IntoStats original = new IntoStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            details("reason", between(0, IntoStats.MAX_FAILURE_DETAILS))
        );
        assertThat(stats(original.toPage(blockFactory())), equalTo(original));
    }

    public void testMergeKeepsAtMostMaxFailureDetails() {
        List<String> full = details("a", IntoStats.MAX_FAILURE_DETAILS);
        IntoStats merged = new IntoStats(1, 0, 0, 0, 1, full).merge(new IntoStats(1, 0, 0, 0, 1, details("b", 1)));
        assertThat(merged.failureDetails(), equalTo(full));
        assertThat(merged.failures(), equalTo(2L));
    }

    private static List<String> details(String prefix, int count) {
        return IntStream.range(0, count).mapToObj(i -> prefix + "-" + i).toList();
    }
}
