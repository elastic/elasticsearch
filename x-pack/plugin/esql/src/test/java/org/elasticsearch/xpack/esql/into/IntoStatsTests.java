/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.esql.into.IntoTestUtils.stats;
import static org.hamcrest.Matchers.equalTo;

public class IntoStatsTests extends ComputeTestCase {
    public void testRoundTripThroughAPage() {
        IntoStats original = IntoStats.builder()
            .total(randomNonNegativeLong())
            .created(randomNonNegativeLong())
            .updated(randomNonNegativeLong())
            .versionConflicts(randomNonNegativeLong())
            .failures(randomNonNegativeLong())
            .failureDetails(details("reason", between(0, IntoStats.MAX_FAILURE_DETAILS)))
            .build();
        assertThat(stats(original.toPage(blockFactory())), equalTo(original));
    }

    public void testMergeKeepsAtMostMaxFailureDetails() {
        List<String> full = details("a", IntoStats.MAX_FAILURE_DETAILS);
        IntoStats merged = IntoStats.builder()
            .total(1)
            .failures(1)
            .failureDetails(full)
            .build()
            .merge(IntoStats.builder().total(1).failures(1).failureDetails(details("b", 1)).build());
        assertThat(merged.failureDetails(), equalTo(full));
        assertThat(merged.failures(), equalTo(2L));
    }

    public void testMergeAddsEveryCounter() {
        IntoStats first = IntoStats.builder().total(1).created(2).updated(3).versionConflicts(4).failures(5).build();
        IntoStats second = IntoStats.builder().total(10).created(20).updated(30).versionConflicts(40).failures(50).build();
        IntoStats sum = IntoStats.builder().total(11).created(22).updated(33).versionConflicts(44).failures(55).build();
        assertThat(first.merge(second), equalTo(sum));
        assertThat(IntoStats.EMPTY.merge(first), equalTo(first));
    }

    public void testAttributesMatchThePageLayout() {
        List<Attribute> attributes = IntoStats.createAttributes(Source.EMPTY);
        assertThat(
            Expressions.names(attributes),
            equalTo(List.of("total", "created", "updated", "version_conflicts", "failures", "failure_details"))
        );
        assertThat(
            attributes.stream().map(Attribute::dataType).toList(),
            equalTo(List.of(DataType.LONG, DataType.LONG, DataType.LONG, DataType.LONG, DataType.LONG, DataType.KEYWORD))
        );
        Page page = IntoStats.EMPTY.toPage(blockFactory());
        try {
            assertThat(page.getBlockCount(), equalTo(attributes.size()));
        } finally {
            page.releaseBlocks();
        }
    }

    private static List<String> details(String prefix, int count) {
        return IntStream.range(0, count).mapToObj(i -> prefix + "-" + i).toList();
    }
}
