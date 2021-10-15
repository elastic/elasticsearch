/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TermsAggregatorFactoryTests extends ESTestCase {
    public void testPickEmpty() throws Exception {
        AggregatorFactories empty = mock(AggregatorFactories.class);
        when(empty.countAggregators()).thenReturn(0);
        assertThat(
            TermsAggregatorFactory.pickSubAggColectMode(empty, randomInt(), randomInt()),
            equalTo(Aggregator.SubAggCollectionMode.DEPTH_FIRST)
        );
    }

    public void testPickNonEempty() {
        AggregatorFactories nonEmpty = mock(AggregatorFactories.class);
        when(nonEmpty.countAggregators()).thenReturn(1);
        assertThat(
            TermsAggregatorFactory.pickSubAggColectMode(nonEmpty, Integer.MAX_VALUE, -1),
            equalTo(Aggregator.SubAggCollectionMode.DEPTH_FIRST)
        );
        assertThat(TermsAggregatorFactory.pickSubAggColectMode(nonEmpty, 10, -1), equalTo(Aggregator.SubAggCollectionMode.BREADTH_FIRST));
        assertThat(TermsAggregatorFactory.pickSubAggColectMode(nonEmpty, 10, 5), equalTo(Aggregator.SubAggCollectionMode.DEPTH_FIRST));
        assertThat(TermsAggregatorFactory.pickSubAggColectMode(nonEmpty, 10, 10), equalTo(Aggregator.SubAggCollectionMode.DEPTH_FIRST));
        assertThat(TermsAggregatorFactory.pickSubAggColectMode(nonEmpty, 10, 100), equalTo(Aggregator.SubAggCollectionMode.BREADTH_FIRST));
        assertThat(TermsAggregatorFactory.pickSubAggColectMode(nonEmpty, 1, 2), equalTo(Aggregator.SubAggCollectionMode.BREADTH_FIRST));
        assertThat(TermsAggregatorFactory.pickSubAggColectMode(nonEmpty, 1, 100), equalTo(Aggregator.SubAggCollectionMode.BREADTH_FIRST));
    }
}
