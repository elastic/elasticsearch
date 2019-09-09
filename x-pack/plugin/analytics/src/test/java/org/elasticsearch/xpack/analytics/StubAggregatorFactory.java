/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test implementation for AggregatorFactory.
 */
public class StubAggregatorFactory extends AggregatorFactory {

    private final Aggregator aggregator;

    private StubAggregatorFactory(SearchContext context, Aggregator aggregator) throws IOException {
        super("_name", context, null, new AggregatorFactories.Builder(), Collections.emptyMap());
        this.aggregator = aggregator;
    }

    @Override
    protected Aggregator createInternal(Aggregator parent, boolean collectsFromSingleBucket, List list, Map metaData) throws IOException {
        return aggregator;
    }

    public static StubAggregatorFactory createInstance() throws IOException {
        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.bigArrays()).thenReturn(bigArrays);

        Aggregator aggregator = mock(Aggregator.class);

        return new StubAggregatorFactory(searchContext, aggregator);
    }
}
