/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test implementation for AggregatorFactory.
 */
public class TestAggregatorFactory extends AggregatorFactory {

    private final Aggregator aggregator;

    TestAggregatorFactory(QueryShardContext queryShardContext, Aggregator aggregator) throws IOException {
        super("_name", queryShardContext, null, new AggregatorFactories.Builder(), Collections.emptyMap());
        this.aggregator = aggregator;
    }

    @Override
    protected Aggregator createInternal(SearchContext searchContext, Aggregator parent, boolean collectsFromSingleBucket,
                                        Map metadata) throws IOException {
        return aggregator;
    }

    public static TestAggregatorFactory createInstance() throws IOException {
        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(queryShardContext.bigArrays()).thenReturn(bigArrays);

        Aggregator aggregator = mock(Aggregator.class);

        return new TestAggregatorFactory(queryShardContext, aggregator);
    }
}
