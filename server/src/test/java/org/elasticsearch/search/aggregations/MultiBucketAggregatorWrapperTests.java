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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class MultiBucketAggregatorWrapperTests extends ESTestCase {

    public void testNoNullScorerIsDelegated() throws Exception {
        LeafReaderContext leafReaderContext = MemoryIndex.fromDocument(Collections.emptyList(), new MockAnalyzer(random()))
                .createSearcher().getIndexReader().leaves().get(0);
        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(queryShardContext.bigArrays()).thenReturn(bigArrays);
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.bigArrays()).thenReturn(bigArrays);

        Aggregator aggregator = mock(Aggregator.class);
        AggregatorFactory aggregatorFactory = new TestAggregatorFactory(queryShardContext, aggregator);
        LeafBucketCollector wrappedCollector = mock(LeafBucketCollector.class);
        when(aggregator.getLeafCollector(leafReaderContext)).thenReturn(wrappedCollector);
        Aggregator wrapper = AggregatorFactory.asMultiBucketAggregator(aggregatorFactory, searchContext, null);

        LeafBucketCollector collector = wrapper.getLeafCollector(leafReaderContext);

        collector.collect(0, 0);
        // setScorer should not be invoked as it has not been set
        // Only collect should be invoked:
        verify(wrappedCollector).collect(0, 0);
        verifyNoMoreInteractions(wrappedCollector);

        reset(wrappedCollector);
        Scorer scorer = mock(Scorer.class);
        collector.setScorer(scorer);
        collector.collect(0, 1);
        verify(wrappedCollector).setScorer(same(scorer));
        verify(wrappedCollector).collect(0, 0);
        verifyNoMoreInteractions(wrappedCollector);
        wrapper.close();
    }
}
