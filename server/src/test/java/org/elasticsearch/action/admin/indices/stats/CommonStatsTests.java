/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.test.ESTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CommonStatsTests extends ESTestCase {

    public void testQueryCacheStatsCalculatorIsLazilyInitialized() {
        var mockedIndicesQueryCache = mock(IndicesQueryCache.class);
        var queryCacheStatsMemoized = new CommonStats.QueryCacheStatsMemoized(mockedIndicesQueryCache);

        for (int i = 0; i < randomIntBetween(2, 1000); i++) {
            queryCacheStatsMemoized.get(new ShardId(randomAlphaOfLength(5), randomAlphaOfLength(5), randomIntBetween(1, 100)));
        }

        verify(mockedIndicesQueryCache, times(1)).getGeneralStats();
    }
}
