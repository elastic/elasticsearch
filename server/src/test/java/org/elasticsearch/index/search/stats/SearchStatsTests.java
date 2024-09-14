/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.search.stats;

import org.elasticsearch.index.search.stats.SearchStats.Stats;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

public class SearchStatsTests extends ESTestCase {

    // https://github.com/elastic/elasticsearch/issues/7644
    public void testShardLevelSearchGroupStats() throws Exception {
        // let's create two dummy search stats with groups
        Map<String, Stats> groupStats1 = new HashMap<>();
        Map<String, Stats> groupStats2 = new HashMap<>();
        groupStats2.put("group1", new Stats(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1));
        SearchStats searchStats1 = new SearchStats(new Stats(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), 0, groupStats1);
        SearchStats searchStats2 = new SearchStats(new Stats(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), 0, groupStats2);

        // adding these two search stats and checking group stats are correct
        searchStats1.add(searchStats2);
        assertStats(groupStats1.get("group1"), 1);

        // another call, adding again ...
        searchStats1.add(searchStats2);
        assertStats(groupStats1.get("group1"), 2);

        // making sure stats2 was not affected (this would previously return 2!)
        assertStats(groupStats2.get("group1"), 1);

        // adding again would then return wrong search stats (would return 4! instead of 3)
        searchStats1.add(searchStats2);
        assertStats(groupStats1.get("group1"), 3);
    }

    private static void assertStats(Stats stats, long equalTo) {
        assertEquals(equalTo, stats.getQueryCount());
        assertEquals(equalTo, stats.getQueryTimeInMillis());
        assertEquals(equalTo, stats.getQueryCurrent());
        assertEquals(equalTo, stats.getFetchCount());
        assertEquals(equalTo, stats.getFetchTimeInMillis());
        assertEquals(equalTo, stats.getFetchCurrent());
        assertEquals(equalTo, stats.getScrollCount());
        assertEquals(equalTo, stats.getScrollTimeInMillis());
        assertEquals(equalTo, stats.getScrollCurrent());
        assertEquals(equalTo, stats.getSuggestCount());
        assertEquals(equalTo, stats.getSuggestTimeInMillis());
        assertEquals(equalTo, stats.getSuggestCurrent());
    }

}
