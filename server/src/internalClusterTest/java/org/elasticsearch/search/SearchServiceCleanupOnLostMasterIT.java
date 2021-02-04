/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;


import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESIntegTestCase;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, scope= ESIntegTestCase.Scope.TEST)
public class SearchServiceCleanupOnLostMasterIT extends ESIntegTestCase {

    public void testMasterRestart() throws Exception {
        String master = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        index("test", "test", "{}");

        SearchResponse searchResponse = client().prepareSearch("test").setScroll("1m").get();
        assertThat(searchResponse.getScrollId(), is(notNullValue()));

        internalCluster().restartNode(master);
        // in the past, this failed because the search context for the scroll would prevent the shard lock from being released.
        ensureYellow();
    }
}
