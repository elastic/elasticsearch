/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

public class SearchUtilsTests extends ESTestCase {

    private static final int maxClauseCount = IndexSearcher.getMaxClauseCount();

    @After
    public void tearDown() throws Exception {
        IndexSearcher.setMaxClauseCount(maxClauseCount);
        super.tearDown();
    }

    public void testConfigureMaxClauses() {

        SearchUtils.configureMaxClauses(13, 1);
        assertEquals(5041, IndexSearcher.getMaxClauseCount());

        SearchUtils.configureMaxClauses(73, 30);
        assertEquals(26932, IndexSearcher.getMaxClauseCount());
    }

}
