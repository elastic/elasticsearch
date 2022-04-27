/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.test.ESTestCase;

public class SearchUtilsTests extends ESTestCase {

    public void testConfigureMaxClauses() {

        // Heap below 1 Gb
        assertEquals(8192, SearchUtils.calculateMaxClauseValue(4, 0.5));

        // Number of processors not available
        assertEquals(1024, SearchUtils.calculateMaxClauseValue(-1, 1));

        // Insanely high configured search thread pool size
        assertEquals(1024, SearchUtils.calculateMaxClauseValue(1024, 1));

        // 1Gb heap, 8 processors
        assertEquals(5041, SearchUtils.calculateMaxClauseValue(13, 1));

        // 30Gb heap, 48 processors
        assertEquals(26932, SearchUtils.calculateMaxClauseValue(73, 30));
    }

}
