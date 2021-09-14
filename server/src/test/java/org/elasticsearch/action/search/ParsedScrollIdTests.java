/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESTestCase;

public class ParsedScrollIdTests extends ESTestCase {
    public void testHasLocalIndices() {
        final int nResults = randomIntBetween(1, 3);
        final SearchContextIdForNode[] searchContextIdForNodes = new SearchContextIdForNode[nResults];

        boolean hasLocal = false;
        for (int i = 0; i < nResults; i++) {
            String clusterAlias = randomBoolean() ? randomAlphaOfLength(8) : null;
            hasLocal = hasLocal || (clusterAlias == null);
            searchContextIdForNodes[i] =
                new SearchContextIdForNode(clusterAlias, "node_" + i, new ShardSearchContextId(randomAlphaOfLength(8), randomLong()));
        }
        final ParsedScrollId parsedScrollId = new ParsedScrollId(randomAlphaOfLength(8), randomAlphaOfLength(8), searchContextIdForNodes);

        assertEquals(hasLocal, parsedScrollId.hasLocalIndices());
    }
}
