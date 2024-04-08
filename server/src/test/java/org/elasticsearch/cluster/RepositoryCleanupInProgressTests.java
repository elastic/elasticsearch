/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

public class RepositoryCleanupInProgressTests extends ESTestCase {
    public void testChunking() {
        AbstractChunkedSerializingTestCase.assertChunkCount(
            new RepositoryCleanupInProgress(
                randomList(10, () -> new RepositoryCleanupInProgress.Entry(randomAlphaOfLength(10), randomNonNegativeLong()))
            ),
            i -> i.entries().size() + 2
        );
    }
}
