/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;

public class FetchSearchResultTests extends ESTestCase {

    public void testClearLastChunkBytesReleasesTheBytesAndResetsTheLastChunk() {
        FetchSearchResult result = new FetchSearchResult(
            new ShardSearchContextId("test", 1L),
            new SearchShardTarget("node", new ShardId(new Index("idx", "uuid"), 0), null)
        );
        try {
            AtomicBoolean released = new AtomicBoolean();
            result.setLastChunkBytes(new ReleasableBytesReference(new BytesArray(new byte[] { 1, 2, 3 }), () -> released.set(true)), 1);
            result.setLastChunkSequenceStart(0L);

            result.clearLastChunkBytes();

            assertTrue("the bytes should be released immediately", released.get());
            assertNull(result.getLastChunkBytes());
            assertThat(result.getLastChunkHitCount(), equalTo(0));
            assertThat(result.getLastChunkSequenceStart(), equalTo(-1L));
        } finally {
            result.decRef();
        }
    }
}
