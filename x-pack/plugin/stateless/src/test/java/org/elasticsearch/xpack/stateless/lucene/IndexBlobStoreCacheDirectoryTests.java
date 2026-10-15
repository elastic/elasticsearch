/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.lucene;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.test.FakeStatelessNode;

import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class IndexBlobStoreCacheDirectoryTests extends ESTestCase {

    public void testOnlyThePerBccMetadataReadDirectoriesClaimOnTheShardReadPool() throws Exception {
        try (var fakeNode = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), 1L)) {
            final var shardReadExecutor = fakeNode.sharedCacheService.getShardReadThreadPoolExecutor();
            final var indexDirectory = IndexBlobStoreCacheDirectory.unwrapDirectory(fakeNode.indexingDirectory);
            final var warmingDirectory = indexDirectory.createNewBlobStoreCacheDirectoryForWarming();

            assertThat(indexDirectory.claimExecutor(), nullValue());
            assertThat(warmingDirectory.claimExecutor(), nullValue());
            assertThat(indexDirectory.createPerBccMetadataReadDirectory().claimExecutor(), sameInstance(shardReadExecutor));
            assertThat(warmingDirectory.createPerBccMetadataReadDirectory().claimExecutor(), sameInstance(shardReadExecutor));
            assertThat(fakeNode.searchDirectory.createPerBccMetadataReadDirectory().claimExecutor(), nullValue());
        }
    }
}
