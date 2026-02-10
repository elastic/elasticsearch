/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class StoreDirectoryMetricsIT extends ESIntegTestCase {
    public void testDirectoryMetrics() throws IOException {
        final String indexName = randomIndexName();
        createIndex(indexName, 1, 0);
        IntStream.range(0, between(10, 100)).forEach(i -> indexDoc(indexName, "id " + i, "f", i));
        flush(indexName);

        String node = internalCluster().nodesInclude(indexName).iterator().next();

        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
        final ShardId shardId = new ShardId(clusterService().state().metadata().getProject().index(indexName).getIndex(), 0);

        IndexShard searchShard = indicesService.getShardOrNull(shardId);
        Tuple<Long, DirectoryMetrics> tuple = indicesService.withDirectoryMetrics(() -> {
            try (Engine.Searcher searcher = searchShard.acquireSearcher("test")) {
                Directory directory = searcher.getDirectoryReader().directory();
                String[] files = directory.listAll();
                long reads = 0;
                for (String file : files) {
                    if (file.endsWith("lock") == false) {
                        IndexInput indexInput = directory.openInput(
                            file,
                            file.startsWith(IndexFileNames.SEGMENTS) ? IOContext.READONCE : IOContext.DEFAULT
                        );
                        indexInput.readByte();
                        ++reads;
                        indexInput.seek(directory.fileLength(file) - 1);
                        indexInput.readByte();
                        ++reads;
                        indexInput.close();
                    }
                }
                return reads;
            }
        });
        DirectoryMetrics metrics = tuple.v2();
        StoreMetrics storeMetrics = metrics.metrics("store").cast(StoreMetrics.class);
        assertThat(storeMetrics.getBytesRead(), equalTo(tuple.v1()));
    }
}
