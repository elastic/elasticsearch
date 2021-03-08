/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.persistent;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

public class PersistentSearchResultsMemoryStore {
    private final Map<String, BytesReference> cache;
    private final PersistentSearchMemoryController memoryController;
    private final ThreadPool threadPool;
    private final NamedWriteableRegistry namedWriteableRegistry;

    // TODO: Add listeners

    public PersistentSearchResultsMemoryStore(PersistentSearchMemoryController memoryController,
                                              ThreadPool threadPool,
                                              NamedWriteableRegistry namedWriteableRegistry) {
        this.cache = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
        this.memoryController = memoryController;
        this.threadPool = threadPool;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    void put(ShardSearchResult shardSearchResult) {
        final String persistentSearchId = shardSearchResult.getId();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Version.writeVersion(Version.CURRENT, out);
            shardSearchResult.writeTo(out);
            final BytesReference bytes = out.bytes();
            memoryController.trackSearchResultSize(bytes.ramBytesUsed());
            cache.put(persistentSearchId, bytes);
        } catch (Exception e) {
            throw new RuntimeException("Unable to serialize shard search result response with id [" + persistentSearchId + "]", e);
        }
    }

    public ShardSearchResult remove(String id) {
        final BytesReference encodedResponse = cache.remove(id);
        if (encodedResponse == null) {
            return null;
        }
        try (StreamInput in = new NamedWriteableAwareStreamInput(encodedResponse.streamInput(), namedWriteableRegistry)) {
            in.setVersion(Version.readVersion(in));
            final ShardSearchResult shardSearchResult = new ShardSearchResult(in);

            memoryController.removedFromCache(encodedResponse.ramBytesUsed());

            return shardSearchResult;
        } catch (Exception e) {
            throw new RuntimeException("Unable to deserialize persistent search response with id [" + id + "]", e);
        }
    }
}
