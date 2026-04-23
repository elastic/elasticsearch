/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.ShardSearchContextId;

import java.util.Collection;
import java.util.Map;

public class ActiveReaders {

    private final Map<Long, ReaderContext> activeReaders = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
    private final Map<ShardSearchContextId, Long> relocationMap = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
    private final String sessionId;

    ActiveReaders(String searchServiceSessionId) {
        this.sessionId = searchServiceSessionId;
    }

    void put(ReaderContext context) {
        ShardSearchContextId contextId = context.id();
        assert sessionId.equals(contextId.getSessionId());
        ReaderContext previous = activeReaders.put(contextId.getId(), context);
        assert previous == null;
    }

    void putRelocatedReader(Long mappingKey, ReaderContext context) {
        ReaderContext previous = activeReaders.put(mappingKey, context);
        assert previous == null;
    }

    Long generateRelocationMapping(ShardSearchContextId relocatedContextId, long mappingKey) {
        return relocationMap.putIfAbsent(relocatedContextId, mappingKey);
    }

    ReaderContext get(ShardSearchContextId contextId) {
        if (sessionId.equals(contextId.getSessionId())) {
            return activeReaders.get(contextId.getId());
        } else {
            // the reader context was opened in another session, get it via the relocation mapping
            Long activeReaderKey = relocationMap.get(contextId);
            if (activeReaderKey != null) {
                return activeReaders.get(activeReaderKey);
            } else {
                return null;
            }
        }
    }

    ReaderContext remove(ShardSearchContextId contextId) {
        if (sessionId.equals(contextId.getSessionId())) {
            return activeReaders.remove(contextId.getId());
        } else {
            // reader context relocated from another node, also remove the relocation mapping
            final Long activeReaderKey = relocationMap.get(contextId);
            if (activeReaderKey != null) {
                // remove the mapping
                relocationMap.remove(contextId);
                return activeReaders.remove(activeReaderKey);
            } else {
                return null;
            }
        }
    }

    int size() {
        return activeReaders.size();
    }

    /**
     * pkg private for testing
     */
    int relocationMapSize() {
        return relocationMap.size();
    }

    Collection<ReaderContext> values() {
        return activeReaders.values();
    }

}
