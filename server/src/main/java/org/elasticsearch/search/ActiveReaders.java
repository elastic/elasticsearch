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
import java.util.concurrent.atomic.AtomicLong;

public class ActiveReaders {

    private final Map<Long, ReaderContext> activeReaders = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
    private final Map<ShardSearchContextId, Long> relocationMap = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
    private final String sessionId;
    private final AtomicLong idGenerator;

    ActiveReaders(String searchServiceSessionId, AtomicLong idGenerator) {
        this.sessionId = searchServiceSessionId;
        this.idGenerator = idGenerator;
    }

    ReaderContext put(ShardSearchContextId contextId, ReaderContext context) {
        if (sessionId.equals(contextId.getSessionId())) {
            return activeReaders.put(contextId.getId(), context);
        } else {
            // the reader context is relocated from another session, keep a relocation mapping for retrieval
            long activeReaderKey = idGenerator.incrementAndGet();
            relocationMap.put(contextId, activeReaderKey);
            return activeReaders.put(activeReaderKey, context);
        }
    }

    ReaderContext get(ShardSearchContextId contextId) {
        if (sessionId.equals(contextId.getSessionId())) {
            return activeReaders.get(contextId.getId());
        } else {
            // the reader context is relocated from another session, get the relocation mapping
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
    int relocatioMapSize() {
        return relocationMap.size();
    }

    Collection<ReaderContext> values() {
        return activeReaders.values();
    }
}
