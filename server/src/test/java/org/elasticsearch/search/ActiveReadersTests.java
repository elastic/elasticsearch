/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.search.internal.LegacyReaderContext;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

public class ActiveReadersTests extends ESTestCase {

    public void testAddAndGetReader() {
        final String sessionId = UUIDs.randomBase64UUID();
        List<String> relocatedSessionIds = randomList(5, 5, UUIDs::randomBase64UUID);
        int numberOfTestContexts = 50;
        Queue<Long> randomUniqueLongs = new LinkedList<>(
            randomSet(numberOfTestContexts, numberOfTestContexts, () -> randomLongBetween(1, 3 * numberOfTestContexts))
        );

        AtomicLong idGenerator = new AtomicLong();
        ActiveReaders activeReaders = new ActiveReaders(sessionId, idGenerator);

        // add a couple of readers, both from same session and relocated ones (different sessionId)
        Map<ShardSearchContextId, ReaderContext> controlData = new HashMap<>();

        for (int i = 0; i < numberOfTestContexts; i++) {
            final ShardSearchContextId id;
            if (randomBoolean()) {
                id = new ShardSearchContextId(sessionId, idGenerator.incrementAndGet());
            } else {
                id = new ShardSearchContextId(randomFrom(relocatedSessionIds), randomUniqueLongs.poll());
            }
            ReaderContext readerContext = createRandomReaderContext(id);
            controlData.put(id, readerContext);
            activeReaders.put(id, readerContext);
        }

        // check that we can retrieve all of them again correctly
        for (ShardSearchContextId contextId : controlData.keySet()) {
            assertSame(controlData.get(contextId), activeReaders.get(contextId));
        }

        // check a few non-existing context ids
        assertNull(activeReaders.get(new ShardSearchContextId(sessionId, idGenerator.get() + randomLongBetween(1, 100))));
        assertNull(activeReaders.get(new ShardSearchContextId(UUIDs.randomBase64UUID(), randomLongBetween(0, idGenerator.get() * 2))));
    }

    public void testRemove() {
        final String sessionId = UUIDs.randomBase64UUID();
        List<String> relocatedSessionIds = randomList(5, 5, UUIDs::randomBase64UUID);
        int numberOfTestContexts = 50;
        Queue<Long> randomUniqueLongs = new LinkedList<>(
            randomSet(numberOfTestContexts, numberOfTestContexts, () -> randomLongBetween(1, 3 * numberOfTestContexts))
        );

        AtomicLong idGenerator = new AtomicLong();
        ActiveReaders activeReaders = new ActiveReaders(sessionId, idGenerator);

        // add a couple of readers, both from same session and relocated ones (different sessionId)
        Map<ShardSearchContextId, ReaderContext> controlData = new HashMap<>();
        int activeRelocatedContexts = 0;

        for (int i = 0; i < numberOfTestContexts; i++) {
            final ShardSearchContextId id;
            if (randomBoolean()) {
                id = new ShardSearchContextId(sessionId, idGenerator.incrementAndGet());
            } else {
                id = new ShardSearchContextId(randomFrom(relocatedSessionIds), randomUniqueLongs.poll());
                activeRelocatedContexts++;
            }
            ReaderContext readerContext = createRandomReaderContext(id);
            controlData.put(id, readerContext);
            activeReaders.put(id, readerContext);
        }
        assertEquals(controlData.size(), activeReaders.size());
        assertEquals(activeRelocatedContexts, activeReaders.relocatioMapSize());

        // remove all contexts in random order
        while (controlData.size() > 0) {
            int lastReaderCount = activeReaders.size();
            int lastRelocatopnMapCount = activeReaders.relocatioMapSize();
            ShardSearchContextId contextId = randomFrom(controlData.keySet());
            assertSame(controlData.remove(contextId), activeReaders.remove(contextId));
            assertEquals(lastReaderCount - 1, activeReaders.size());
            if (contextId.getSessionId().equals(sessionId) == false) {
                assertEquals(lastRelocatopnMapCount - 1, activeReaders.relocatioMapSize());
            } else {
                assertEquals(lastRelocatopnMapCount, activeReaders.relocatioMapSize());
            }
            // trying to remove same id twice should not throw error but return null
            assertNull(activeReaders.remove(contextId));
        }
        assertEquals(0, activeReaders.size());
        assertEquals(0, activeReaders.relocatioMapSize());
    }

    private static ReaderContext createRandomReaderContext(ShardSearchContextId id) {
        IndexShard mockShard = Mockito.mock(IndexShard.class);
        ThreadPool mockThreadPool = Mockito.mock(ThreadPool.class);
        Mockito.when(mockThreadPool.relativeTimeInMillis()).thenReturn(System.currentTimeMillis());
        Mockito.when(mockShard.getThreadPool()).thenReturn(mockThreadPool);
        return randomBoolean()
            ? new LegacyReaderContext(id, null, mockShard, null, Mockito.mock(ShardSearchRequest.class), randomPositiveTimeValue().millis())
            : new ReaderContext(id, null, mockShard, null, randomPositiveTimeValue().millis(), randomBoolean());
    }

}
