/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.search.SearchContextMissingException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class ReaderContextTests extends IndexShardTestCase {

    public void testMarkAsUsedRacesWithClose() throws Exception {
        IndexShard shard = newShard(true);
        try {
            final int racers = between(4, 8);
            final int rounds = scaledRandomIntBetween(20, 100);
            for (int round = 0; round < rounds; round++) {
                final var context = newReaderContext(shard, new ShardSearchContextId(randomAlphaOfLength(8), round));
                final List<Throwable> unexpected = Collections.synchronizedList(new ArrayList<>());
                startInParallel(racers + 1, taskId -> {
                    if (taskId == 0) {
                        context.close();
                        return;
                    }
                    try (Releasable ignored = context.markAsUsed(0L)) {
                        // won the race; ref acquired and immediately released
                    } catch (SearchContextMissingException expected) {
                        // lost the race against close(); this is the expected, recoverable outcome
                    } catch (Throwable other) {
                        unexpected.add(other);
                    }
                });
                assertThat("only SearchContextMissingException is allowed under the race", unexpected, empty());
                SearchContextMissingException e = expectThrows(SearchContextMissingException.class, () -> context.markAsUsed(0L));
                assertThat(e.contextId(), equalTo(context.id()));
            }
        } finally {
            closeShards(shard);
        }
    }

    private static ReaderContext newReaderContext(IndexShard shard, ShardSearchContextId id) {
        return new ReaderContext(id, null, shard, null, 0L, randomBoolean(), 0L);
    }
}
