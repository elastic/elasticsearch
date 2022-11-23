/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

public class GlobalCheckpointListenersIT extends ESSingleNodeTestCase {

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @After
    public void shutdownExecutor() {
        executor.shutdown();
    }

    public void testGlobalCheckpointListeners() throws Exception {
        createIndex("test", Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build());
        ensureGreen();
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService test = indicesService.indexService(resolveIndex("test"));
        final IndexShard shard = test.getShardOrNull(0);
        final int numberOfUpdates = randomIntBetween(1, 128);
        for (int i = 0; i < numberOfUpdates; i++) {
            final int index = i;
            final AtomicLong globalCheckpoint = new AtomicLong();
            shard.addGlobalCheckpointListener(i, new GlobalCheckpointListeners.GlobalCheckpointListener() {

                @Override
                public Executor executor() {
                    return executor;
                }

                @Override
                public void accept(final long g, final Exception e) {
                    assertThat(g, greaterThanOrEqualTo(NO_OPS_PERFORMED));
                    assertNull(e);
                    globalCheckpoint.set(g);
                }

            }, null);
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("{}", XContentType.JSON).get();
            assertBusy(() -> assertThat(globalCheckpoint.get(), equalTo((long) index)));
            // adding a listener expecting a lower global checkpoint should fire immediately
            final AtomicLong immediateGlobalCheckpint = new AtomicLong();
            shard.addGlobalCheckpointListener(randomLongBetween(0, i), new GlobalCheckpointListeners.GlobalCheckpointListener() {

                @Override
                public Executor executor() {
                    return executor;
                }

                @Override
                public void accept(final long g, final Exception e) {
                    assertThat(g, greaterThanOrEqualTo(NO_OPS_PERFORMED));
                    assertNull(e);
                    immediateGlobalCheckpint.set(g);
                }

            }, null);
            assertBusy(() -> assertThat(immediateGlobalCheckpint.get(), equalTo((long) index)));
        }
        final AtomicBoolean invoked = new AtomicBoolean();
        shard.addGlobalCheckpointListener(numberOfUpdates, new GlobalCheckpointListeners.GlobalCheckpointListener() {

            @Override
            public Executor executor() {
                return executor;
            }

            @Override
            public void accept(final long g, final Exception e) {
                invoked.set(true);
                assertThat(g, equalTo(UNASSIGNED_SEQ_NO));
                assertThat(e, instanceOf(IndexShardClosedException.class));
                assertThat(((IndexShardClosedException) e).getShardId(), equalTo(shard.shardId()));
            }

        }, null);
        shard.close("closed", randomBoolean());
        assertBusy(() -> assertTrue(invoked.get()));
    }

    public void testGlobalCheckpointListenerTimeout() throws InterruptedException {
        createIndex("test", Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build());
        ensureGreen();
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService test = indicesService.indexService(resolveIndex("test"));
        final IndexShard shard = test.getShardOrNull(0);
        final AtomicBoolean notified = new AtomicBoolean();
        final CountDownLatch latch = new CountDownLatch(1);
        final TimeValue timeout = TimeValue.timeValueMillis(randomIntBetween(1, 50));
        shard.addGlobalCheckpointListener(0, new GlobalCheckpointListeners.GlobalCheckpointListener() {

            @Override
            public Executor executor() {
                return executor;
            }

            @Override
            public void accept(final long g, final Exception e) {
                try {
                    notified.set(true);
                    assertThat(g, equalTo(UNASSIGNED_SEQ_NO));
                    assertNotNull(e);
                    assertThat(e, instanceOf(TimeoutException.class));
                    assertThat(e.getMessage(), equalTo(timeout.getStringRep()));
                } finally {
                    latch.countDown();
                }
            }

        }, timeout);
        latch.await();
        assertTrue(notified.get());
    }

}
