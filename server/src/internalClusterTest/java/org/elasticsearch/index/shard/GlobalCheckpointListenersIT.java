/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESSingleNodeTestCase;
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
        createIndex("test", Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0).build());
        ensureGreen();
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService test = indicesService.indexService(resolveIndex("test"));
        final IndexShard shard = test.getShardOrNull(0);
        final int numberOfUpdates = randomIntBetween(1, 128);
        for (int i = 0; i < numberOfUpdates; i++) {
            final int index = i;
            final AtomicLong globalCheckpoint = new AtomicLong();
            shard.addGlobalCheckpointListener(
                i,
                new GlobalCheckpointListeners.GlobalCheckpointListener() {

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

                },
                null);
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("{}", XContentType.JSON).get();
            assertBusy(() -> assertThat(globalCheckpoint.get(), equalTo((long) index)));
            // adding a listener expecting a lower global checkpoint should fire immediately
            final AtomicLong immediateGlobalCheckpint = new AtomicLong();
            shard.addGlobalCheckpointListener(
                randomLongBetween(0, i),
                new GlobalCheckpointListeners.GlobalCheckpointListener() {

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

                },
                null);
            assertBusy(() -> assertThat(immediateGlobalCheckpint.get(), equalTo((long) index)));
        }
        final AtomicBoolean invoked = new AtomicBoolean();
        shard.addGlobalCheckpointListener(
            numberOfUpdates,
            new GlobalCheckpointListeners.GlobalCheckpointListener() {

                @Override
                public Executor executor() {
                    return executor;
                }

                @Override
                public void accept(final long g, final Exception e) {
                    invoked.set(true);
                    assertThat(g, equalTo(UNASSIGNED_SEQ_NO));
                    assertThat(e, instanceOf(IndexShardClosedException.class));
                    assertThat(((IndexShardClosedException)e).getShardId(), equalTo(shard.shardId()));
                }

            },
            null);
        shard.close("closed", randomBoolean());
        assertBusy(() -> assertTrue(invoked.get()));
    }

    public void testGlobalCheckpointListenerTimeout() throws InterruptedException {
        createIndex("test", Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0).build());
        ensureGreen();
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService test = indicesService.indexService(resolveIndex("test"));
        final IndexShard shard = test.getShardOrNull(0);
        final AtomicBoolean notified = new AtomicBoolean();
        final CountDownLatch latch = new CountDownLatch(1);
        final TimeValue timeout = TimeValue.timeValueMillis(randomIntBetween(1, 50));
        shard.addGlobalCheckpointListener(
            0,
            new GlobalCheckpointListeners.GlobalCheckpointListener() {

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

            },
            timeout);
        latch.await();
        assertTrue(notified.get());
    }

}
