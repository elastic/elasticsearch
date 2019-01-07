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
package org.elasticsearch.indices.state;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexStateService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CloseIndexIT extends ESIntegTestCase {

    public void testCloseMissingIndex() {
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class, () -> client().admin().indices().prepareClose("test").get());
        assertThat(e.getMessage(), is("no such index [test]"));
    }

    public void testCloseOneMissingIndex() {
        createIndex("test1");
        final IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
            () -> client().admin().indices().prepareClose("test1", "test2").get());
        assertThat(e.getMessage(), is("no such index [test2]"));
    }

    public void testCloseOneMissingIndexIgnoreMissing() {
        createIndex("test1");
        assertAcked(client().admin().indices().prepareClose("test1", "test2").setIndicesOptions(IndicesOptions.lenientExpandOpen()));
        assertIndexIsClosed("test1");
    }

    public void testCloseNoIndex() {
        final ActionRequestValidationException e = expectThrows(ActionRequestValidationException.class,
            () -> client().admin().indices().prepareClose().get());
        assertThat(e.getMessage(), containsString("index is missing"));
    }

    public void testCloseNullIndex() {
        final ActionRequestValidationException e = expectThrows(ActionRequestValidationException.class,
            () -> client().admin().indices().prepareClose((String[])null).get());
        assertThat(e.getMessage(), containsString("index is missing"));
    }

    public void testCloseIndex() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        final int nbDocs = randomIntBetween(0, 50);
        indexRandom(randomBoolean(), false, randomBoolean(), IntStream.range(0, nbDocs)
            .mapToObj(i -> client().prepareIndex(indexName, "_doc", String.valueOf(i)).setSource("num", i)).collect(toList()));

        assertAcked(client().admin().indices().prepareClose(indexName));
        assertIndexIsClosed(indexName);

        assertAcked(client().admin().indices().prepareOpen(indexName));
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), nbDocs);
    }

    public void testCloseAlreadyClosedIndex() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        if (randomBoolean()) {
            indexRandom(randomBoolean(), false, randomBoolean(), IntStream.range(0, randomIntBetween(1, 10))
                .mapToObj(i -> client().prepareIndex(indexName, "_doc", String.valueOf(i)).setSource("num", i)).collect(toList()));
        }
        // First close should be acked
        assertAcked(client().admin().indices().prepareClose(indexName));
        assertIndexIsClosed(indexName);

        // Second close should be acked too
        assertAcked(client().admin().indices().prepareClose(indexName));
        assertIndexIsClosed(indexName);
    }

    public void testCloseUnassignedIndex() {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        assertAcked(prepareCreate(indexName)
            .setWaitForActiveShards(ActiveShardCount.NONE)
            .setSettings(Settings.builder().put("index.routing.allocation.include._name", "nothing").build()));

        final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        assertThat(clusterState.metaData().indices().get(indexName).getState(), is(IndexMetaData.State.OPEN));
        assertThat(clusterState.routingTable().allShards().stream().allMatch(ShardRouting::unassigned), is(true));

        assertAcked(client().admin().indices().prepareClose(indexName));
        assertIndexIsClosed(indexName);
    }

    public void testConcurrentClose() throws InterruptedException {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        final int nbDocs = randomIntBetween(10, 50);
        indexRandom(randomBoolean(), false, randomBoolean(), IntStream.range(0, nbDocs)
            .mapToObj(i -> client().prepareIndex(indexName, "_doc", String.valueOf(i)).setSource("num", i)).collect(toList()));
        ensureYellowAndNoInitializingShards(indexName);

        final CountDownLatch startClosing = new CountDownLatch(1);
        final Thread[] threads = new Thread[randomIntBetween(2, 5)];

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                try {
                    startClosing.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                try {
                    client().admin().indices().prepareClose(indexName).get();
                } catch (final Exception e) {
                    assertException(e, indexName);
                }
            });
            threads[i].start();
        }

        startClosing.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
        assertIndexIsClosed(indexName);
    }

    public void testCloseWhileIndexingDocuments() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        int nbDocs = 0;
        try (BackgroundIndexer indexer = new BackgroundIndexer(indexName, "_doc", client())) {
            indexer.setAssertNoFailuresOnStop(false);

            waitForDocs(randomIntBetween(10, 50), indexer);
            assertAcked(client().admin().indices().prepareClose(indexName));
            indexer.stop();
            nbDocs += indexer.totalIndexedDocs();

            final Throwable[] failures = indexer.getFailures();
            if (failures != null) {
                for (Throwable failure : failures) {
                    assertException(failure, indexName);
                }
            }
        }

        assertIndexIsClosed(indexName);
        assertAcked(client().admin().indices().prepareOpen(indexName));
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), nbDocs);
    }

    public void testCloseWhileDeletingIndices() throws Exception {
        final String[] indices = new String[randomIntBetween(3, 10)];
        for (int i = 0; i < indices.length; i++) {
            final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
            createIndex(indexName);
            if (randomBoolean()) {
                indexRandom(randomBoolean(), false, randomBoolean(), IntStream.range(0, 10)
                    .mapToObj(n -> client().prepareIndex(indexName, "_doc", String.valueOf(n)).setSource("num", n)).collect(toList()));
            }
            indices[i] = indexName;
        }
        assertThat(client().admin().cluster().prepareState().get().getState().metaData().indices().size(), equalTo(indices.length));

        final List<Thread> threads = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(1);

        for (final String indexToDelete : indices) {
            threads.add(new Thread(() -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                try {
                    assertAcked(client().admin().indices().prepareDelete(indexToDelete));
                } catch (final Exception e) {
                    assertException(e, indexToDelete);
                }
            }));
        }
        for (final String indexToClose : indices) {
            threads.add(new Thread(() -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                try {
                    client().admin().indices().prepareClose(indexToClose).get();
                } catch (final Exception e) {
                    assertException(e, indexToClose);
                }
            }));
        }

        for (Thread thread : threads) {
            thread.start();
        }
        latch.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
    }

    public void testConcurrentClosesAndOpens() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        final BackgroundIndexer indexer = new BackgroundIndexer(indexName, "_doc", client());
        waitForDocs(1, indexer);

        final CountDownLatch latch = new CountDownLatch(1);
        final Runnable waitForLatch = () -> {
            try {
                latch.await();
            } catch (final InterruptedException e) {
                throw new AssertionError(e);
            }
        };

        final List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 3); i++) {
            threads.add(new Thread(() -> {
                try {
                    waitForLatch.run();
                    client().admin().indices().prepareClose(indexName).get();
                } catch (final Exception e) {
                    throw new AssertionError(e);
                }
            }));
        }
        for (int i = 0; i < randomIntBetween(1, 3); i++) {
            threads.add(new Thread(() -> {
                try {
                    waitForLatch.run();
                    assertAcked(client().admin().indices().prepareOpen(indexName).get());
                } catch (final Exception e) {
                    throw new AssertionError(e);
                }
            }));
        }

        for (Thread thread : threads) {
            thread.start();
        }
        latch.countDown();
        for (Thread thread : threads) {
            thread.join();
        }

        indexer.setAssertNoFailuresOnStop(false);
        indexer.stop();

        final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        if (clusterState.metaData().indices().get(indexName).getState() == IndexMetaData.State.CLOSE) {
            assertIndexIsClosed(indexName);
            assertAcked(client().admin().indices().prepareOpen(indexName));
        }
        refresh(indexName);
        assertIndexIsOpened(indexName);
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), indexer.totalIndexedDocs());
    }

    static void assertIndexIsClosed(final String... indices) {
        final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        for (String index : indices) {
            assertThat(clusterState.metaData().indices().get(index).getState(), is(IndexMetaData.State.CLOSE));
            assertThat(clusterState.routingTable().index(index), nullValue());
            assertThat(clusterState.blocks().hasIndexBlock(index, MetaDataIndexStateService.INDEX_CLOSED_BLOCK), is(true));
            assertThat("Index " + index + " must have only 1 block with [id=" + MetaDataIndexStateService.INDEX_CLOSED_BLOCK_ID + "]",
                clusterState.blocks().indices().getOrDefault(index, emptySet()).stream()
                    .filter(clusterBlock -> clusterBlock.id() == MetaDataIndexStateService.INDEX_CLOSED_BLOCK_ID).count(), equalTo(1L));
        }
    }

    static void assertIndexIsOpened(final String... indices) {
        final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        for (String index : indices) {
            assertThat(clusterState.metaData().indices().get(index).getState(), is(IndexMetaData.State.OPEN));
            assertThat(clusterState.routingTable().index(index), notNullValue());
            assertThat(clusterState.blocks().hasIndexBlock(index, MetaDataIndexStateService.INDEX_CLOSED_BLOCK), is(false));
        }
    }

    static void assertException(final Throwable throwable, final String indexName) {
        final Throwable t = ExceptionsHelper.unwrapCause(throwable);
        if (t instanceof ClusterBlockException) {
            ClusterBlockException clusterBlockException = (ClusterBlockException) t;
            assertThat(clusterBlockException.blocks(), hasSize(1));
            assertTrue(clusterBlockException.blocks().stream().allMatch(b -> b.id() == MetaDataIndexStateService.INDEX_CLOSED_BLOCK_ID));
        } else if (t instanceof IndexClosedException) {
            IndexClosedException indexClosedException = (IndexClosedException) t;
            assertThat(indexClosedException.getIndex(), notNullValue());
            assertThat(indexClosedException.getIndex().getName(), equalTo(indexName));
        } else if (t instanceof IndexNotFoundException) {
            IndexNotFoundException indexNotFoundException = (IndexNotFoundException) t;
            assertThat(indexNotFoundException.getIndex(), notNullValue());
            assertThat(indexNotFoundException.getIndex().getName(), equalTo(indexName));
        } else {
            fail("Unexpected exception: " + t);
        }
    }
}
