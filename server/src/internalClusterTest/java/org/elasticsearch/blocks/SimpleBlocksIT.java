/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.blocks;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequestBuilder;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.action.support.IndicesOptions.lenientExpandOpen;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.elasticsearch.search.internal.SearchContext.TRACK_TOTAL_HITS_ACCURATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class SimpleBlocksIT extends ESIntegTestCase {
    public void testVerifyIndexAndClusterReadOnly() throws Exception {
        // cluster.read_only = null: write and metadata not blocked
        canCreateIndex("test1");
        canIndexDocument("test1");
        setIndexReadOnly("test1", "false");
        assertTrue(indexExists("test1"));

        // cluster.read_only = true: block write and metadata
        setClusterReadOnly(true);
        canNotCreateIndex("test2");
        // even if index has index.read_only = false
        canNotIndexDocument("test1");
        assertTrue(indexExists("test1"));

        // cluster.read_only = false: removes the block
        setClusterReadOnly(false);
        canCreateIndex("test2");
        canIndexDocument("test2");
        canIndexDocument("test1");
        assertTrue(indexExists("test1"));

        // newly created an index has no blocks
        canCreateIndex("ro");
        canIndexDocument("ro");
        assertTrue(indexExists("ro"));

        // adds index write and metadata block
        setIndexReadOnly("ro", "true");
        canNotIndexDocument("ro");
        assertTrue(indexExists("ro"));

        // other indices not blocked
        canCreateIndex("rw");
        canIndexDocument("rw");
        assertTrue(indexExists("rw"));

        // blocks can be removed
        setIndexReadOnly("ro", "false");
        canIndexDocument("ro");
        assertTrue(indexExists("ro"));
    }

    public void testIndexReadWriteMetadataBlocks() {
        canCreateIndex("test1");
        canIndexDocument("test1");
        updateIndexSettings(Settings.builder().put(SETTING_BLOCKS_WRITE, true), "test1");
        canNotIndexDocument("test1");
        updateIndexSettings(Settings.builder().put(SETTING_BLOCKS_WRITE, false), "test1");
        canIndexDocument("test1");
    }

    private void canCreateIndex(String index) {
        try {
            CreateIndexResponse r = indicesAdmin().prepareCreate(index).execute().actionGet();
            assertThat(r, notNullValue());
        } catch (ClusterBlockException e) {
            fail();
        }
    }

    private void canNotCreateIndex(String index) {
        try {
            indicesAdmin().prepareCreate(index).execute().actionGet();
            fail();
        } catch (ClusterBlockException e) {
            // all is well
        }
    }

    private void canIndexDocument(String index) {
        try {
            IndexRequestBuilder builder = client().prepareIndex(index);
            builder.setSource("foo", "bar");
            IndexResponse r = builder.execute().actionGet();
            assertThat(r, notNullValue());
        } catch (ClusterBlockException e) {
            fail();
        }
    }

    private void canNotIndexDocument(String index) {
        try {
            IndexRequestBuilder builder = client().prepareIndex(index);
            builder.setSource("foo", "bar");
            builder.execute().actionGet();
            fail();
        } catch (ClusterBlockException e) {
            // all is well
        }
    }

    private void setIndexReadOnly(String index, String value) {
        updateIndexSettings(Settings.builder().put(SETTING_READ_ONLY, value), index);
    }

    public void testAddBlocksWhileExistingBlocks() {
        createIndex("test");
        ensureGreen("test");

        for (APIBlock otherBlock : APIBlock.values()) {
            if (otherBlock == APIBlock.READ_ONLY_ALLOW_DELETE) {
                continue;
            }

            for (APIBlock block : Arrays.asList(APIBlock.READ, APIBlock.WRITE)) {
                try {
                    enableIndexBlock("test", block.settingName());

                    // Adding a block is not blocked
                    AcknowledgedResponse addBlockResponse = indicesAdmin().prepareAddBlock(otherBlock, "test").get();
                    assertAcked(addBlockResponse);
                } finally {
                    disableIndexBlock("test", otherBlock.settingName());
                    disableIndexBlock("test", block.settingName());
                }
            }

            for (APIBlock block : Arrays.asList(APIBlock.READ_ONLY, APIBlock.METADATA, APIBlock.READ_ONLY_ALLOW_DELETE)) {
                boolean success = false;
                try {
                    enableIndexBlock("test", block.settingName());
                    // Adding a block is blocked when there is a metadata block and the new block to be added is not a metadata block
                    if (block.getBlock().contains(ClusterBlockLevel.METADATA_WRITE)
                        && otherBlock.getBlock().contains(ClusterBlockLevel.METADATA_WRITE) == false) {
                        assertBlocked(indicesAdmin().prepareAddBlock(otherBlock, "test"));
                    } else {
                        assertAcked(indicesAdmin().prepareAddBlock(otherBlock, "test"));
                        success = true;
                    }
                } finally {
                    if (success) {
                        disableIndexBlock("test", otherBlock.settingName());
                    }
                    disableIndexBlock("test", block.settingName());
                }
            }
        }
    }

    public void testAddBlockToMissingIndex() {
        IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> indicesAdmin().prepareAddBlock(randomAddableBlock(), "test").get()
        );
        assertThat(e.getMessage(), is("no such index [test]"));
    }

    public void testAddBlockToOneMissingIndex() {
        createIndex("test1");
        final IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> indicesAdmin().prepareAddBlock(randomAddableBlock(), "test1", "test2").get()
        );
        assertThat(e.getMessage(), is("no such index [test2]"));
    }

    public void testCloseOneMissingIndexIgnoreMissing() throws Exception {
        createIndex("test1");
        final APIBlock block = randomAddableBlock();
        try {
            assertBusy(() -> assertAcked(indicesAdmin().prepareAddBlock(block, "test1", "test2").setIndicesOptions(lenientExpandOpen())));
            assertIndexHasBlock(block, "test1");
        } finally {
            disableIndexBlock("test1", block);
        }
    }

    public void testAddBlockNoIndex() {
        final ActionRequestValidationException e = expectThrows(
            ActionRequestValidationException.class,
            () -> indicesAdmin().prepareAddBlock(randomAddableBlock()).get()
        );
        assertThat(e.getMessage(), containsString("index is missing"));
    }

    public void testAddBlockNullIndex() {
        expectThrows(NullPointerException.class, () -> indicesAdmin().prepareAddBlock(randomAddableBlock(), (String[]) null));
    }

    public void testCannotAddReadOnlyAllowDeleteBlock() {
        createIndex("test1");
        final AddIndexBlockRequestBuilder request = indicesAdmin().prepareAddBlock(APIBlock.READ_ONLY_ALLOW_DELETE, "test1");
        final ActionRequestValidationException e = expectThrows(ActionRequestValidationException.class, request::get);
        assertThat(e.getMessage(), containsString("read_only_allow_delete block is for internal use only"));
    }

    public void testAddIndexBlock() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);
        ensureGreen(indexName);

        final int nbDocs = randomIntBetween(0, 50);
        indexRandom(
            randomBoolean(),
            false,
            randomBoolean(),
            IntStream.range(0, nbDocs)
                .mapToObj(i -> client().prepareIndex(indexName).setId(String.valueOf(i)).setSource("num", i))
                .collect(toList())
        );

        final APIBlock block = randomAddableBlock();
        try {
            AddIndexBlockResponse response = indicesAdmin().prepareAddBlock(block, indexName).get();
            assertTrue("Add block [" + block + "] to index [" + indexName + "] not acknowledged: " + response, response.isAcknowledged());
            assertIndexHasBlock(block, indexName);
        } finally {
            disableIndexBlock(indexName, block);
        }

        indicesAdmin().prepareRefresh(indexName).get();
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), nbDocs);
    }

    public void testSameBlockTwice() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        if (randomBoolean()) {
            indexRandom(
                randomBoolean(),
                false,
                randomBoolean(),
                IntStream.range(0, randomIntBetween(1, 10))
                    .mapToObj(i -> client().prepareIndex(indexName).setId(String.valueOf(i)).setSource("num", i))
                    .collect(toList())
            );
        }
        final APIBlock block = randomAddableBlock();
        try {
            assertAcked(indicesAdmin().prepareAddBlock(block, indexName));
            assertIndexHasBlock(block, indexName);
            // Second add block should be acked too, even if it was a METADATA block
            assertAcked(indicesAdmin().prepareAddBlock(block, indexName));
            assertIndexHasBlock(block, indexName);
        } finally {
            disableIndexBlock(indexName, block);
        }
    }

    public void testAddBlockToUnassignedIndex() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        assertAcked(
            prepareCreate(indexName).setWaitForActiveShards(ActiveShardCount.NONE)
                .setSettings(Settings.builder().put("index.routing.allocation.include._name", "nothing").build())
        );

        final ClusterState clusterState = clusterAdmin().prepareState().get().getState();
        assertThat(clusterState.metadata().indices().get(indexName).getState(), is(IndexMetadata.State.OPEN));
        assertThat(clusterState.routingTable().allShards().allMatch(ShardRouting::unassigned), is(true));

        final APIBlock block = randomAddableBlock();
        try {
            assertAcked(indicesAdmin().prepareAddBlock(block, indexName));
            assertIndexHasBlock(block, indexName);
        } finally {
            disableIndexBlock(indexName, block);
        }
    }

    public void testConcurrentAddBlock() throws InterruptedException {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        final int nbDocs = randomIntBetween(10, 50);
        indexRandom(
            randomBoolean(),
            false,
            randomBoolean(),
            IntStream.range(0, nbDocs)
                .mapToObj(i -> client().prepareIndex(indexName).setId(String.valueOf(i)).setSource("num", i))
                .collect(toList())
        );
        ensureYellowAndNoInitializingShards(indexName);

        final CountDownLatch startClosing = new CountDownLatch(1);
        final Thread[] threads = new Thread[randomIntBetween(2, 5)];

        final APIBlock block = randomAddableBlock();

        try {
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(() -> {
                    try {
                        startClosing.await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    try {
                        indicesAdmin().prepareAddBlock(block, indexName).get();
                        assertIndexHasBlock(block, indexName);
                    } catch (final ClusterBlockException e) {
                        assertThat(e.blocks(), hasSize(1));
                        assertTrue(e.blocks().stream().allMatch(b -> b.id() == block.getBlock().id()));
                    }
                });
                threads[i].start();
            }

            startClosing.countDown();
            for (Thread thread : threads) {
                thread.join();
            }
            assertIndexHasBlock(block, indexName);
        } finally {
            disableIndexBlock(indexName, block);
        }
    }

    public void testAddBlockWhileIndexingDocuments() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);
        ensureGreen(indexName);

        final APIBlock block = randomAddableBlock();
        long nbDocs = 0;
        try {
            try (BackgroundIndexer indexer = new BackgroundIndexer(indexName, client(), 1000)) {
                indexer.setFailureAssertion(t -> {
                    Throwable cause = ExceptionsHelper.unwrapCause(t);
                    assertThat(cause, instanceOf(ClusterBlockException.class));
                    ClusterBlockException e = (ClusterBlockException) cause;
                    assertThat(e.blocks(), hasSize(1));
                    assertTrue(e.blocks().stream().allMatch(b -> b.id() == block.getBlock().id()));
                });

                waitForDocs(randomIntBetween(10, 50), indexer);
                final AddIndexBlockResponse response = indicesAdmin().prepareAddBlock(block, indexName).get();
                assertTrue(
                    "Add block [" + block + "] to index [" + indexName + "] not acknowledged: " + response,
                    response.isAcknowledged()
                );
                indexer.stopAndAwaitStopped();
                nbDocs += indexer.totalIndexedDocs();
            }
            assertIndexHasBlock(block, indexName);
        } finally {
            disableIndexBlock(indexName, block);
        }
        refresh(indexName);
        assertHitCount(client().prepareSearch(indexName).setSize(0).setTrackTotalHitsUpTo(TRACK_TOTAL_HITS_ACCURATE).get(), nbDocs);
    }

    public void testAddBlockWhileDeletingIndices() throws Exception {
        final String[] indices = new String[randomIntBetween(3, 10)];
        for (int i = 0; i < indices.length; i++) {
            final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
            createIndex(indexName);
            if (randomBoolean()) {
                indexRandom(
                    randomBoolean(),
                    false,
                    randomBoolean(),
                    IntStream.range(0, 10)
                        .mapToObj(n -> client().prepareIndex(indexName).setId(String.valueOf(n)).setSource("num", n))
                        .collect(toList())
                );
            }
            indices[i] = indexName;
        }
        assertThat(clusterAdmin().prepareState().get().getState().metadata().indices().size(), equalTo(indices.length));

        final List<Thread> threads = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(1);

        final APIBlock block = randomAddableBlock();

        Consumer<Exception> exceptionConsumer = t -> {
            Throwable cause = ExceptionsHelper.unwrapCause(t);
            if (cause instanceof ClusterBlockException e) {
                assertThat(e.blocks(), hasSize(1));
                assertTrue(e.blocks().stream().allMatch(b -> b.id() == block.getBlock().id()));
            } else {
                assertThat(cause, instanceOf(IndexNotFoundException.class));
            }
        };

        try {
            for (final String indexToDelete : indices) {
                threads.add(new Thread(() -> {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    try {
                        assertAcked(indicesAdmin().prepareDelete(indexToDelete));
                    } catch (final Exception e) {
                        exceptionConsumer.accept(e);
                    }
                }));
            }
            for (final String indexToBlock : indices) {
                threads.add(new Thread(() -> {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    try {
                        indicesAdmin().prepareAddBlock(block, indexToBlock).get();
                    } catch (final Exception e) {
                        exceptionConsumer.accept(e);
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
        } finally {
            for (final String indexToBlock : indices) {
                try {
                    disableIndexBlock(indexToBlock, block);
                } catch (IndexNotFoundException infe) {
                    // ignore
                }
            }
        }
    }

    static void assertIndexHasBlock(APIBlock block, final String... indices) {
        final ClusterState clusterState = clusterAdmin().prepareState().get().getState();
        for (String index : indices) {
            final IndexMetadata indexMetadata = clusterState.metadata().indices().get(index);
            final Settings indexSettings = indexMetadata.getSettings();
            assertThat(indexSettings.hasValue(block.settingName()), is(true));
            assertThat(indexSettings.getAsBoolean(block.settingName(), false), is(true));
            assertThat(clusterState.blocks().hasIndexBlock(index, block.getBlock()), is(true));
            assertThat(
                "Index " + index + " must have only 1 block with [id=" + block.getBlock().id() + "]",
                clusterState.blocks()
                    .indices()
                    .getOrDefault(index, emptySet())
                    .stream()
                    .filter(clusterBlock -> clusterBlock.id() == block.getBlock().id())
                    .count(),
                equalTo(1L)
            );
        }
    }

    public static void disableIndexBlock(String index, APIBlock block) {
        disableIndexBlock(index, block.settingName());
    }

    /**
     * The read-only-allow-delete block cannot be added via the add index block API; this method chooses randomly from the values that
     * the add index block API does support.
     */
    private static APIBlock randomAddableBlock() {
        return randomValueOtherThan(APIBlock.READ_ONLY_ALLOW_DELETE, () -> randomFrom(APIBlock.values()));
    }
}
