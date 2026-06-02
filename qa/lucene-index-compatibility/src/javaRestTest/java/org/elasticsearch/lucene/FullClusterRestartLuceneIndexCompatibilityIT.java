/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene;

import org.elasticsearch.client.Request;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_READ_ONLY_BLOCK;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_WRITE_BLOCK;
import static org.elasticsearch.cluster.metadata.MetadataIndexStateService.INDEX_CLOSED_BLOCK;
import static org.elasticsearch.cluster.metadata.MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING;
import static org.elasticsearch.cluster.metadata.MetadataIndexStateService.VERIFIED_READ_ONLY_SETTING;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class FullClusterRestartLuceneIndexCompatibilityIT extends FullClusterRestartIndexCompatibilityTestCase {

    static {
        clusterConfig = config -> config.setting("xpack.license.self_generated.type", "trial");
    }

    public FullClusterRestartLuceneIndexCompatibilityIT(Version version) {
        super(version);
    }

    /**
     * Creates an index on N-2, upgrades to N-1 and marks as read-only, then upgrades to N.
     */
    public void testIndexUpgrade() throws Exception {
        final String index = suffix("index");
        final int numDocs = 2431;

        if (isFullyUpgradedTo(VERSION_MINUS_2)) {
            createIndex(
                client(),
                index,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomInt(2))
                    .build()
            );
            indexDocs(index, numDocs);
            return;
        }

        assertThat(indexVersion(index), equalTo(VERSION_MINUS_2));
        ensureGreen(index);

        if (isIndexClosed(index) == false) {
            assertDocCount(client(), index, numDocs);
        }

        if (isFullyUpgradedTo(VERSION_MINUS_1)) {
            final boolean maybeClose = randomBoolean();
            if (maybeClose) {
                logger.debug("--> closing index [{}] before upgrade", index);
                closeIndex(index);
            }

            final var block = randomFrom(IndexMetadata.APIBlock.WRITE, IndexMetadata.APIBlock.READ_ONLY);
            addIndexBlock(index, block);

            assertThat(indexBlocks(index), maybeClose ? contains(INDEX_CLOSED_BLOCK, block.getBlock()) : contains(block.getBlock()));
            assertIndexSetting(index, VERIFIED_BEFORE_CLOSE_SETTING, is(maybeClose));
            assertIndexSetting(index, VERIFIED_READ_ONLY_SETTING, is(true));
            return;
        }

        if (isFullyUpgradedTo(VERSION_CURRENT)) {
            final var isClosed = isIndexClosed(index);
            logger.debug("--> upgraded index [{}] is in [{}] state", index, isClosed ? "closed" : "open");
            assertThat(
                indexBlocks(index),
                isClosed
                    ? either(contains(INDEX_CLOSED_BLOCK, INDEX_WRITE_BLOCK)).or(contains(INDEX_CLOSED_BLOCK, INDEX_READ_ONLY_BLOCK))
                    : either(contains(INDEX_WRITE_BLOCK)).or(contains(INDEX_READ_ONLY_BLOCK))
            );
            assertIndexSetting(index, VERIFIED_BEFORE_CLOSE_SETTING, is(isClosed));
            assertIndexSetting(index, VERIFIED_READ_ONLY_SETTING, is(true));

            if (isClosed == false) {
                logger.debug("--> write/read_only API blocks cannot be removed on an opened index");
                var ex = expectUpdateIndexSettingsThrows(
                    index,
                    Settings.builder()
                        .putNull(IndexMetadata.APIBlock.WRITE.settingName())
                        .putNull(IndexMetadata.APIBlock.READ_ONLY.settingName())
                );
                assertThat(ex.getMessage(), containsStringCannotRemoveBlockOnReadOnlyIndex(index));

            } else if (randomBoolean()) {
                logger.debug("--> write/read_only API blocks can be removed on a closed index: INDEX_CLOSED_BLOCK already blocks writes");
                updateIndexSettings(
                    index,
                    Settings.builder()
                        .putNull(IndexMetadata.APIBlock.WRITE.settingName())
                        .putNull(IndexMetadata.APIBlock.READ_ONLY.settingName())
                );

                assertThat(indexBlocks(index), contains(INDEX_CLOSED_BLOCK));
                assertIndexSetting(index, VERIFIED_BEFORE_CLOSE_SETTING, is(true));
                assertIndexSetting(index, VERIFIED_READ_ONLY_SETTING, is(true));
            }

            var block = indexBlocks(index).stream().filter(c -> c.equals(INDEX_WRITE_BLOCK) || c.equals(INDEX_READ_ONLY_BLOCK)).findFirst();
            if (block.isPresent() && block.get().equals(INDEX_READ_ONLY_BLOCK)) {
                logger.debug("--> read_only API block can be replaced by a write block (required for the remaining tests)");
                updateIndexSettings(
                    index,
                    Settings.builder()
                        .putNull(IndexMetadata.APIBlock.READ_ONLY.settingName())
                        .put(IndexMetadata.APIBlock.WRITE.settingName(), true)
                );

                assertThat(indexBlocks(index), isClosed ? contains(INDEX_CLOSED_BLOCK, INDEX_WRITE_BLOCK) : contains(INDEX_WRITE_BLOCK));
                assertIndexSetting(index, VERIFIED_BEFORE_CLOSE_SETTING, is(isClosed));
                assertIndexSetting(index, VERIFIED_READ_ONLY_SETTING, is(true));
            }

            var numberOfReplicas = getNumberOfReplicas(index);
            if (0 < numberOfReplicas) {
                logger.debug("--> resetting number of replicas [{}] to [0]", numberOfReplicas);
                updateIndexSettings(index, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0));
            }

            updateRandomIndexSettings(index);
            updateRandomMappings(index);

            logger.debug("--> adding replica to test peer-recovery");
            updateIndexSettings(index, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));
            ensureGreen(index);

            if (isClosed) {
                logger.debug("--> re-opening index [{}]", index);
                openIndex(index);
                ensureGreen(index);

                assertDocCount(client(), index, numDocs);
            } else {
                logger.debug("--> closing index [{}]", index);
                closeIndex(index);
                ensureGreen(index);
            }

            logger.debug("--> adding more replicas to test peer-recovery");
            updateIndexSettings(index, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2));
            ensureGreen(index);

            assertIndexSetting(index, VERIFIED_READ_ONLY_SETTING, is(true));
            assertThat(
                indexBlocks(index),
                isIndexClosed(index) ? contains(INDEX_CLOSED_BLOCK, INDEX_WRITE_BLOCK) : contains(INDEX_WRITE_BLOCK)
            );

            deleteIndex(index);
        }
    }

    /**
     * Creates an index on N-2, closes it on N-1 (without marking it as read-only), then upgrades to N.
     */
    public void testClosedIndexUpgrade() throws Exception {
        final String index = suffix("index");
        final int numDocs = 2437;

        if (isFullyUpgradedTo(VERSION_MINUS_2)) {
            createIndex(
                client(),
                index,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomInt(2))
                    .build()
            );
            indexDocs(index, numDocs);
            return;
        }

        assertThat(indexVersion(index), equalTo(VERSION_MINUS_2));
        ensureGreen(index);

        if (isIndexClosed(index) == false) {
            assertDocCount(client(), index, numDocs);
        }

        if (isFullyUpgradedTo(VERSION_MINUS_1)) {
            logger.debug("--> [{}] closing index before upgrade without adding a read_only/write block", index);
            closeIndex(index);

            assertThat(indexBlocks(index), contains(INDEX_CLOSED_BLOCK));
            assertThat(indexBlocks(index), not(contains(INDEX_WRITE_BLOCK)));
            assertIndexSetting(index, VERIFIED_BEFORE_CLOSE_SETTING, is(true));
            assertIndexSetting(index, VERIFIED_READ_ONLY_SETTING, is(false));
            return;
        }

        if (isFullyUpgradedTo(VERSION_CURRENT)) {
            assertThat(indexBlocks(index), contains(INDEX_CLOSED_BLOCK));
            assertIndexSetting(index, VERIFIED_BEFORE_CLOSE_SETTING, is(true));
            assertIndexSetting(index, VERIFIED_READ_ONLY_SETTING, is(false));

            logger.debug("--> re-opening index [{}] will add a write block", index);
            openIndex(index);
            ensureGreen(index);

            assertThat(indexBlocks(index), contains(INDEX_WRITE_BLOCK));
            assertIndexSetting(index, VERIFIED_BEFORE_CLOSE_SETTING, is(false));
            assertIndexSetting(index, VERIFIED_READ_ONLY_SETTING, is(true));
            assertDocCount(client(), index, numDocs);

            logger.debug("--> closing index [{}]", index);
            closeIndex(index);
            ensureGreen(index);

            assertThat(indexBlocks(index), contains(INDEX_CLOSED_BLOCK, INDEX_WRITE_BLOCK));
            assertIndexSetting(index, VERIFIED_BEFORE_CLOSE_SETTING, is(true));
            assertIndexSetting(index, VERIFIED_READ_ONLY_SETTING, is(true));

            deleteIndex(index);
        }
    }

    /**
     * Creates an index on N-2, marks as read-only on N-1 and creates a snapshot, then restores the snapshot on N.
     */
    public void testRestoreIndex() throws Exception {
        final String repository = suffix("repository");
        final String snapshot = suffix("snapshot");
        final String index = suffix("index");
        final int numDocs = 1234;

        if (isFullyUpgradedTo(VERSION_MINUS_2)) {
            logger.debug("--> registering repository [{}]", repository);
            registerRepository(client(), repository, FsRepository.TYPE, true, repositorySettings());

            logger.debug("--> creating index [{}]", index);
            createIndex(
                client(),
                index,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
            );

            logger.debug("--> indexing [{}] docs in [{}]", numDocs, index);
            indexDocs(index, numDocs);
            return;
        }

        if (isFullyUpgradedTo(VERSION_MINUS_1)) {
            ensureGreen(index);

            assertThat(indexVersion(index), equalTo(VERSION_MINUS_2));
            assertDocCount(client(), index, numDocs);

            addIndexBlock(index, IndexMetadata.APIBlock.WRITE);

            logger.debug("--> creating snapshot [{}]", snapshot);
            createSnapshot(client(), repository, snapshot, true);

            logger.debug("--> deleting index [{}]", index);
            deleteIndex(index);
            return;
        }

        if (isFullyUpgradedTo(VERSION_CURRENT)) {
            var restoredIndex = suffix("index-restored");
            logger.debug("--> restoring index [{}] as [{}]", index, restoredIndex);
            restoreIndex(repository, snapshot, index, restoredIndex);
            ensureGreen(restoredIndex);

            assertIndexSetting(restoredIndex, VERIFIED_READ_ONLY_SETTING, is(true));
            assertThat(indexBlocks(restoredIndex), contains(INDEX_WRITE_BLOCK));
            assertThat(indexVersion(restoredIndex), equalTo(VERSION_MINUS_2));
            assertDocCount(client(), restoredIndex, numDocs);

            updateRandomIndexSettings(restoredIndex);
            updateRandomMappings(restoredIndex);

            logger.debug("--> adding replica to test peer-recovery");
            updateIndexSettings(restoredIndex, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));
            ensureGreen(restoredIndex);

            logger.debug("--> closing restored index [{}]", restoredIndex);
            closeIndex(restoredIndex);
            ensureGreen(restoredIndex);

            logger.debug("--> adding replica to test peer-recovery for closed shards");
            updateIndexSettings(restoredIndex, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2));
            ensureGreen(restoredIndex);

            logger.debug("--> re-opening restored index [{}]", restoredIndex);
            openIndex(restoredIndex);
            ensureGreen(restoredIndex);

            assertDocCount(client(), restoredIndex, numDocs);

            logger.debug("--> deleting restored index [{}]", restoredIndex);
            deleteIndex(restoredIndex);
        }
    }

    /**
     * Creates an index on N-2, marks as read-only on N-1 and creates a snapshot and then closes the index, then restores the snapshot on N.
     */
    public void testRestoreIndexOverClosedIndex() throws Exception {
        final String repository = suffix("repository");
        final String snapshot = suffix("snapshot");
        final String index = suffix("index");
        final int numDocs = 2134;

        if (isFullyUpgradedTo(VERSION_MINUS_2)) {
            logger.debug("--> registering repository [{}]", repository);
            registerRepository(client(), repository, FsRepository.TYPE, true, repositorySettings());

            logger.debug("--> creating index [{}]", index);
            createIndex(
                client(),
                index,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
            );

            logger.debug("--> indexing [{}] docs in [{}]", numDocs, index);
            indexDocs(index, numDocs);
            return;
        }

        if (isFullyUpgradedTo(VERSION_MINUS_1)) {
            ensureGreen(index);

            assertThat(indexVersion(index), equalTo(VERSION_MINUS_2));
            assertDocCount(client(), index, numDocs);

            addIndexBlock(index, IndexMetadata.APIBlock.WRITE);

            logger.debug("--> creating snapshot [{}]", snapshot);
            createSnapshot(client(), repository, snapshot, true);

            logger.debug("--> force-merge index [{}] to 1 segment", index);
            forceMerge(index, 1);

            logger.debug("--> closing index [{}]", index);
            closeIndex(index);
            ensureGreen(index);
            return;
        }

        if (isFullyUpgradedTo(VERSION_CURRENT)) {
            assertThat(isIndexClosed(index), equalTo(true));
            assertThat(indexBlocks(index), contains(INDEX_CLOSED_BLOCK, INDEX_WRITE_BLOCK));
            assertIndexSetting(index, VERIFIED_READ_ONLY_SETTING, is(true));

            logger.debug("--> restoring index [{}] over existing closed index", index);
            restoreIndex(repository, snapshot, index, index);
            ensureGreen(index);

            assertThat(indexVersion(index), equalTo(VERSION_MINUS_2));
            assertDocCount(client(), index, numDocs);

            logger.debug("--> deleting index [{}]", index);
            deleteIndex(index);
        }
    }

    /**
     * Creates an index on N-2 and takes a snapshot on N-2 (without marking as read-only first),
     * then restores the snapshot directly on N. This reproduces the scenario reported in
     * <a href="https://github.com/elastic/elasticsearch/issues/145141">#145141</a> where restoring
     * a 7.x snapshot on 9.x fails because the verified_read_only flag was never set.
     */
    public void testRestoreSnapshotCreatedOnOldVersion() throws Exception {
        final String repository = suffix("repository");
        final String snapshot = suffix("snapshot");
        final String index = suffix("index");
        final int numDocs = 1543;

        if (isFullyUpgradedTo(VERSION_MINUS_2)) {
            logger.debug("--> registering repository [{}]", repository);
            registerRepository(client(), repository, FsRepository.TYPE, true, repositorySettings());

            logger.debug("--> creating index [{}]", index);
            createIndex(
                client(),
                index,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
            );

            logger.debug("--> indexing [{}] docs in [{}]", numDocs, index);
            indexDocs(index, numDocs);

            logger.debug("--> creating snapshot [{}] on N-2 without read-only block", snapshot);
            createSnapshot(client(), repository, snapshot, true);

            logger.debug("--> deleting index [{}]", index);
            deleteIndex(index);
            return;
        }

        if (isFullyUpgradedTo(VERSION_MINUS_1)) {
            // snapshot was already created on N-2; nothing to do on N-1
            return;
        }

        if (isFullyUpgradedTo(VERSION_CURRENT)) {
            var restoredIndex = suffix("index-restored-from-old");
            logger.debug("--> restoring snapshot [{}] created on N-2 as [{}]", snapshot, restoredIndex);
            restoreIndex(repository, snapshot, index, restoredIndex);
            ensureGreen(restoredIndex);

            assertIndexSetting(restoredIndex, VERIFIED_READ_ONLY_SETTING, is(true));
            assertThat(indexBlocks(restoredIndex), contains(INDEX_WRITE_BLOCK));
            assertThat(indexVersion(restoredIndex), equalTo(VERSION_MINUS_2));
            assertDocCount(client(), restoredIndex, numDocs);

            updateRandomIndexSettings(restoredIndex);
            updateRandomMappings(restoredIndex);

            logger.debug("--> adding replica to test peer-recovery");
            updateIndexSettings(restoredIndex, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));
            ensureGreen(restoredIndex);

            logger.debug("--> closing restored index [{}]", restoredIndex);
            closeIndex(restoredIndex);
            ensureGreen(restoredIndex);

            logger.debug("--> re-opening restored index [{}]", restoredIndex);
            openIndex(restoredIndex);
            ensureGreen(restoredIndex);

            assertDocCount(client(), restoredIndex, numDocs);

            logger.debug("--> deleting restored index [{}]", restoredIndex);
            deleteIndex(restoredIndex);
        }
    }

    /**
     * Attempts to capture a snapshot whose Lucene commit has {@code local_checkpoint < max_seq_no}
     * on the N-2 cluster (7.x). Concurrent indexing threads run while up to 10 snapshots are
     * taken. After each snapshot it is mounted as a full-copy searchable snapshot and {@code _stats}
     * on the mounted index is checked for {@code local_checkpoint < max_seq_no}.
     * The loop stops as soon as such a snapshot is found.
     * The live index is then deleted so that no 7.x index remains in the cluster state during
     * the upgrade to 9.x. On the N cluster (9.x), if a lagged snapshot was captured, it is
     * restored as a read-only index via {@code RestoreService.prepareForReadOnlyRestore()}, and
     * {@code Store#checkAndPatchLocalCheckpoint()} must have equalized {@code local_checkpoint}
     * with {@code max_seq_no} after restore. The test is skipped if no natural lag is observed.
     */
    @SuppressWarnings("resource")
    public void testRestoreCheckpointPatch() throws Exception {
        final String repository = suffix("repository");
        final String index = suffix("index");
        final int numSnapshots = 10;
        final int numThreads = 10;
        final Path metaFile = Path.of(repositorySettings().get("location")).resolve("test-snaps-taken");

        if (isFullyUpgradedTo(VERSION_MINUS_2)) {
            logger.debug("--> registering repository [{}]", repository);
            registerRepository(client(), repository, FsRepository.TYPE, true, repositorySettings());

            logger.debug("--> creating index [{}]", index);
            createIndex(
                client(),
                index,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build()
            );
            ensureGreen(index);

            final AtomicBoolean keepIndexing = new AtomicBoolean(true);
            final AtomicInteger docId = new AtomicInteger();
            final CountDownLatch indexingDone = new CountDownLatch(numThreads);
            final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            for (int t = 0; t < numThreads; t++) {
                executor.submit(() -> {
                    try {
                        while (keepIndexing.get()) {
                            int id = docId.getAndIncrement();
                            var req = new Request("PUT", "/" + index + "/_doc/" + id);
                            req.setJsonEntity("{\"f\": 1}");
                            try {
                                client().performRequest(req);
                            } catch (IOException ignored) {}
                        }
                    } finally {
                        indexingDone.countDown();
                    }
                });
            }

            boolean lagObserved = false;
            int snapshotsTaken = 0;
            for (int s = 0; s < numSnapshots; s++) {
                createSnapshot(client(), repository, "snap-" + s, true);
                snapshotsTaken++;
                // Mount as full-copy searchable snapshot: ReadOnlyEngine reads the Lucene commit
                // as-is (no bootstrapNewHistory()), so _stats reflects the lcp/msn in the blob.
                var checkIndex = suffix("check-" + s);
                mountIndex(repository, "snap-" + s, index, false, checkIndex);
                ensureGreen(checkIndex);
                long[] seqNo = readPrimaryShardSeqNoStats(checkIndex);
                deleteIndex(checkIndex);
                if (seqNo[1] < seqNo[0]) {
                    lagObserved = true;
                    break;
                }
            }
            keepIndexing.set(false);
            assertTrue(indexingDone.await(60, TimeUnit.SECONDS));
            executor.close();

            var countPath = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/" + index + "/_count")));
            long docCount = ((Number) countPath.evaluate("count")).longValue();
            logger.info(
                "--> lcp lag observed after [{}] snapshot(s), index has [{}] docs",
                lagObserved ? snapshotsTaken : "none of " + numSnapshots,
                docCount
            );
            Files.writeString(metaFile, String.valueOf(snapshotsTaken));

            // Delete the live index so no 7.x index remains in the cluster state during upgrade.
            deleteIndex(index);
            return;
        }

        if (isFullyUpgradedTo(VERSION_CURRENT)) {
            assumeTrue("no snapshot metadata found from N-2 phase", Files.exists(metaFile));
            int snapshotsTaken = Integer.parseInt(Files.readString(metaFile));

            // Fewer snapshots than the max means the loop stopped early on lag detection.
            boolean lagObserved = snapshotsTaken > 0 && snapshotsTaken < numSnapshots;
            assumeTrue("no naturally lagged snapshot observed during N-2 indexing", lagObserved);

            int lastSnap = snapshotsTaken - 1;
            var restoredIndex = suffix("index-restored");
            logger.debug("--> restoring lagged snapshot [snap-{}] as [{}]", lastSnap, restoredIndex);
            restoreIndex(repository, "snap-" + lastSnap, index, restoredIndex);
            ensureGreen(restoredIndex);

            long[] seqNo = readPrimaryShardSeqNoStats(restoredIndex);
            assertThat(
                "restored index [" + restoredIndex + "] must have local_checkpoint == max_seq_no after read-only restore",
                seqNo[1],
                equalTo(seqNo[0])
            );

            logger.debug("--> deleting restored index [{}]", restoredIndex);
            deleteIndex(restoredIndex);
        }
    }

    /** Returns {@code [max_seq_no, local_checkpoint]} from the primary shard of {@code indexName}. */
    private long[] readPrimaryShardSeqNoStats(String indexName) throws IOException {
        var request = new Request("GET", "/" + indexName + "/_stats");
        request.addParameter("level", "shards");
        var path = ObjectPath.createFromResponse(client().performRequest(request));
        Number msn = path.evaluate("indices." + indexName + ".shards.0.0.seq_no.max_seq_no");
        Number lcp = path.evaluate("indices." + indexName + ".shards.0.0.seq_no.local_checkpoint");
        assertNotNull("max_seq_no must not be null for [" + indexName + "]", msn);
        assertNotNull("local_checkpoint must not be null for [" + indexName + "]", lcp);
        return new long[] { msn.longValue(), lcp.longValue() };
    }
}
