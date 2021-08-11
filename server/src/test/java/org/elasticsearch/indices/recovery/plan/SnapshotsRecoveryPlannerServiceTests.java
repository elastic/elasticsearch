/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery.plan;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NoMergeScheduler;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardSnapshotInfo;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.common.util.CollectionUtils.iterableAsArrayList;
import static org.elasticsearch.index.engine.Engine.HISTORY_UUID_KEY;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SnapshotsRecoveryPlannerServiceTests extends ESTestCase {
    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings("index",
        Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, org.elasticsearch.Version.CURRENT).build());
    private static final ByteSizeValue PART_SIZE = new ByteSizeValue(Long.MAX_VALUE);
    private static final ShardId shardId = new ShardId(INDEX_SETTINGS.getIndex(), 1);

    private String shardHistoryUUID;
    private final AtomicLong clock = new AtomicLong();

    @Before
    public void setUpHistoryUUID() {
        shardHistoryUUID = UUIDs.randomBase64UUID();
    }

    public void testOnlyUsesSourceFilesWhenUseSnapshotsFlagIsFalse() throws Exception {
        createStore(store -> {
            Store.MetadataSnapshot targetMetadataSnapshot = generateRandomTargetState(store);

            writeRandomDocs(store, randomIntBetween(10, 100));
            Store.MetadataSnapshot sourceMetadata = store.getMetadata(null);

            long startingSeqNo = randomNonNegativeLong();
            int translogOps = randomIntBetween(1, 100);

            ShardRecoveryPlan shardRecoveryPlan = computeShardRecoveryPlan(
                randomBoolean() ? randomAlphaOfLength(10) : null,
                sourceMetadata,
                targetMetadataSnapshot,
                startingSeqNo,
                translogOps,
                new ShardSnapshotsService(null, null, null, null) {
                    @Override
                    public void fetchLatestSnapshotsForShard(ShardId shardId, ActionListener<Optional<ShardSnapshot>> listener) {
                        assert false: "Unexpected call";
                    }
                },
                false
            );
            assertPlanIsValid(shardRecoveryPlan, sourceMetadata);
            assertAllSourceFilesAreAvailableInSource(shardRecoveryPlan, sourceMetadata);
            assertAllIdenticalFilesAreAvailableInTarget(shardRecoveryPlan, targetMetadataSnapshot);
            assertThat(shardRecoveryPlan.getSnapshotFilesToRecover(), is(equalTo(ShardRecoveryPlan.SnapshotFilesToRecover.EMPTY)));

            assertThat(shardRecoveryPlan.getStartingSeqNo(), equalTo(startingSeqNo));
            assertThat(shardRecoveryPlan.getTranslogOps(), equalTo(translogOps));
        });
    }

    public void testFallbacksToRegularPlanIfThereAreNotAvailableSnapshotsOrThereIsAFailureDuringFetch() throws Exception {
        createStore(store -> {
            Store.MetadataSnapshot targetMetadataSnapshot = generateRandomTargetState(store);

            writeRandomDocs(store, randomIntBetween(10, 100));
            final Store.MetadataSnapshot sourceMetadata = store.getMetadata(null);

            long startingSeqNo = randomNonNegativeLong();
            int translogOps = randomIntBetween(1, 100);
            ShardRecoveryPlan shardRecoveryPlan = computeShardRecoveryPlan(
                null,
                sourceMetadata,
                targetMetadataSnapshot,
                startingSeqNo,
                translogOps,
                new ShardSnapshotsService(null, null, null, null) {
                    @Override
                    public void fetchLatestSnapshotsForShard(ShardId shardId, ActionListener<Optional<ShardSnapshot>> listener) {
                        if (randomBoolean()) {
                            listener.onResponse(Optional.empty());
                        } else {
                            listener.onFailure(new IOException("Boom!"));
                        }
                    }
                },
                true
            );

            assertPlanIsValid(shardRecoveryPlan, sourceMetadata);
            assertAllSourceFilesAreAvailableInSource(shardRecoveryPlan, sourceMetadata);
            assertAllIdenticalFilesAreAvailableInTarget(shardRecoveryPlan, targetMetadataSnapshot);
            assertThat(shardRecoveryPlan.getSnapshotFilesToRecover(), is(equalTo(ShardRecoveryPlan.SnapshotFilesToRecover.EMPTY)));

            assertThat(shardRecoveryPlan.getStartingSeqNo(), equalTo(startingSeqNo));
            assertThat(shardRecoveryPlan.getTranslogOps(), equalTo(translogOps));
        });
    }

    public void testLogicallyEquivalentSnapshotIsUsed() throws Exception {
        createStore(store -> {
            Store.MetadataSnapshot targetSourceMetadata = generateRandomTargetState(store);

            writeRandomDocs(store, randomIntBetween(10, 100));
            Store.MetadataSnapshot sourceMetadata = store.getMetadata(null);

            ShardSnapshot shardSnapshotData = createShardSnapshotThatSharesSegmentFiles(store, "repo");
            // The shardStateIdentifier is shared with the latest snapshot,
            // meaning that the current shard and the snapshot are logically equivalent
            String shardStateIdentifier = shardSnapshotData.getShardStateIdentifier();

            long startingSeqNo = randomNonNegativeLong();
            int translogOps = randomIntBetween(1, 100);
            ShardRecoveryPlan shardRecoveryPlan = computeShardRecoveryPlan(
                shardStateIdentifier,
                sourceMetadata,
                targetSourceMetadata,
                startingSeqNo,
                translogOps,
                new ShardSnapshotsService(null, null, null, null) {
                    @Override
                    public void fetchLatestSnapshotsForShard(ShardId shardId, ActionListener<Optional<ShardSnapshot>> listener) {
                        listener.onResponse(Optional.of(shardSnapshotData));
                    }
                },
                true
            );

            assertPlanIsValid(shardRecoveryPlan, sourceMetadata);
            assertAllSourceFilesAreAvailableInSource(shardRecoveryPlan, sourceMetadata);
            assertAllIdenticalFilesAreAvailableInTarget(shardRecoveryPlan, targetSourceMetadata);
            assertUsesExpectedSnapshot(shardRecoveryPlan, shardSnapshotData);

            assertThat(shardRecoveryPlan.getStartingSeqNo(), equalTo(startingSeqNo));
            assertThat(shardRecoveryPlan.getTranslogOps(), equalTo(translogOps));
        });
    }

    public void testLogicallyEquivalentSnapshotIsSkippedIfUnderlyingFilesAreDifferent() throws Exception {
        createStore(store -> {
            Store.MetadataSnapshot targetSourceMetadata = generateRandomTargetState(store);

            writeRandomDocs(store, randomIntBetween(10, 100));
            Store.MetadataSnapshot sourceMetadata = store.getMetadata(null);

            // The snapshot shardStateIdentifier is the same as the source, but the files are different.
            // This can happen after a primary fail-over.
            ShardSnapshot shardSnapshotData = createShardSnapshotThatDoNotShareSegmentFiles("repo");
            String shardStateIdentifier = shardSnapshotData.getShardStateIdentifier();

            long startingSeqNo = randomNonNegativeLong();
            int translogOps = randomIntBetween(1, 100);
            ShardRecoveryPlan shardRecoveryPlan = computeShardRecoveryPlan(
                shardStateIdentifier,
                sourceMetadata,
                targetSourceMetadata,
                startingSeqNo,
                translogOps,
                new ShardSnapshotsService(null, null, null, null) {
                    @Override
                    public void fetchLatestSnapshotsForShard(ShardId shardId, ActionListener<Optional<ShardSnapshot>> listener) {
                        listener.onResponse(Optional.of(shardSnapshotData));
                    }
                },
                true
            );

            assertPlanIsValid(shardRecoveryPlan, sourceMetadata);
            assertAllSourceFilesAreAvailableInSource(shardRecoveryPlan, sourceMetadata);
            assertAllIdenticalFilesAreAvailableInTarget(shardRecoveryPlan, targetSourceMetadata);
            assertThat(shardRecoveryPlan.getSnapshotFilesToRecover(), is(equalTo(ShardRecoveryPlan.SnapshotFilesToRecover.EMPTY)));

            assertThat(shardRecoveryPlan.getStartingSeqNo(), equalTo(startingSeqNo));
            assertThat(shardRecoveryPlan.getTranslogOps(), equalTo(translogOps));
        });
    }

    public void testPlannerTriesToUseMostFilesFromSnapshots() throws Exception {
        createStore(store -> {
            Store.MetadataSnapshot targetMetadataSnapshot = generateRandomTargetState(store);

            List<ShardSnapshot> availableSnapshots = new ArrayList<>();

            int numberOfStaleSnapshots = randomIntBetween(0, 5);
            for (int i = 0; i < numberOfStaleSnapshots; i++) {
                availableSnapshots.add(createShardSnapshotThatDoNotShareSegmentFiles("stale-repo-" + i));
            }

            int numberOfValidSnapshots = randomIntBetween(0, 10);
            for (int i = 0; i < numberOfValidSnapshots; i++) {
                writeRandomDocs(store, randomIntBetween(10, 100));
                availableSnapshots.add(createShardSnapshotThatSharesSegmentFiles(store, "repo-" + i));
            }

            // Write new segments
            writeRandomDocs(store, randomIntBetween(20, 50));
            Store.MetadataSnapshot latestSourceMetadata = store.getMetadata(null);
            String latestShardIdentifier = randomAlphaOfLength(10);

            long startingSeqNo = randomNonNegativeLong();
            int translogOps = randomIntBetween(0, 100);
            ShardRecoveryPlan shardRecoveryPlan = computeShardRecoveryPlan(
                latestShardIdentifier,
                latestSourceMetadata,
                targetMetadataSnapshot,
                startingSeqNo,
                translogOps,
                new ShardSnapshotsService(null, null, null, null) {
                    @Override
                    public void fetchLatestSnapshotsForShard(ShardId shardId, ActionListener<Optional<ShardSnapshot>> listener) {
                        listener.onResponse(Optional.of(availableSnapshots.get(availableSnapshots.size() - 1)));
                    }
                },
                true
            );

            assertPlanIsValid(shardRecoveryPlan, latestSourceMetadata);
            assertAllSourceFilesAreAvailableInSource(shardRecoveryPlan, latestSourceMetadata);
            assertAllIdenticalFilesAreAvailableInTarget(shardRecoveryPlan, targetMetadataSnapshot);

            if (numberOfValidSnapshots > 0) {
                ShardSnapshot latestValidSnapshot =
                    availableSnapshots.get(availableSnapshots.size() - 1);
                assertUsesExpectedSnapshot(shardRecoveryPlan, latestValidSnapshot);
            } else {
                assertThat(shardRecoveryPlan.getSnapshotFilesToRecover(), is(equalTo(ShardRecoveryPlan.SnapshotFilesToRecover.EMPTY)));
            }

            assertThat(shardRecoveryPlan.getStartingSeqNo(), equalTo(startingSeqNo));
            assertThat(shardRecoveryPlan.getTranslogOps(), equalTo(translogOps));
        });
    }

    public void testSnapshotsWithADifferentHistoryUUIDAreUsedIfFilesAreShared() throws Exception {
        createStore(store -> {
            Store.MetadataSnapshot targetMetadataSnapshot = generateRandomTargetState(store);

            List<ShardSnapshot> availableSnapshots = new ArrayList<>();
            int numberOfValidSnapshots = randomIntBetween(1, 4);
            for (int i = 0; i < numberOfValidSnapshots; i++) {
                writeRandomDocs(store, randomIntBetween(10, 100));
                availableSnapshots.add(createShardSnapshotThatSharesSegmentFiles(store, "repo-" + i));
            }

            // Simulate a restore/stale primary allocation
            shardHistoryUUID = UUIDs.randomBase64UUID();
            String latestShardIdentifier = randomAlphaOfLength(10);
            // Write new segments
            writeRandomDocs(store, randomIntBetween(20, 50));
            Store.MetadataSnapshot latestSourceMetadata = store.getMetadata(null);

            long startingSeqNo = randomNonNegativeLong();
            int translogOps = randomIntBetween(0, 100);
            ShardRecoveryPlan shardRecoveryPlan = computeShardRecoveryPlan(
                latestShardIdentifier,
                latestSourceMetadata,
                targetMetadataSnapshot,
                startingSeqNo,
                translogOps,
                new ShardSnapshotsService(null, null, null, null) {
                    @Override
                    public void fetchLatestSnapshotsForShard(ShardId shardId, ActionListener<Optional<ShardSnapshot>> listener) {
                        listener.onResponse(Optional.of(availableSnapshots.get(availableSnapshots.size() - 1)));
                    }
                },
                true
            );

            assertPlanIsValid(shardRecoveryPlan, latestSourceMetadata);
            assertAllSourceFilesAreAvailableInSource(shardRecoveryPlan, latestSourceMetadata);
            assertAllIdenticalFilesAreAvailableInTarget(shardRecoveryPlan, targetMetadataSnapshot);
            assertUsesExpectedSnapshot(shardRecoveryPlan, availableSnapshots.get(availableSnapshots.size() - 1));

            assertThat(shardRecoveryPlan.getStartingSeqNo(), equalTo(startingSeqNo));
            assertThat(shardRecoveryPlan.getTranslogOps(), equalTo(translogOps));
        });
    }

    public void testFallbacksToSourceOnlyPlanIfTargetNodeIsInUnsupportedVersion() throws Exception {
        createStore(store -> {
            Store.MetadataSnapshot targetMetadataSnapshot = generateRandomTargetState(store);

            writeRandomDocs(store, randomIntBetween(10, 100));
            ShardSnapshot shardSnapshot = createShardSnapshotThatSharesSegmentFiles(store, "repo");

            Store.MetadataSnapshot sourceMetadata = store.getMetadata(null);

            long startingSeqNo = randomNonNegativeLong();
            int translogOps = randomIntBetween(0, 100);
            ShardRecoveryPlan shardRecoveryPlan = computeShardRecoveryPlan(
                "shard-id",
                sourceMetadata,
                targetMetadataSnapshot,
                startingSeqNo,
                translogOps,
                new ShardSnapshotsService(null, null, null, null) {
                    @Override
                    public void fetchLatestSnapshotsForShard(ShardId shardId, ActionListener<Optional<ShardSnapshot>> listener) {
                        listener.onResponse(Optional.of(shardSnapshot));
                    }
                },
                true,
                Version.V_7_14_0 // Unsupported version
            );

            assertPlanIsValid(shardRecoveryPlan, sourceMetadata);
            assertAllSourceFilesAreAvailableInSource(shardRecoveryPlan, sourceMetadata);
            assertAllIdenticalFilesAreAvailableInTarget(shardRecoveryPlan, targetMetadataSnapshot);
            assertThat(shardRecoveryPlan.getSnapshotFilesToRecover(), is(equalTo(ShardRecoveryPlan.SnapshotFilesToRecover.EMPTY)));

            assertThat(shardRecoveryPlan.getStartingSeqNo(), equalTo(startingSeqNo));
            assertThat(shardRecoveryPlan.getTranslogOps(), equalTo(translogOps));
        });
    }

    private ShardRecoveryPlan computeShardRecoveryPlan(String shardIdentifier,
                                                       Store.MetadataSnapshot sourceMetadataSnapshot,
                                                       Store.MetadataSnapshot targetMetadataSnapshot,
                                                       long startingSeqNo,
                                                       int translogOps,
                                                       ShardSnapshotsService shardSnapshotsService,
                                                       boolean snapshotRecoveriesEnabled) throws Exception {
        return computeShardRecoveryPlan(shardIdentifier,
            sourceMetadataSnapshot,
            targetMetadataSnapshot,
            startingSeqNo,
            translogOps,
            shardSnapshotsService,
            snapshotRecoveriesEnabled,
            Version.CURRENT
        );
    }

    private ShardRecoveryPlan computeShardRecoveryPlan(String shardIdentifier,
                                                       Store.MetadataSnapshot sourceMetadataSnapshot,
                                                       Store.MetadataSnapshot targetMetadataSnapshot,
                                                       long startingSeqNo,
                                                       int translogOps,
                                                       ShardSnapshotsService shardSnapshotsService,
                                                       boolean snapshotRecoveriesEnabled,
                                                       Version version) throws Exception {
        SnapshotsRecoveryPlannerService recoveryPlannerService =
            new SnapshotsRecoveryPlannerService(shardSnapshotsService);

        PlainActionFuture<ShardRecoveryPlan> planFuture = PlainActionFuture.newFuture();
        recoveryPlannerService.computeRecoveryPlan(shardId,
            sourceMetadataSnapshot,
            targetMetadataSnapshot,
            startingSeqNo,
            translogOps,
            version,
            snapshotRecoveriesEnabled,
            planFuture
        );
        final ShardRecoveryPlan shardRecoveryPlan = planFuture.get();
        assertThat(shardRecoveryPlan, notNullValue());
        return shardRecoveryPlan;
    }

    private void assertPlanIsValid(ShardRecoveryPlan shardRecoveryPlan,
                                   Store.MetadataSnapshot expectedMetadataSnapshot) {
        List<StoreFileMetadata> planFiles = new ArrayList<>();
        planFiles.addAll(shardRecoveryPlan.getFilesPresentInTarget());
        planFiles.addAll(shardRecoveryPlan.getSourceFilesToRecover());
        for (BlobStoreIndexShardSnapshot.FileInfo fileInfo : shardRecoveryPlan.getSnapshotFilesToRecover()) {
            planFiles.add(fileInfo.metadata());
        }

        final ArrayList<StoreFileMetadata> storeFileMetadata = iterableAsArrayList(expectedMetadataSnapshot);
        List<StoreFileMetadata> missingFiles = storeFileMetadata.stream()
            .filter(f -> containsFile(planFiles, f) == false)
            .collect(Collectors.toList());

        List<StoreFileMetadata> unexpectedFiles = planFiles.stream()
            .filter(f -> containsFile(storeFileMetadata, f) == false)
            .collect(Collectors.toList());

        assertThat(missingFiles, is(empty()));
        assertThat(unexpectedFiles, is(empty()));
        assertThat(planFiles.size(), is(equalTo(storeFileMetadata.size())));
        Store.MetadataSnapshot sourceMetadataSnapshot = shardRecoveryPlan.getSourceMetadataSnapshot();
        assertThat(sourceMetadataSnapshot.size(), equalTo(expectedMetadataSnapshot.size()));
        assertThat(sourceMetadataSnapshot.getHistoryUUID(), equalTo(expectedMetadataSnapshot.getHistoryUUID()));
    }

    private void assertAllSourceFilesAreAvailableInSource(ShardRecoveryPlan shardRecoveryPlan,
                                                          Store.MetadataSnapshot sourceMetadataSnapshot) {
        for (StoreFileMetadata sourceFile : shardRecoveryPlan.getSourceFilesToRecover()) {
            final StoreFileMetadata actual = sourceMetadataSnapshot.get(sourceFile.name());
            assertThat(actual, is(notNullValue()));
            assertThat(actual.isSame(sourceFile), is(equalTo(true)));
        }
    }

    private void assertAllIdenticalFilesAreAvailableInTarget(ShardRecoveryPlan shardRecoveryPlan,
                                                             Store.MetadataSnapshot targetMetadataSnapshot) {
        for (StoreFileMetadata identicalFile : shardRecoveryPlan.getFilesPresentInTarget()) {
            final StoreFileMetadata targetFile = targetMetadataSnapshot.get(identicalFile.name());
            assertThat(targetFile, notNullValue());
            assertThat(targetFile.isSame(identicalFile), is(equalTo(true)));
        }
    }

    private void assertUsesExpectedSnapshot(ShardRecoveryPlan shardRecoveryPlan,
                                            ShardSnapshot expectedSnapshotToUse) {
        assertThat(shardRecoveryPlan.getSnapshotFilesToRecover().getIndexId(), equalTo(expectedSnapshotToUse.getIndexId()));
        assertThat(shardRecoveryPlan.getSnapshotFilesToRecover().getRepository(), equalTo(expectedSnapshotToUse.getRepository()));

        final Store.MetadataSnapshot shardSnapshotMetadataSnapshot = expectedSnapshotToUse.getMetadataSnapshot();
        for (BlobStoreIndexShardSnapshot.FileInfo fileInfo : shardRecoveryPlan.getSnapshotFilesToRecover()) {
            final StoreFileMetadata snapshotFile = shardSnapshotMetadataSnapshot.get(fileInfo.metadata().name());
            assertThat(snapshotFile, is(notNullValue()));
            assertThat(snapshotFile.isSame(fileInfo.metadata()), is(equalTo(true)));
        }
    }

    // StoreFileMetadata doesn't implement #equals, we rely on StoreFileMetadata#isSame for equality checks
    private boolean containsFile(List<StoreFileMetadata> files, StoreFileMetadata fileMetadata) {
        for (StoreFileMetadata file : files) {
            if (file.isSame(fileMetadata)) {
                return true;
            }
        }
        return false;
    }

    private void createStore(CheckedConsumer<Store, Exception> testBody) throws Exception {
        BaseDirectoryWrapper baseDirectoryWrapper = newFSDirectory(createTempDir());
        Store store = new Store(shardId, INDEX_SETTINGS, baseDirectoryWrapper, new DummyShardLock(shardId));
        try {
            testBody.accept(store);
        } finally {
            IOUtils.close(store);
        }
    }

    private Store.MetadataSnapshot generateRandomTargetState(Store store) throws IOException {
        final Store.MetadataSnapshot targetMetadataSnapshot;
        if (randomBoolean()) {
            // The target can share some files with the source
            writeRandomDocs(store, randomIntBetween(20, 50));
            targetMetadataSnapshot = store.getMetadata(null);
        } else {
            if (randomBoolean()) {
                targetMetadataSnapshot = Store.MetadataSnapshot.EMPTY;
            } else {
                // None of the files in the target would match
                final int filesInTargetCount = randomIntBetween(1, 20);
                Map<String, StoreFileMetadata> filesInTarget = IntStream.range(0, filesInTargetCount)
                    .mapToObj(i -> randomStoreFileMetadata())
                    .collect(Collectors.toMap(StoreFileMetadata::name, Function.identity()));
                targetMetadataSnapshot = new Store.MetadataSnapshot(filesInTarget, Collections.emptyMap(), 0);
            }
        }
        return targetMetadataSnapshot;
    }

    private void writeRandomDocs(Store store, int numDocs) throws IOException {
        Directory dir = store.directory();

        // Disable merges to control the files that are used in this tests
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig()
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setMergeScheduler(NoMergeScheduler.INSTANCE);
        IndexWriter writer = new IndexWriter(dir, indexWriterConfig);
        for (int i = 0; i < numDocs; i++) {
            Document document = new Document();
            document.add(new StringField("id", Integer.toString(i), Field.Store.YES));
            document.add(newField("field", randomUnicodeOfCodepointLengthBetween(1, 10), TextField.TYPE_STORED));
            writer.addDocument(document);
        }
        Map<String, String> userData = new HashMap<>();
        userData.put(HISTORY_UUID_KEY, shardHistoryUUID);
        writer.setLiveCommitData(userData.entrySet());
        writer.commit();
        writer.close();
    }

    private ShardSnapshot createShardSnapshotThatDoNotShareSegmentFiles(String repoName) {
        List<BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles = randomList(randomIntBetween(10, 20), () -> {
            StoreFileMetadata storeFileMetadata = randomStoreFileMetadata();
            return new BlobStoreIndexShardSnapshot.FileInfo(randomAlphaOfLength(10), storeFileMetadata, PART_SIZE);
        });

        return createShardSnapshot(repoName, snapshotFiles);
    }

    private ShardSnapshot createShardSnapshotThatSharesSegmentFiles(Store store,
                                                                    String repository) throws Exception {
        Store.MetadataSnapshot sourceMetadata = store.getMetadata(null);
        assertThat(sourceMetadata.size(), is(greaterThan(1)));

        List<BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles = new ArrayList<>(sourceMetadata.size());
        for (StoreFileMetadata storeFileMetadata : sourceMetadata) {
            BlobStoreIndexShardSnapshot.FileInfo fileInfo =
                new BlobStoreIndexShardSnapshot.FileInfo(randomAlphaOfLength(10), storeFileMetadata, PART_SIZE);
            snapshotFiles.add(fileInfo);
        }
        return createShardSnapshot(repository, snapshotFiles);
    }

    private ShardSnapshot createShardSnapshot(String repoName,
                                              List<BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles) {
        String shardIdentifier = randomAlphaOfLength(10);

        Snapshot snapshot = new Snapshot(repoName, new SnapshotId("snap", UUIDs.randomBase64UUID(random())));
        IndexId indexId = randomIndexId();
        ShardSnapshotInfo shardSnapshotInfo =
            new ShardSnapshotInfo(indexId, shardId, snapshot, randomAlphaOfLength(10), shardIdentifier, clock.incrementAndGet());

        return new ShardSnapshot(shardSnapshotInfo, snapshotFiles);
    }

    private StoreFileMetadata randomStoreFileMetadata() {
        return new StoreFileMetadata(randomAlphaOfLength(10), randomLongBetween(1, 100),
            randomAlphaOfLength(10), Version.CURRENT.toString());
    }

    private IndexId randomIndexId() {
        return new IndexId(shardId.getIndexName(), randomAlphaOfLength(10));
    }
}
