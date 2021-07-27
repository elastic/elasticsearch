/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

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

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.common.util.CollectionUtils.iterableAsArrayList;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class ShardRecoveryPlannerTests extends ESTestCase {
    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings("index",
        Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, org.elasticsearch.Version.CURRENT).build());
    private static final ByteSizeValue PART_SIZE = new ByteSizeValue(Long.MAX_VALUE);
    private static final ShardId shardId = new ShardId(INDEX_SETTINGS.getIndex(), 1);

    public void testOnlyUsesSourceFilesWhenUseSnapshotsFlagIsFalse() throws Exception {
        createStore(store -> {
            Store.MetadataSnapshot targetMetadataSnapshot = generateRandomTargetState(store, true);

            writeRandomDocs(store, randomIntBetween(10, 100));
            Store.MetadataSnapshot sourceMetadata = store.getMetadata(null);

            long startingSeqNo = randomNonNegativeLong();
            int translogOps = randomIntBetween(1, 100);
            final ShardRecoveryPlanner shardRecoveryPlanner = new ShardRecoveryPlanner(shardId,
                randomBoolean() ? randomAlphaOfLength(10) : null,
                sourceMetadata,
                targetMetadataSnapshot,
                startingSeqNo,
                translogOps,
                new ShardSnapshotsService(null, null, null) {
                    @Override
                    protected void fetchAvailableSnapshots(ShardId shardId, ActionListener<List<ShardSnapshotData>> listener) {
                        assert false: "Unexpected call";
                    }
                },
                false
            );

            final ShardRecoveryPlan shardRecoveryPlan = computeShardRecoveryPlan(shardRecoveryPlanner);
            assertPlanIsValid(shardRecoveryPlan, sourceMetadata);

            final Store.RecoveryDiff recoveryDiff = sourceMetadata.recoveryDiff(targetMetadataSnapshot);
            assertThat(shardRecoveryPlan.getSourceFilesToRecover(), is(not(empty())));
            assertThat(shardRecoveryPlan.getSourceFilesToRecover(), is(equalTo(recoveryDiff.missingAndDifferent)));

            assertThat(shardRecoveryPlan.getIdenticalFiles(), is(equalTo(recoveryDiff.identical)));

            assertThat(shardRecoveryPlan.getSnapshotFilesToRecover(), is(equalTo(ShardRecoveryPlan.SnapshotFilesToRecover.EMPTY)));

            assertThat(shardRecoveryPlan.getMetadataSnapshot(), equalTo(sourceMetadata));

            assertThat(shardRecoveryPlan.getStartingSeqNo(), equalTo(startingSeqNo));
            assertThat(shardRecoveryPlan.getTranslogOps(), equalTo(translogOps));
        });
    }

    public void testFallbacksToRegularPlanIfThereAreNotAvailableSnapshotsOrThereIsAFailureDuringFetch() throws Exception {
        createStore(store -> {
            Store.MetadataSnapshot targetMetadataSnapshot = generateRandomTargetState(store, true);

            writeRandomDocs(store, randomIntBetween(10, 100));
            final Store.MetadataSnapshot sourceMetadata = store.getMetadata(null);

            long startingSeqNo = randomNonNegativeLong();
            int translogOps = randomIntBetween(1, 100);
            final ShardRecoveryPlanner shardRecoveryPlanner = new ShardRecoveryPlanner(shardId,
                null,
                sourceMetadata,
                targetMetadataSnapshot,
                startingSeqNo,
                translogOps,
                new ShardSnapshotsService(null, null, null) {
                    @Override
                    protected void fetchAvailableSnapshots(ShardId shardId, ActionListener<List<ShardSnapshotData>> listener) {
                        if (randomBoolean()) {
                            listener.onResponse(Collections.emptyList());
                        } else {
                            listener.onFailure(new IOException("Boom!"));
                        }
                    }
                },
                true
            );

            final ShardRecoveryPlan shardRecoveryPlan = computeShardRecoveryPlan(shardRecoveryPlanner);
            assertPlanIsValid(shardRecoveryPlan, sourceMetadata);

            final Store.RecoveryDiff recoveryDiff = sourceMetadata.recoveryDiff(targetMetadataSnapshot);
            assertThat(shardRecoveryPlan.getSourceFilesToRecover(), is(not(empty())));
            assertThat(shardRecoveryPlan.getSourceFilesToRecover(), is(equalTo(recoveryDiff.missingAndDifferent)));

            assertThat(shardRecoveryPlan.getIdenticalFiles(), is(equalTo(recoveryDiff.identical)));

            assertThat(shardRecoveryPlan.getSnapshotFilesToRecover(), is(equalTo(ShardRecoveryPlan.SnapshotFilesToRecover.EMPTY)));

            assertThat(shardRecoveryPlan.getMetadataSnapshot(), equalTo(sourceMetadata));

            assertThat(shardRecoveryPlan.getStartingSeqNo(), equalTo(startingSeqNo));
            assertThat(shardRecoveryPlan.getTranslogOps(), equalTo(translogOps));
        });
    }

    public void testLogicallyEquivalentSnapshotIsUsed() throws Exception {
        createStore(store -> {
            writeRandomDocs(store, randomIntBetween(10, 100));
            Store.MetadataSnapshot sourceMetadata = store.getMetadata(null);

            ShardSnapshotsService.ShardSnapshotData shardSnapshotData = createSnapshotThatSharesSegmentFiles(store, "repo");
            String shardStateIdentifier = shardSnapshotData.getShardStateIdentifier();

            long startingSeqNo = randomNonNegativeLong();
            int translogOps = randomIntBetween(1, 100);
            ShardRecoveryPlanner shardRecoveryPlanner = new ShardRecoveryPlanner(shardId,
                shardStateIdentifier,
                sourceMetadata,
                Store.MetadataSnapshot.EMPTY,
                startingSeqNo,
                translogOps,
                new ShardSnapshotsService(null, null, null) {
                    @Override
                    protected void fetchAvailableSnapshots(ShardId shardId, ActionListener<List<ShardSnapshotData>> listener) {
                        listener.onResponse(Collections.singletonList(shardSnapshotData));
                    }
                },
                true
            );

            final ShardRecoveryPlan shardRecoveryPlan = computeShardRecoveryPlan(shardRecoveryPlanner);
            assertPlanIsValid(shardRecoveryPlan, sourceMetadata);

            final Store.RecoveryDiff recoveryDiff = sourceMetadata.recoveryDiff(shardSnapshotData.getMetadataSnapshot());

            ShardRecoveryPlan.SnapshotFilesToRecover expectedSnapshotFilesToRecover =
                new ShardRecoveryPlan.SnapshotFilesToRecover(shardSnapshotData.getIndexId(),
                    shardSnapshotData.getSnapshotFiles(recoveryDiff.identical));

            assertThat(shardRecoveryPlan.getSourceFilesToRecover(), is(empty()));
            assertEqualSnapshotFiles(expectedSnapshotFilesToRecover, shardRecoveryPlan.getSnapshotFilesToRecover());

            assertThat(shardRecoveryPlan.getIdenticalFiles(), is(empty()));
            assertThat(shardRecoveryPlan.getExistingSize(), equalTo(0L));

            assertThat(shardRecoveryPlan.getMetadataSnapshot(), equalTo(sourceMetadata));

            assertThat(shardRecoveryPlan.getStartingSeqNo(), equalTo(startingSeqNo));
            assertThat(shardRecoveryPlan.getTranslogOps(), equalTo(translogOps));
        });
    }

    public void testLogicallyEquivalentSnapshotIsSkippedIfUnderlyingFilesAreDifferent() throws Exception {
        createStore(store -> {
            writeRandomDocs(store, randomIntBetween(10, 100));
            Store.MetadataSnapshot sourceMetadata = store.getMetadata(null);

            ShardSnapshotsService.ShardSnapshotData shardSnapshotData = createSnapshotThatDoNotShareSegmentFiles("repo");
            String shardStateIdentifier = shardSnapshotData.getShardStateIdentifier();

            long startingSeqNo = randomNonNegativeLong();
            int translogOps = randomIntBetween(1, 100);
            ShardRecoveryPlanner shardRecoveryPlanner = new ShardRecoveryPlanner(shardId,
                shardStateIdentifier,
                sourceMetadata,
                Store.MetadataSnapshot.EMPTY,
                startingSeqNo,
                translogOps,
                new ShardSnapshotsService(null, null, null) {
                    @Override
                    protected void fetchAvailableSnapshots(ShardId shardId, ActionListener<List<ShardSnapshotData>> listener) {
                        listener.onResponse(Collections.singletonList(shardSnapshotData));
                    }
                },
                true
            );

            final ShardRecoveryPlan shardRecoveryPlan = computeShardRecoveryPlan(shardRecoveryPlanner);
            assertPlanIsValid(shardRecoveryPlan, sourceMetadata);

            assertThat(shardRecoveryPlan.getSourceFilesToRecover(), is(not(empty())));
            assertThat(shardRecoveryPlan.getSnapshotFilesToRecover(), is(equalTo(ShardRecoveryPlan.SnapshotFilesToRecover.EMPTY)));
            assertThat(shardRecoveryPlan.getIdenticalFiles(), is(empty()));

            assertThat(shardRecoveryPlan.getMetadataSnapshot(), equalTo(sourceMetadata));

            assertThat(shardRecoveryPlan.getStartingSeqNo(), equalTo(startingSeqNo));
            assertThat(shardRecoveryPlan.getTranslogOps(), equalTo(translogOps));
        });
    }

    public void testPlannerTriesToUseMostFilesFromSnapshots() throws Exception {
        createStore(store -> {
            Store.MetadataSnapshot targetMetadataSnapshot = generateRandomTargetState(store, true);

            List<ShardSnapshotsService.ShardSnapshotData> availableSnapshots = new ArrayList<>();

            int numberOfStaleSnapshots = randomIntBetween(0, 5);
            for (int i = 0; i < numberOfStaleSnapshots; i++) {
                availableSnapshots.add(createSnapshotThatDoNotShareSegmentFiles("stale-repo-" + i));
            }

            int numberOfValidSnapshots = randomIntBetween(0, 10);
            for (int i = 0; i < numberOfValidSnapshots; i++) {
                writeRandomDocs(store, randomIntBetween(10, 100));
                availableSnapshots.add(createSnapshotThatSharesSegmentFiles(store, "repo-" + i));
            }

            // Write new segments
            writeRandomDocs(store, randomIntBetween(20, 50));
            Store.MetadataSnapshot latestSourceMetadata = store.getMetadata(null);
            String latestShardIdentifier = randomAlphaOfLength(10);

            long startingSeqNo = randomNonNegativeLong();
            int translogOps = randomIntBetween(0, 100);
            ShardRecoveryPlanner shardRecoveryPlanner = new ShardRecoveryPlanner(shardId,
                latestShardIdentifier,
                latestSourceMetadata,
                targetMetadataSnapshot,
                startingSeqNo,
                translogOps,
                new ShardSnapshotsService(null, null, null) {
                    @Override
                    protected void fetchAvailableSnapshots(ShardId shardId, ActionListener<List<ShardSnapshotData>> listener) {
                        listener.onResponse(availableSnapshots);
                    }
                },
                true
            );

            final ShardRecoveryPlan shardRecoveryPlan = computeShardRecoveryPlan(shardRecoveryPlanner);
            assertPlanIsValid(shardRecoveryPlan, latestSourceMetadata);

            if (numberOfValidSnapshots > 0) {
                ShardSnapshotsService.ShardSnapshotData latestValidSnapshot =
                    availableSnapshots.get(availableSnapshots.size() - 1);

                final Store.RecoveryDiff recoveryDiff = latestSourceMetadata.recoveryDiff(latestValidSnapshot.getMetadataSnapshot());

                ShardRecoveryPlan.SnapshotFilesToRecover expectedSnapshotFilesToRecover =
                    new ShardRecoveryPlan.SnapshotFilesToRecover(latestValidSnapshot.getIndexId(),
                        latestValidSnapshot.getSnapshotFiles(recoveryDiff.identical));

                assertThat(shardRecoveryPlan.getSourceFilesToRecover(), is(equalTo(recoveryDiff.missingAndDifferent)));
                assertEqualSnapshotFiles(expectedSnapshotFilesToRecover, shardRecoveryPlan.getSnapshotFilesToRecover());
            } else {
                List<StoreFileMetadata> sourceFiles = iterableAsArrayList(latestSourceMetadata);
                assertThat(shardRecoveryPlan.getSourceFilesToRecover(), is(equalTo(sourceFiles)));
                assertEqualSnapshotFiles(ShardRecoveryPlan.SnapshotFilesToRecover.EMPTY, shardRecoveryPlan.getSnapshotFilesToRecover());
            }

            assertThat(shardRecoveryPlan.getIdenticalFiles(), is(empty()));
            assertThat(shardRecoveryPlan.getExistingSize(), equalTo(0L));

            assertThat(shardRecoveryPlan.getMetadataSnapshot(), equalTo(latestSourceMetadata));

            assertThat(shardRecoveryPlan.getStartingSeqNo(), equalTo(startingSeqNo));
            assertThat(shardRecoveryPlan.getTranslogOps(), equalTo(translogOps));
        });
    }

    private void assertPlanIsValid(ShardRecoveryPlan shardRecoveryPlan, Store.MetadataSnapshot expectedMetadataSnapshot) {
        List<StoreFileMetadata> planFiles = new ArrayList<>();
        planFiles.addAll(shardRecoveryPlan.getIdenticalFiles());
        planFiles.addAll(shardRecoveryPlan.getSourceFilesToRecover());
        for (BlobStoreIndexShardSnapshot.FileInfo fileInfo : shardRecoveryPlan.getSnapshotFilesToRecover()) {
            planFiles.add(fileInfo.metadata());
        }

        List<StoreFileMetadata> missingFiles = iterableAsArrayList(expectedMetadataSnapshot)
            .stream()
            .filter(f -> planFiles.contains(f) == false)
            .collect(Collectors.toList());

        List<StoreFileMetadata> unexpectedFiles = planFiles.stream()
            .filter(f -> expectedMetadataSnapshot.get(f.name()) == null)
            .collect(Collectors.toList());

        assertThat(missingFiles, is(empty()));
        assertThat(unexpectedFiles, is(empty()));
        assertThat(planFiles.size(), is(equalTo(expectedMetadataSnapshot.size())));
        assertThat(shardRecoveryPlan.getMetadataSnapshot(), is(equalTo(expectedMetadataSnapshot)));
    }

    private void assertEqualSnapshotFiles(ShardRecoveryPlan.SnapshotFilesToRecover expected,
                                          ShardRecoveryPlan.SnapshotFilesToRecover actual) {
        assertThat(actual.getIndexId(), is(equalTo(expected.getIndexId())));

        final List<BlobStoreIndexShardSnapshot.FileInfo> missingSnapshotFiles = expected.getFiles()
            .stream()
            .filter(f -> actual.getFiles().contains(f) == false)
            .collect(Collectors.toList());

        final List<BlobStoreIndexShardSnapshot.FileInfo> unexpectedSnapshotFiles = actual.getFiles()
            .stream()
            .filter(f -> expected.getFiles().contains(f) == false)
            .collect(Collectors.toList());

        assertThat(missingSnapshotFiles, is(empty()));
        assertThat(unexpectedSnapshotFiles, is(empty()));
    }

    private Store.MetadataSnapshot generateRandomTargetState(Store store, boolean canShareFilesWithSource) throws IOException {
        final Store.MetadataSnapshot targetMetadataSnapshot;
        if (canShareFilesWithSource && randomBoolean()) {
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

    private ShardSnapshotsService.ShardSnapshotData createSnapshotThatDoNotShareSegmentFiles(String repoName) {
        List<BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles = randomList(randomIntBetween(10, 20), () -> {
            StoreFileMetadata storeFileMetadata = randomStoreFileMetadata();
            return new BlobStoreIndexShardSnapshot.FileInfo(randomAlphaOfLength(10), storeFileMetadata, PART_SIZE);
        });

        return createSnapshotData(repoName, snapshotFiles);
    }

    private ShardSnapshotsService.ShardSnapshotData createSnapshotThatSharesSegmentFiles(Store store, String repository) throws Exception {
        Store.MetadataSnapshot sourceMetadata = store.getMetadata(null);
        List<BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles = new ArrayList<>(sourceMetadata.size());
        for (StoreFileMetadata storeFileMetadata : sourceMetadata) {
            BlobStoreIndexShardSnapshot.FileInfo fileInfo =
                new BlobStoreIndexShardSnapshot.FileInfo(randomAlphaOfLength(10), storeFileMetadata, PART_SIZE);
            snapshotFiles.add(fileInfo);
        }
        return createSnapshotData(repository, snapshotFiles);
    }

    private ShardSnapshotsService.ShardSnapshotData createSnapshotData(String repoName,
                                                                       List<BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles) {
        String shardIdentifier = randomAlphaOfLength(10);

        Snapshot snapshot = new Snapshot(repoName, new SnapshotId("snap", UUIDs.randomBase64UUID(random())));
        IndexId indexId = randomIndexId();
        ShardSnapshotInfo shardSnapshotInfo =
            new ShardSnapshotInfo(indexId, shardId, snapshot, randomAlphaOfLength(10), shardIdentifier);

        return new ShardSnapshotsService.ShardSnapshotData(shardSnapshotInfo, snapshotFiles);
    }

    private ShardRecoveryPlan computeShardRecoveryPlan(ShardRecoveryPlanner shardRecoveryPlanner) throws Exception {
        PlainActionFuture<ShardRecoveryPlan> planFuture = PlainActionFuture.newFuture();
        shardRecoveryPlanner.computeRecoveryPlan(planFuture);
        final ShardRecoveryPlan shardRecoveryPlan = planFuture.get();
        assertThat(shardRecoveryPlan, notNullValue());
        return shardRecoveryPlan;
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
        writer.commit();
        writer.close();
    }

    private void createStore(CheckedConsumer<Store, Exception> testBody) throws Exception {
        final Store store = newStore(createTempDir());
        try {
            testBody.accept(store);
        } finally {
            IOUtils.close(store);
        }
    }

    private Store newStore(Path path) {
        BaseDirectoryWrapper baseDirectoryWrapper = RecoverySourceHandlerTests.newFSDirectory(path);
        return new Store(shardId, INDEX_SETTINGS, baseDirectoryWrapper, new DummyShardLock(shardId));
    }

    private StoreFileMetadata randomStoreFileMetadata() {
        return new StoreFileMetadata(randomAlphaOfLength(10), randomLongBetween(1, 100),
            randomAlphaOfLength(10), Version.CURRENT.toString());
    }

    private IndexId randomIndexId() {
        return new IndexId(shardId.getIndexName(), randomAlphaOfLength(10));
    }
}
