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
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
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
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class ShardRecoveryPlannerTests extends ESTestCase {
    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings("index",
        Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, org.elasticsearch.Version.CURRENT).build());
    private final ShardId shardId = new ShardId(INDEX_SETTINGS.getIndex(), 1);

    public void testSimpleRecovery() throws Exception {
        runTest(store -> {
            writeRandomDocs(store, randomIntBetween(10, 100));
            final Store.MetadataSnapshot sourceMetadata = store.getMetadata(null);

            final ShardRecoveryPlanner shardRecoveryPlanner = new ShardRecoveryPlanner(shardId,
                null,
                sourceMetadata,
                Store.MetadataSnapshot.EMPTY,
                0,
                0,
                new SnapshotInfoFetcher(null, null, null) {
                    @Override
                    protected void fetchAvailableSnapshots(ShardId shardId, ActionListener<List<ShardSnapshotData>> listener) {
                        assert false: "Unexpected call";
                    }
                },
                false
            );

            PlainActionFuture<ShardRecoveryPlan> planFuture = PlainActionFuture.newFuture();
            shardRecoveryPlanner.computeRecoveryPlan(planFuture);
            final ShardRecoveryPlan shardRecoveryPlan = planFuture.get();
            assertThat(shardRecoveryPlan, notNullValue());

            List<StoreFileMetadata> missingAndDifferent = sourceMetadata.recoveryDiff(Store.MetadataSnapshot.EMPTY).missingAndDifferent;
            assertThat(shardRecoveryPlan.getSourceFilesToRecover(), is(equalTo(missingAndDifferent)));
            assertThat(shardRecoveryPlan.getSnapshotFilesToRecover(), is(equalTo(ShardRecoveryPlan.SnapshotFilesToRecover.EMPTY)));
            assertThat(shardRecoveryPlan.getIdenticalFiles(), is(empty()));
            assertThat(shardRecoveryPlan.getExistingSize(), equalTo(0L));
            assertThat(shardRecoveryPlan.getMetadataSnapshot(), equalTo(sourceMetadata));

            assertThat(shardRecoveryPlan.getStartingSeqNo(), equalTo(0L));
            assertThat(shardRecoveryPlan.getTranslogOps(), equalTo(0));
        });
    }

    public void testFallbacksToRegularPlanIfThereAreNotAvailableSnapshots() throws Exception {
        runTest(store -> {
            writeRandomDocs(store, randomIntBetween(10, 100));
            final Store.MetadataSnapshot sourceMetadata = store.getMetadata(null);

            final ShardRecoveryPlanner shardRecoveryPlanner = new ShardRecoveryPlanner(shardId,
                null,
                sourceMetadata,
                Store.MetadataSnapshot.EMPTY,
                0,
                0,
                new SnapshotInfoFetcher(null, null, null) {
                    @Override
                    protected void fetchAvailableSnapshots(ShardId shardId, ActionListener<List<ShardSnapshotData>> listener) {
                        listener.onResponse(Collections.emptyList());
                    }
                },
                true
            );

            PlainActionFuture<ShardRecoveryPlan> planFuture = PlainActionFuture.newFuture();
            shardRecoveryPlanner.computeRecoveryPlan(planFuture);
            final ShardRecoveryPlan shardRecoveryPlan = planFuture.get();
            assertThat(shardRecoveryPlan, notNullValue());

            List<StoreFileMetadata> missingAndDifferent = sourceMetadata.recoveryDiff(Store.MetadataSnapshot.EMPTY).missingAndDifferent;
            assertThat(shardRecoveryPlan.getSourceFilesToRecover(), is(equalTo(missingAndDifferent)));
            assertThat(shardRecoveryPlan.getSnapshotFilesToRecover(), is(equalTo(ShardRecoveryPlan.SnapshotFilesToRecover.EMPTY)));
            assertThat(shardRecoveryPlan.getIdenticalFiles(), is(empty()));
        });
    }

    public void testFallbacksToRegularPlanIfThereIsAFailureWhileFetchingTheSnapshots() throws Exception {
        runTest(store -> {
            writeRandomDocs(store, randomIntBetween(10, 100));
            Store.MetadataSnapshot sourceMetadata = store.getMetadata(null);

            ShardRecoveryPlanner shardRecoveryPlanner = new ShardRecoveryPlanner(shardId,
                null,
                sourceMetadata,
                Store.MetadataSnapshot.EMPTY,
                0,
                0,
                new SnapshotInfoFetcher(null, null, null) {
                    @Override
                    protected void fetchAvailableSnapshots(ShardId shardId, ActionListener<List<ShardSnapshotData>> listener) {
                        listener.onFailure(new IOException("Unable to fetch the snapshots"));
                    }
                },
                true
            );

            PlainActionFuture<ShardRecoveryPlan> planFuture = PlainActionFuture.newFuture();
            shardRecoveryPlanner.computeRecoveryPlan(planFuture);
            final ShardRecoveryPlan shardRecoveryPlan = planFuture.get();
            assertThat(shardRecoveryPlan, notNullValue());

            List<StoreFileMetadata> missingAndDifferent = sourceMetadata.recoveryDiff(Store.MetadataSnapshot.EMPTY).missingAndDifferent;
            assertThat(shardRecoveryPlan.getSourceFilesToRecover(), is(equalTo(missingAndDifferent)));

            assertEqualSnapshotFiles(shardRecoveryPlan.getSnapshotFilesToRecover(), ShardRecoveryPlan.SnapshotFilesToRecover.EMPTY);
            assertThat(shardRecoveryPlan.getIdenticalFiles(), is(empty()));
        });
    }

    public void testLogicallyEquivalentSnapshotIsUsed() throws Exception {
        runTest(store -> {
            writeRandomDocs(store, randomIntBetween(10, 100));
            final String shardIdentifier = randomAlphaOfLength(10);
            Store.MetadataSnapshot sourceMetadata = store.getMetadata(null);
            List<BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles = new ArrayList<>(sourceMetadata.size());
            for (StoreFileMetadata storeFileMetadata : sourceMetadata) {
                snapshotFiles.add(new BlobStoreIndexShardSnapshot.FileInfo(randomAlphaOfLength(10), storeFileMetadata,
                    new ByteSizeValue(Long.MAX_VALUE)));
            }
            IndexId indexId = new IndexId(randomAlphaOfLength(10), randomAlphaOfLength(10));
            Snapshot snapshot = new Snapshot("repo", new SnapshotId("snap", UUIDs.randomBase64UUID(random())));
            ShardSnapshotInfo shardSnapshotInfo =
                new ShardSnapshotInfo(indexId, shardId, snapshot, randomAlphaOfLength(10), shardIdentifier);
            SnapshotInfoFetcher.ShardSnapshotData shardSnapshotData =
                new SnapshotInfoFetcher.ShardSnapshotData(shardSnapshotInfo, snapshotFiles);

            ShardRecoveryPlanner shardRecoveryPlanner = new ShardRecoveryPlanner(shardId,
                shardIdentifier,
                sourceMetadata,
                Store.MetadataSnapshot.EMPTY,
                0,
                0,
                new SnapshotInfoFetcher(null, null, null) {
                    @Override
                    protected void fetchAvailableSnapshots(ShardId shardId, ActionListener<List<ShardSnapshotData>> listener) {
                        listener.onResponse(Collections.singletonList(shardSnapshotData));
                    }
                },
                true
            );

            PlainActionFuture<ShardRecoveryPlan> planFuture = PlainActionFuture.newFuture();
            shardRecoveryPlanner.computeRecoveryPlan(planFuture);
            final ShardRecoveryPlan shardRecoveryPlan = planFuture.get();
            assertThat(shardRecoveryPlan, notNullValue());

            ShardRecoveryPlan.SnapshotFilesToRecover expectedSnapshotFilesToRecover = new ShardRecoveryPlan.SnapshotFilesToRecover(
                indexId,
                snapshotFiles
            );

            assertThat(shardRecoveryPlan.getSourceFilesToRecover(), is(empty()));
            assertEqualSnapshotFiles(expectedSnapshotFilesToRecover, shardRecoveryPlan.getSnapshotFilesToRecover());

            assertThat(shardRecoveryPlan.getIdenticalFiles(), is(empty()));
            assertThat(shardRecoveryPlan.getExistingSize(), equalTo(0L));

            assertThat(shardRecoveryPlan.getMetadataSnapshot(), equalTo(sourceMetadata));

            assertThat(shardRecoveryPlan.getStartingSeqNo(), equalTo(0L));
            assertThat(shardRecoveryPlan.getTranslogOps(), equalTo(0));
        });
    }

    public void testPlannerTriesToUseMostFilesFromSnapshots() throws Exception {
        runTest(store -> {
            writeRandomDocs(store, randomIntBetween(10, 100));
            final String shardIdentifier = randomAlphaOfLength(10);
            Store.MetadataSnapshot sourceMetadata = store.getMetadata(null);
            List<BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles = new ArrayList<>(sourceMetadata.size());
            final StoreFileMetadata segmentsFile = sourceMetadata.getSegmentsFile();
            for (StoreFileMetadata storeFileMetadata : sourceMetadata) {
                if (storeFileMetadata.equals(segmentsFile)) {
                    continue;
                }
                snapshotFiles.add(new BlobStoreIndexShardSnapshot.FileInfo(randomAlphaOfLength(10), storeFileMetadata,
                    new ByteSizeValue(Long.MAX_VALUE)));
            }
            IndexId indexId = new IndexId(randomAlphaOfLength(10), randomAlphaOfLength(10));
            Snapshot snapshot = new Snapshot("repo", new SnapshotId("snap", UUIDs.randomBase64UUID(random())));
            ShardSnapshotInfo shardSnapshotInfo =
                new ShardSnapshotInfo(indexId, shardId, snapshot, randomAlphaOfLength(10), shardIdentifier);
            SnapshotInfoFetcher.ShardSnapshotData shardSnapshotData =
                new SnapshotInfoFetcher.ShardSnapshotData(shardSnapshotInfo, snapshotFiles);

            writeRandomDocs(store, randomIntBetween(20, 50));
            Store.MetadataSnapshot latestSourceMetadata = store.getMetadata(null);
            String latestShardIdentifier = randomAlphaOfLength(10);

            ShardRecoveryPlanner shardRecoveryPlanner = new ShardRecoveryPlanner(shardId,
                latestShardIdentifier,
                latestSourceMetadata,
                Store.MetadataSnapshot.EMPTY,
                0,
                0,
                new SnapshotInfoFetcher(null, null, null) {
                    @Override
                    protected void fetchAvailableSnapshots(ShardId shardId, ActionListener<List<ShardSnapshotData>> listener) {
                        listener.onResponse(Collections.singletonList(shardSnapshotData));
                    }
                },
                true
            );

            PlainActionFuture<ShardRecoveryPlan> planFuture = PlainActionFuture.newFuture();
            shardRecoveryPlanner.computeRecoveryPlan(planFuture);
            final ShardRecoveryPlan shardRecoveryPlan = planFuture.get();
            assertThat(shardRecoveryPlan, notNullValue());

            ShardRecoveryPlan.SnapshotFilesToRecover expectedSnapshotFilesToRecover = new ShardRecoveryPlan.SnapshotFilesToRecover(
                indexId,
                snapshotFiles
            );

            final Store.RecoveryDiff recoveryDiff = latestSourceMetadata.recoveryDiff(sourceMetadata);
            assertThat(shardRecoveryPlan.getSourceFilesToRecover(), is(equalTo(recoveryDiff.missingAndDifferent)));
            assertEqualSnapshotFiles(expectedSnapshotFilesToRecover, shardRecoveryPlan.getSnapshotFilesToRecover());

            assertThat(shardRecoveryPlan.getIdenticalFiles(), is(empty()));
            assertThat(shardRecoveryPlan.getExistingSize(), equalTo(0L));

            assertThat(shardRecoveryPlan.getMetadataSnapshot(), equalTo(latestSourceMetadata));

            assertThat(shardRecoveryPlan.getStartingSeqNo(), equalTo(0L));
            assertThat(shardRecoveryPlan.getTranslogOps(), equalTo(0));
        });
    }

    public void testPlannerTriesToUseMostFilesFromSnapshots2() throws Exception {
        runTest(store -> {
            int numberOfAvailableSnapshots = randomIntBetween(1, 10);
            List<SnapshotInfoFetcher.ShardSnapshotData> availableSnapshots = new ArrayList<>(numberOfAvailableSnapshots);
            for (int i = 0; i < numberOfAvailableSnapshots; i++) {
                String shardIdentifier = randomAlphaOfLength(10);
                IndexId indexId = new IndexId(randomAlphaOfLength(10), randomAlphaOfLength(10));
                List<BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles = randomList(randomIntBetween(10, 20), () -> {
                    StoreFileMetadata storeFileMetadata = new StoreFileMetadata(randomAlphaOfLength(10), randomLongBetween(1, 100),
                        randomAlphaOfLength(10), Version.CURRENT.toString());
                    return new BlobStoreIndexShardSnapshot.FileInfo(randomAlphaOfLength(10), storeFileMetadata,
                        new ByteSizeValue(Long.MAX_VALUE));
                });

                Snapshot snapshot = new Snapshot("repo" + i, new SnapshotId("snap", UUIDs.randomBase64UUID(random())));
                ShardSnapshotInfo shardSnapshotInfo =
                    new ShardSnapshotInfo(indexId, shardId, snapshot, randomAlphaOfLength(10), shardIdentifier);
                availableSnapshots.add(new SnapshotInfoFetcher.ShardSnapshotData(shardSnapshotInfo, snapshotFiles));
            }

            writeRandomDocs(store, randomIntBetween(10, 100));
            Store.MetadataSnapshot sourceMetadata = store.getMetadata(null);
            String latestShardIdentifier = randomAlphaOfLength(10);

            long startingSeqNo = randomLongBetween(0, 100);
            int translogOps = randomIntBetween(0, 100);
            ShardRecoveryPlanner shardRecoveryPlanner = new ShardRecoveryPlanner(shardId,
                latestShardIdentifier,
                sourceMetadata,
                Store.MetadataSnapshot.EMPTY,
                startingSeqNo,
                translogOps,
                new SnapshotInfoFetcher(null, null, null) {
                    @Override
                    protected void fetchAvailableSnapshots(ShardId shardId, ActionListener<List<ShardSnapshotData>> listener) {
                        listener.onResponse(availableSnapshots);
                    }
                },
                true
            );

            PlainActionFuture<ShardRecoveryPlan> planFuture = PlainActionFuture.newFuture();
            shardRecoveryPlanner.computeRecoveryPlan(planFuture);
            final ShardRecoveryPlan shardRecoveryPlan = planFuture.get();
            assertThat(shardRecoveryPlan, notNullValue());

            assertThat(Set.copyOf(shardRecoveryPlan.getSourceFilesToRecover()),
                is(equalTo(Set.copyOf(CollectionUtils.iterableAsArrayList(sourceMetadata)))));
            assertEqualSnapshotFiles(shardRecoveryPlan.getSnapshotFilesToRecover(), ShardRecoveryPlan.SnapshotFilesToRecover.EMPTY);

            assertThat(shardRecoveryPlan.getIdenticalFiles(), is(empty()));
            assertThat(shardRecoveryPlan.getExistingSize(), equalTo(0L));

            assertThat(shardRecoveryPlan.getMetadataSnapshot(), equalTo(sourceMetadata));

            assertThat(shardRecoveryPlan.getStartingSeqNo(), equalTo(startingSeqNo));
            assertThat(shardRecoveryPlan.getTranslogOps(), equalTo(translogOps));
        });
    }

    public void testExistingFilesInTarget() {
        fail();
    }

    private void assertEqualSnapshotFiles(ShardRecoveryPlan.SnapshotFilesToRecover expected,
                                          ShardRecoveryPlan.SnapshotFilesToRecover actual) {
        assertThat(expected.getIndexId(), is(equalTo(actual.getIndexId())));
        assertThat(Set.copyOf(expected.getFiles()), is(equalTo(Set.copyOf(actual.getFiles()))));
    }

    private void writeRandomDocs(Store store, int numDocs) throws IOException {
        Directory dir = store.directory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig());
        for (int i = 0; i < numDocs; i++) {
            Document document = new Document();
            document.add(new StringField("id", Integer.toString(i), Field.Store.YES));
            document.add(newField("field", randomUnicodeOfCodepointLengthBetween(1, 10), TextField.TYPE_STORED));
            writer.addDocument(document);
        }
        writer.commit();
        writer.close();
    }

    private void runTest(CheckedConsumer<Store, Exception> testBody) throws Exception {
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
}
