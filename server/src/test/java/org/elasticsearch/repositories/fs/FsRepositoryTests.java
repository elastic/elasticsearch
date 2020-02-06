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
package org.elasticsearch.repositories.fs;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

public class FsRepositoryTests extends ESTestCase {

    public void testSnapshotAndRestore() throws IOException, InterruptedException {
        ThreadPool threadPool = new TestThreadPool(getClass().getSimpleName());
        try (Directory directory = newDirectory()) {
            Path repo = createTempDir();
            Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
                .put(Environment.PATH_REPO_SETTING.getKey(), repo.toAbsolutePath())
                .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths())
                .put("location", repo)
                .put("compress", randomBoolean())
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES).build();

            int numDocs = indexDocs(directory);
            RepositoryMetaData metaData = new RepositoryMetaData("test", "fs", settings);
            FsRepository repository = new FsRepository(metaData, new Environment(settings, null), NamedXContentRegistry.EMPTY,
                BlobStoreTestUtil.mockClusterService());
            repository.start();
            final Settings indexSettings = Settings.builder().put(IndexMetaData.SETTING_INDEX_UUID, "myindexUUID").build();
            IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("myindex", indexSettings);
            ShardId shardId = new ShardId(idxSettings.getIndex(), 1);
            Store store = new Store(shardId, idxSettings, directory, new DummyShardLock(shardId));
            SnapshotId snapshotId = new SnapshotId("test", "test");
            IndexId indexId = new IndexId(idxSettings.getIndex().getName(), idxSettings.getUUID());

            IndexCommit indexCommit = Lucene.getIndexCommit(Lucene.readSegmentInfos(store.directory()), store.directory());
            final PlainActionFuture<String> future1 = PlainActionFuture.newFuture();
            runGeneric(threadPool, () -> {
                IndexShardSnapshotStatus snapshotStatus = IndexShardSnapshotStatus.newInitializing(null);
                repository.snapshotShard(store, null, snapshotId, indexId, indexCommit, snapshotStatus, true,
                    Collections.emptyMap(), future1);
                future1.actionGet();
                IndexShardSnapshotStatus.Copy copy = snapshotStatus.asCopy();
                assertEquals(copy.getTotalFileCount(), copy.getIncrementalFileCount());
            });
            final String shardGeneration = future1.actionGet();
            Lucene.cleanLuceneIndex(directory);
            expectThrows(org.apache.lucene.index.IndexNotFoundException.class, () -> Lucene.readSegmentInfos(directory));
            DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
            ShardRouting routing = ShardRouting.newUnassigned(shardId, true, new RecoverySource.SnapshotRecoverySource("test",
                    new Snapshot("foo", snapshotId), Version.CURRENT, "myindex"),
                new UnassignedInfo(UnassignedInfo.Reason.EXISTING_INDEX_RESTORED, ""));
            routing = ShardRoutingHelper.initialize(routing, localNode.getId(), 0);
            RecoveryState state = new RecoveryState(routing, localNode, null);
            final PlainActionFuture<Void> futureA = PlainActionFuture.newFuture();
            runGeneric(threadPool, () -> repository.restoreShard(store, snapshotId, indexId, shardId, state, futureA));
            futureA.actionGet();
            assertTrue(state.getIndex().recoveredBytes() > 0);
            assertEquals(0, state.getIndex().reusedFileCount());
            assertEquals(indexCommit.getFileNames().size(), state.getIndex().recoveredFileCount());
            assertEquals(numDocs, Lucene.readSegmentInfos(directory).totalMaxDoc());
            deleteRandomDoc(store.directory());
            SnapshotId incSnapshotId = new SnapshotId("test1", "test1");
            IndexCommit incIndexCommit = Lucene.getIndexCommit(Lucene.readSegmentInfos(store.directory()), store.directory());
            Collection<String> commitFileNames = incIndexCommit.getFileNames();
            final PlainActionFuture<String> future2 = PlainActionFuture.newFuture();
            runGeneric(threadPool, () -> {
                IndexShardSnapshotStatus snapshotStatus = IndexShardSnapshotStatus.newInitializing(shardGeneration);
                repository.snapshotShard(store, null, incSnapshotId, indexId, incIndexCommit, snapshotStatus, true,
                    Collections.emptyMap(), future2);
                future2.actionGet();
                IndexShardSnapshotStatus.Copy copy = snapshotStatus.asCopy();
                assertEquals(2, copy.getIncrementalFileCount());
                assertEquals(commitFileNames.size(), copy.getTotalFileCount());
            });

            // roll back to the first snap and then incrementally restore
            RecoveryState firstState = new RecoveryState(routing, localNode, null);
            final PlainActionFuture<Void> futureB =  PlainActionFuture.newFuture();
            runGeneric(threadPool, () -> repository.restoreShard(store, snapshotId, indexId, shardId, firstState, futureB));
            futureB.actionGet();
            assertEquals("should reuse everything except of .liv and .si",
                commitFileNames.size()-2, firstState.getIndex().reusedFileCount());

            RecoveryState secondState = new RecoveryState(routing, localNode, null);
            final PlainActionFuture<Void> futureC = PlainActionFuture.newFuture();
            runGeneric(threadPool, () -> repository.restoreShard(store, incSnapshotId, indexId, shardId, secondState, futureC));
            futureC.actionGet();
            assertEquals(secondState.getIndex().reusedFileCount(), commitFileNames.size()-2);
            assertEquals(secondState.getIndex().recoveredFileCount(), 2);
            List<RecoveryState.File> recoveredFiles =
                secondState.getIndex().fileDetails().stream().filter(f -> f.reused() == false).collect(Collectors.toList());
            Collections.sort(recoveredFiles, Comparator.comparing(RecoveryState.File::name));
            assertTrue(recoveredFiles.get(0).name(), recoveredFiles.get(0).name().endsWith(".liv"));
            assertTrue(recoveredFiles.get(1).name(), recoveredFiles.get(1).name().endsWith("segments_" + incIndexCommit.getGeneration()));
        } finally {
            terminate(threadPool);
        }
    }

    private void runGeneric(ThreadPool threadPool, Runnable runnable) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        threadPool.generic().submit(() -> {
            try {
                runnable.run();
            } finally {
                latch.countDown();
            }
        });
        latch.await();
    }

    private void deleteRandomDoc(Directory directory) throws IOException {
        try(IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig(random(),
            new MockAnalyzer(random())).setCodec(TestUtil.getDefaultCodec()).setMergePolicy(new FilterMergePolicy(NoMergePolicy.INSTANCE) {
            @Override
            public boolean keepFullyDeletedSegment(IOSupplier<CodecReader> readerIOSupplier) {
                return true;
            }

        }))) {
            final int numDocs = writer.getDocStats().numDocs;
            writer.deleteDocuments(new Term("id", "" + randomIntBetween(0, writer.getDocStats().numDocs-1)));
            writer.commit();
            assertEquals(writer.getDocStats().numDocs, numDocs-1);
        }
    }

    private int indexDocs(Directory directory) throws IOException {
        try(IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig(random(),
            new MockAnalyzer(random())).setCodec(TestUtil.getDefaultCodec()))) {
            int docs = 1 + random().nextInt(100);
            for (int i = 0; i < docs; i++) {
                Document doc = new Document();
                doc.add(new StringField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
                doc.add(new TextField("body",
                    TestUtil.randomRealisticUnicodeString(random()), random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
                doc.add(new SortedDocValuesField("dv", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));
                writer.addDocument(doc);
            }
            writer.commit();
            return docs;
        }
    }

}
