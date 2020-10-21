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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.AccessControlException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Predicate;

import static org.hamcrest.CoreMatchers.equalTo;

public class StoreRecoveryTests extends ESTestCase {

    public void testAddIndices() throws IOException {
        Directory[] dirs = new Directory[randomIntBetween(1, 10)];
        final int numDocs = randomIntBetween(50, 100);
        final Sort indexSort;
        if (randomBoolean()) {
            indexSort = new Sort(new SortedNumericSortField("num", SortField.Type.LONG, true));
        } else {
            indexSort = null;
        }
        int id = 0;
        for (int i = 0; i < dirs.length; i++) {
            dirs[i] = newFSDirectory(createTempDir());
            IndexWriterConfig iwc = newIndexWriterConfig()
                .setMergePolicy(NoMergePolicy.INSTANCE)
                .setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            if (indexSort != null) {
                iwc.setIndexSort(indexSort);
            }
            IndexWriter writer = new IndexWriter(dirs[i], iwc);
            for (int j = 0; j < numDocs; j++) {
                writer.addDocument(Arrays.asList(
                    new StringField("id", Integer.toString(id++), Field.Store.YES),
                    new SortedNumericDocValuesField("num", randomLong())
                ));
            }

            writer.commit();
            writer.close();
        }
        StoreRecovery storeRecovery = new StoreRecovery(new ShardId("foo", "bar", 1), logger);
        RecoveryState.Index indexStats = new RecoveryState.Index();
        Directory target = newFSDirectory(createTempDir());
        final long maxSeqNo = randomNonNegativeLong();
        final long maxUnsafeAutoIdTimestamp = randomNonNegativeLong();
        storeRecovery.addIndices(indexStats, target, indexSort, dirs, maxSeqNo, maxUnsafeAutoIdTimestamp, null,  0, false, false);
        int numFiles = 0;
        Predicate<String> filesFilter = (f) -> f.startsWith("segments") == false && f.equals("write.lock") == false
            && f.startsWith("extra") == false;
        for (Directory d : dirs) {
            numFiles += Arrays.asList(d.listAll()).stream().filter(filesFilter).count();
        }
        final long targetNumFiles = Arrays.asList(target.listAll()).stream().filter(filesFilter).count();
        assertEquals(numFiles, targetNumFiles);
        assertEquals(indexStats.totalFileCount(), targetNumFiles);
        if (hardLinksSupported(createTempDir())) {
            assertEquals(targetNumFiles, indexStats.reusedFileCount());
        } else {
            assertEquals(0, indexStats.reusedFileCount(), 0);
        }
        DirectoryReader reader = DirectoryReader.open(target);
        SegmentInfos segmentCommitInfos = SegmentInfos.readLatestCommit(target);
        final Map<String, String> userData = segmentCommitInfos.getUserData();
        assertThat(userData.get(SequenceNumbers.MAX_SEQ_NO), equalTo(Long.toString(maxSeqNo)));
        assertThat(userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY), equalTo(Long.toString(maxSeqNo)));
        assertThat(userData.get(Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID), equalTo(Long.toString(maxUnsafeAutoIdTimestamp)));
        for (SegmentCommitInfo info : segmentCommitInfos) { // check that we didn't merge
            assertEquals("all sources must be flush",
                info.info.getDiagnostics().get("source"), "flush");
            if (indexSort != null) {
                assertEquals(indexSort, info.info.getIndexSort());
            }
        }
        assertEquals(reader.numDeletedDocs(), 0);
        assertEquals(reader.numDocs(), id);
        reader.close();
        target.close();
        IOUtils.close(dirs);
    }

    public void testSplitShard() throws IOException {
        Directory dir = newFSDirectory(createTempDir());
        final int numDocs = randomIntBetween(50, 100);
        final Sort indexSort;
        if (randomBoolean()) {
            indexSort = new Sort(new SortedNumericSortField("num", SortField.Type.LONG, true));
        } else {
            indexSort = null;
        }
        IndexWriterConfig iwc = newIndexWriterConfig()
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        if (indexSort != null) {
            iwc.setIndexSort(indexSort);
        }
        IndexWriter writer = new IndexWriter(dir, iwc);
        for (int j = 0; j < numDocs; j++) {
            writer.addDocument(Arrays.asList(
                new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(j)), Field.Store.YES),
                new SortedNumericDocValuesField("num", randomLong())
            ));
        }

        writer.commit();
        writer.close();
        StoreRecovery storeRecovery = new StoreRecovery(new ShardId("foo", "bar", 1), logger);
        RecoveryState.Index indexStats = new RecoveryState.Index();
        Directory target = newFSDirectory(createTempDir());
        final long maxSeqNo = randomNonNegativeLong();
        final long maxUnsafeAutoIdTimestamp = randomNonNegativeLong();
        int numShards =  randomIntBetween(2, 10);
        int targetShardId = randomIntBetween(0, numShards-1);
        IndexMetadata metadata = IndexMetadata.builder("test")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(numShards)
            .setRoutingNumShards(numShards * 1000000)
            .numberOfReplicas(0).build();
        storeRecovery.addIndices(indexStats, target, indexSort, new Directory[] {dir}, maxSeqNo, maxUnsafeAutoIdTimestamp, metadata,
            targetShardId, true, false);


        SegmentInfos segmentCommitInfos = SegmentInfos.readLatestCommit(target);
        final Map<String, String> userData = segmentCommitInfos.getUserData();
        assertThat(userData.get(SequenceNumbers.MAX_SEQ_NO), equalTo(Long.toString(maxSeqNo)));
        assertThat(userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY), equalTo(Long.toString(maxSeqNo)));
        assertThat(userData.get(Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID), equalTo(Long.toString(maxUnsafeAutoIdTimestamp)));
        for (SegmentCommitInfo info : segmentCommitInfos) { // check that we didn't merge
            assertEquals("all sources must be flush",
                info.info.getDiagnostics().get("source"), "flush");
            if (indexSort != null) {
                assertEquals(indexSort, info.info.getIndexSort());
            }
        }

        iwc = newIndexWriterConfig()
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        if (indexSort != null) {
            iwc.setIndexSort(indexSort);
        }
        writer = new IndexWriter(target, iwc);
        writer.forceMerge(1, true);
        writer.commit();
        writer.close();

        DirectoryReader reader = DirectoryReader.open(target);
        for (LeafReaderContext ctx : reader.leaves()) {
            LeafReader leafReader = ctx.reader();
            Terms terms = leafReader.terms(IdFieldMapper.NAME);
            TermsEnum iterator = terms.iterator();
            BytesRef ref;
            while((ref = iterator.next()) != null) {
                String value = ref.utf8ToString();
                assertEquals("value has wrong shards: " + value, targetShardId, OperationRouting.generateShardId(metadata, value, null));
            }
            for (int i = 0; i < numDocs; i++) {
                ref = new BytesRef(Integer.toString(i));
                int shardId = OperationRouting.generateShardId(metadata, ref.utf8ToString(), null);
                if (shardId == targetShardId) {
                    assertTrue(ref.utf8ToString() + " is missing", terms.iterator().seekExact(ref));
                } else {
                    assertFalse(ref.utf8ToString() + " was found but shouldn't", terms.iterator().seekExact(ref));
                }

            }
        }

        reader.close();
        target.close();
        IOUtils.close(dir);
    }

    public void testStatsDirWrapper() throws IOException {
        Directory dir = newDirectory();
        Directory target = newDirectory();
        RecoveryState.Index indexStats = new RecoveryState.Index();
        StoreRecovery.StatsDirectoryWrapper wrapper = new StoreRecovery.StatsDirectoryWrapper(target, indexStats);
        try (IndexOutput output = dir.createOutput("foo.bar", IOContext.DEFAULT)) {
            CodecUtil.writeHeader(output, "foo", 0);
            int numBytes = randomIntBetween(100, 20000);
            for (int i = 0; i < numBytes; i++) {
                output.writeByte((byte)i);
            }
            CodecUtil.writeFooter(output);
        }
        wrapper.copyFrom(dir, "foo.bar", "bar.foo", IOContext.DEFAULT);
        assertNotNull(indexStats.getFileDetails("bar.foo"));
        assertNull(indexStats.getFileDetails("foo.bar"));
        assertEquals(dir.fileLength("foo.bar"), indexStats.getFileDetails("bar.foo").length());
        assertEquals(dir.fileLength("foo.bar"), indexStats.getFileDetails("bar.foo").recovered());
        assertFalse(indexStats.getFileDetails("bar.foo").reused());
        IOUtils.close(dir, target);
    }

    public boolean hardLinksSupported(Path path) throws IOException {
        try {
            Files.createFile(path.resolve("foo.bar"));
            Files.createLink(path.resolve("test"), path.resolve("foo.bar"));
            BasicFileAttributes destAttr = Files.readAttributes(path.resolve("test"), BasicFileAttributes.class);
            BasicFileAttributes sourceAttr = Files.readAttributes(path.resolve("foo.bar"), BasicFileAttributes.class);
            // we won't get here - no permission ;)
            return destAttr.fileKey() != null
                && destAttr.fileKey().equals(sourceAttr.fileKey());
        } catch (AccessControlException ex) {
            return true; // if we run into that situation we know it's supported.
        } catch (UnsupportedOperationException ex) {
            return false;
        }
    }
}
