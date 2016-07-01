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
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.AccessControlException;
import java.util.Arrays;
import java.util.function.Predicate;

public class StoreRecoveryTests extends ESTestCase {

    public void testAddIndices() throws IOException {
        Directory[] dirs = new Directory[randomIntBetween(1, 10)];
        final int numDocs = randomIntBetween(50, 100);
        int id = 0;
        for (int i = 0; i < dirs.length; i++) {
            dirs[i] = newFSDirectory(createTempDir());
            IndexWriter writer = new IndexWriter(dirs[i], newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
                .setOpenMode(IndexWriterConfig.OpenMode.CREATE));
            for (int j = 0; j < numDocs; j++) {
                writer.addDocument(Arrays.asList(new StringField("id", Integer.toString(id++), Field.Store.YES)));
            }

            writer.commit();
            writer.close();
        }
        StoreRecovery storeRecovery = new StoreRecovery(new ShardId("foo", "bar", 1), logger);
        RecoveryState.Index indexStats = new RecoveryState.Index();
        Directory target = newFSDirectory(createTempDir());
        storeRecovery.addIndices(indexStats, target, dirs);
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
        for (SegmentCommitInfo info : segmentCommitInfos) { // check that we didn't merge
            assertEquals("all sources must be flush", info.info.getDiagnostics().get("source"), "flush");
        }
        assertEquals(reader.numDeletedDocs(), 0);
        assertEquals(reader.numDocs(), id);
        reader.close();
        target.close();
        IOUtils.close(dirs);
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
