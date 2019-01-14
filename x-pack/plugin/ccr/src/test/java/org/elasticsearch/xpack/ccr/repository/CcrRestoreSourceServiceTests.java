/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.repository;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class CcrRestoreSourceServiceTests extends IndexShardTestCase {

    private CcrRestoreSourceService restoreSourceService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        restoreSourceService = new CcrRestoreSourceService(Settings.EMPTY, threadPool);
    }

    public void testOpenSession() throws IOException {
        IndexShard indexShard1 = newStartedShard(true);
        IndexShard indexShard2 = newStartedShard(true);

        Tuple<String, Store.MetadataSnapshot> session1 = restoreSourceService.openSession(indexShard1);
        Tuple<String, Store.MetadataSnapshot> session2 = restoreSourceService.openSession(indexShard1);

        try (CcrRestoreSourceService.SessionReader reader1 = restoreSourceService.getSessionReader(session1.v1());
             CcrRestoreSourceService.SessionReader reader2 = restoreSourceService.getSessionReader(session2.v1())) {
            // Would throw exception if missing
        }

        Tuple<String, Store.MetadataSnapshot> session3 = restoreSourceService.openSession(indexShard2);

        try (CcrRestoreSourceService.SessionReader reader1 = restoreSourceService.getSessionReader(session1.v1());
             CcrRestoreSourceService.SessionReader reader2 = restoreSourceService.getSessionReader(session2.v1());
             CcrRestoreSourceService.SessionReader reader3 = restoreSourceService.getSessionReader(session3.v1())) {
            // Would throw exception if missing
        }

        restoreSourceService.closeSession(session1.v1());
        restoreSourceService.closeSession(session2.v1());
        restoreSourceService.closeSession(session3.v1());

        closeShards(indexShard1, indexShard2);
    }

    public void testCannotOpenSessionForClosedShard() throws IOException {
        IndexShard indexShard = newStartedShard(true);
        closeShards(indexShard);
        expectThrows(IllegalIndexShardStateException.class, () -> restoreSourceService.openSession(indexShard));
    }

    public void testCloseSession() throws IOException {
        IndexShard indexShard1 = newStartedShard(true);
        IndexShard indexShard2 = newStartedShard(true);

        Tuple<String, Store.MetadataSnapshot> session1 = restoreSourceService.openSession(indexShard1);
        Tuple<String, Store.MetadataSnapshot> session2 = restoreSourceService.openSession(indexShard1);
        Tuple<String, Store.MetadataSnapshot> session3 = restoreSourceService.openSession(indexShard2);

        try (CcrRestoreSourceService.SessionReader reader1 = restoreSourceService.getSessionReader(session1.v1());
             CcrRestoreSourceService.SessionReader reader2 = restoreSourceService.getSessionReader(session2.v1());
             CcrRestoreSourceService.SessionReader reader3 = restoreSourceService.getSessionReader(session3.v1())) {
            // Would throw exception if missing
        }

        restoreSourceService.closeSession(session1.v1());
        expectThrows(IllegalArgumentException.class, () -> restoreSourceService.getSessionReader(session1.v1()));

        restoreSourceService.closeSession(session2.v1());
        expectThrows(IllegalArgumentException.class, () -> restoreSourceService.getSessionReader(session2.v1()));

        restoreSourceService.closeSession(session3.v1());
        expectThrows(IllegalArgumentException.class, () -> restoreSourceService.getSessionReader(session3.v1()));

        closeShards(indexShard1, indexShard2);
    }

    public void testCloseShardListenerFunctionality() throws IOException {
        IndexShard indexShard1 = newStartedShard(true);
        IndexShard indexShard2 = newStartedShard(true);

        Tuple<String, Store.MetadataSnapshot> session1 = restoreSourceService.openSession(indexShard1);
        Tuple<String, Store.MetadataSnapshot> session2 = restoreSourceService.openSession(indexShard1);
        Tuple<String, Store.MetadataSnapshot> session3 = restoreSourceService.openSession(indexShard2);

        try (CcrRestoreSourceService.SessionReader reader1 = restoreSourceService.getSessionReader(session1.v1());
             CcrRestoreSourceService.SessionReader reader2 = restoreSourceService.getSessionReader(session2.v1());
             CcrRestoreSourceService.SessionReader reader3 = restoreSourceService.getSessionReader(session3.v1())) {
            // Would throw exception if missing
        }

        restoreSourceService.afterIndexShardClosed(indexShard1.shardId(), indexShard1, Settings.EMPTY);

        expectThrows(IllegalArgumentException.class, () -> restoreSourceService.getSessionReader(session1.v1()));
        expectThrows(IllegalArgumentException.class, () -> restoreSourceService.getSessionReader(session2.v1()));

        try (CcrRestoreSourceService.SessionReader reader3 = restoreSourceService.getSessionReader(session3.v1())) {
            // Would throw exception if missing
        }

        restoreSourceService.closeSession(session3.v1());
        closeShards(indexShard1, indexShard2);
    }

    public void testGetSessionReader() throws IOException {
        IndexShard indexShard1 = newStartedShard(true);

        Tuple<String, Store.MetadataSnapshot> session = restoreSourceService.openSession(indexShard1);

        ArrayList<StoreFileMetaData> files = new ArrayList<>();
        indexShard1.snapshotStoreMetadata().forEach(files::add);

        StoreFileMetaData fileMetaData = files.get(0);
        String fileName = fileMetaData.name();

        byte[] expectedBytes = new byte[(int) fileMetaData.length()];
        byte[] actualBytes = new byte[(int) fileMetaData.length()];
        Engine.IndexCommitRef indexCommitRef = indexShard1.acquireSafeIndexCommit();
        try (IndexInput indexInput = indexCommitRef.getIndexCommit().getDirectory().openInput(fileName, IOContext.READONCE)) {
            indexInput.seek(0);
            indexInput.readBytes(expectedBytes, 0, (int) fileMetaData.length());
        }

        BytesArray byteArray = new BytesArray(actualBytes);
        try (CcrRestoreSourceService.SessionReader sessionReader = restoreSourceService.getSessionReader(session.v1())) {
            long offset = sessionReader.readFileBytes(fileName, byteArray);
            assertEquals(offset, fileMetaData.length());
        }

        assertArrayEquals(expectedBytes, actualBytes);
        restoreSourceService.closeSession(session.v1());
        closeShards(indexShard1);
    }

    public void testGetSessionDoesNotLeakFileIfClosed() throws IOException {
        Settings settings = Settings.builder().put("index.merge.enabled", false).build();
        IndexShard indexShard = newStartedShard(true, settings);
        for (int i = 0; i < 5; i++) {
            indexDoc(indexShard, "_doc", Integer.toString(i));
            flushShard(indexShard, true);
        }

        Tuple<String, Store.MetadataSnapshot> session = restoreSourceService.openSession(indexShard);

        ArrayList<StoreFileMetaData> files = new ArrayList<>();
        indexShard.snapshotStoreMetadata().forEach(files::add);

        try (CcrRestoreSourceService.SessionReader sessionReader = restoreSourceService.getSessionReader(session.v1())) {
            sessionReader.readFileBytes(files.get(0).name(), new BytesArray(new byte[10]));
        }

        // Request a second file to ensure that original file is not leaked
        try (CcrRestoreSourceService.SessionReader sessionReader = restoreSourceService.getSessionReader(session.v1())) {
            sessionReader.readFileBytes(files.get(1).name(), new BytesArray(new byte[10]));
        }

        restoreSourceService.closeSession(session.v1());
        closeShards(indexShard);
        // Exception will be thrown if file is not closed.
    }

    public void testSessionCanTimeout() throws Exception {
        restoreSourceService = new CcrRestoreSourceService(Settings.EMPTY, threadPool, new TimeValue(10, TimeUnit.MILLISECONDS));
        IndexShard indexShard = newStartedShard(true);

        assertBusy(() -> {
            Tuple<String, Store.MetadataSnapshot> session = restoreSourceService.openSession(indexShard);
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            try (CcrRestoreSourceService.SessionReader reader = restoreSourceService.getSessionReader(session.v1())) {
                fail("Should have timed out.");
            } catch (IllegalArgumentException e) {
                // This is fine we want the session to be missing
            }
        });

        closeShards(indexShard);
    }
}
