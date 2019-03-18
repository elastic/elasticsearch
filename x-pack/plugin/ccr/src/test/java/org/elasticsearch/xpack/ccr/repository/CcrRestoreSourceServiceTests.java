/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.repository;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.Collectors;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;

public class CcrRestoreSourceServiceTests extends IndexShardTestCase {

    private CcrRestoreSourceService restoreSourceService;
    private DeterministicTaskQueue taskQueue;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build();
        taskQueue = new DeterministicTaskQueue(settings, random());
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, CcrSettings.getSettings()
            .stream().filter(s -> s.hasNodeScope()).collect(Collectors.toSet()));
        restoreSourceService = new CcrRestoreSourceService(taskQueue.getThreadPool(), new CcrSettings(Settings.EMPTY, clusterSettings));
    }

    public void testOpenSession() throws IOException {
        IndexShard indexShard1 = newStartedShard(true);
        IndexShard indexShard2 = newStartedShard(true);
        final String sessionUUID1 = UUIDs.randomBase64UUID();
        final String sessionUUID2 = UUIDs.randomBase64UUID();
        final String sessionUUID3 = UUIDs.randomBase64UUID();

        restoreSourceService.openSession(sessionUUID1, indexShard1);
        restoreSourceService.openSession(sessionUUID2, indexShard1);

        try (CcrRestoreSourceService.SessionReader reader1 = restoreSourceService.getSessionReader(sessionUUID1);
             CcrRestoreSourceService.SessionReader reader2 = restoreSourceService.getSessionReader(sessionUUID2)) {
            // Would throw exception if missing
        }

        restoreSourceService.openSession(sessionUUID3, indexShard2);

        try (CcrRestoreSourceService.SessionReader reader1 = restoreSourceService.getSessionReader(sessionUUID1);
             CcrRestoreSourceService.SessionReader reader2 = restoreSourceService.getSessionReader(sessionUUID2);
             CcrRestoreSourceService.SessionReader reader3 = restoreSourceService.getSessionReader(sessionUUID3)) {
            // Would throw exception if missing
        }

        restoreSourceService.closeSession(sessionUUID1);
        restoreSourceService.closeSession(sessionUUID2);
        restoreSourceService.closeSession(sessionUUID3);

        closeShards(indexShard1, indexShard2);
    }

    public void testCannotOpenSessionForClosedShard() throws IOException {
        IndexShard indexShard = newStartedShard(true);
        closeShards(indexShard);
        String sessionUUID = UUIDs.randomBase64UUID();
        expectThrows(IllegalIndexShardStateException.class, () -> restoreSourceService.openSession(sessionUUID, indexShard));
    }

    public void testCloseSession() throws IOException {
        IndexShard indexShard1 = newStartedShard(true);
        IndexShard indexShard2 = newStartedShard(true);
        final String sessionUUID1 = UUIDs.randomBase64UUID();
        final String sessionUUID2 = UUIDs.randomBase64UUID();
        final String sessionUUID3 = UUIDs.randomBase64UUID();

        restoreSourceService.openSession(sessionUUID1, indexShard1);
        restoreSourceService.openSession(sessionUUID2, indexShard1);
        restoreSourceService.openSession(sessionUUID3, indexShard2);

        try (CcrRestoreSourceService.SessionReader reader1 = restoreSourceService.getSessionReader(sessionUUID1);
             CcrRestoreSourceService.SessionReader reader2 = restoreSourceService.getSessionReader(sessionUUID2);
             CcrRestoreSourceService.SessionReader reader3 = restoreSourceService.getSessionReader(sessionUUID3)) {
            // Would throw exception if missing
        }

        assertTrue(taskQueue.hasDeferredTasks());

        restoreSourceService.closeSession(sessionUUID1);
        expectThrows(IllegalArgumentException.class, () -> restoreSourceService.getSessionReader(sessionUUID1));

        restoreSourceService.closeSession(sessionUUID2);
        expectThrows(IllegalArgumentException.class, () -> restoreSourceService.getSessionReader(sessionUUID2));

        restoreSourceService.closeSession(sessionUUID3);
        expectThrows(IllegalArgumentException.class, () -> restoreSourceService.getSessionReader(sessionUUID3));

        taskQueue.runAllTasks();
        // The tasks will not be rescheduled as the sessions are closed.
        assertFalse(taskQueue.hasDeferredTasks());

        closeShards(indexShard1, indexShard2);
    }

    public void testCloseShardListenerFunctionality() throws IOException {
        IndexShard indexShard1 = newStartedShard(true);
        IndexShard indexShard2 = newStartedShard(true);
        final String sessionUUID1 = UUIDs.randomBase64UUID();
        final String sessionUUID2 = UUIDs.randomBase64UUID();
        final String sessionUUID3 = UUIDs.randomBase64UUID();

        restoreSourceService.openSession(sessionUUID1, indexShard1);
        restoreSourceService.openSession(sessionUUID2, indexShard1);
        restoreSourceService.openSession(sessionUUID3, indexShard2);

        try (CcrRestoreSourceService.SessionReader reader1 = restoreSourceService.getSessionReader(sessionUUID1);
             CcrRestoreSourceService.SessionReader reader2 = restoreSourceService.getSessionReader(sessionUUID2);
             CcrRestoreSourceService.SessionReader reader3 = restoreSourceService.getSessionReader(sessionUUID3)) {
            // Would throw exception if missing
        }

        restoreSourceService.afterIndexShardClosed(indexShard1.shardId(), indexShard1, Settings.EMPTY);

        expectThrows(IllegalArgumentException.class, () -> restoreSourceService.getSessionReader(sessionUUID1));
        expectThrows(IllegalArgumentException.class, () -> restoreSourceService.getSessionReader(sessionUUID2));

        try (CcrRestoreSourceService.SessionReader reader3 = restoreSourceService.getSessionReader(sessionUUID3)) {
            // Would throw exception if missing
        }

        restoreSourceService.closeSession(sessionUUID3);
        closeShards(indexShard1, indexShard2);
    }

    public void testGetSessionReader() throws IOException {
        IndexShard indexShard1 = newStartedShard(true);
        final String sessionUUID1 = UUIDs.randomBase64UUID();

        restoreSourceService.openSession(sessionUUID1, indexShard1);

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
        try (CcrRestoreSourceService.SessionReader sessionReader = restoreSourceService.getSessionReader(sessionUUID1)) {
            long offset = sessionReader.readFileBytes(fileName, byteArray);
            assertEquals(offset, fileMetaData.length());
        }

        assertArrayEquals(expectedBytes, actualBytes);
        restoreSourceService.closeSession(sessionUUID1);
        closeShards(indexShard1);
    }

    public void testGetSessionDoesNotLeakFileIfClosed() throws IOException {
        Settings settings = Settings.builder().put("index.merge.enabled", false).build();
        IndexShard indexShard = newStartedShard(true, settings);
        for (int i = 0; i < 5; i++) {
            indexDoc(indexShard, "_doc", Integer.toString(i));
            flushShard(indexShard, true);
        }
        final String sessionUUID = UUIDs.randomBase64UUID();

        restoreSourceService.openSession(sessionUUID, indexShard);

        ArrayList<StoreFileMetaData> files = new ArrayList<>();
        indexShard.snapshotStoreMetadata().forEach(files::add);

        try (CcrRestoreSourceService.SessionReader sessionReader = restoreSourceService.getSessionReader(sessionUUID)) {
            sessionReader.readFileBytes(files.get(0).name(), new BytesArray(new byte[10]));
        }

        // Request a second file to ensure that original file is not leaked
        try (CcrRestoreSourceService.SessionReader sessionReader = restoreSourceService.getSessionReader(sessionUUID)) {
            sessionReader.readFileBytes(files.get(1).name(), new BytesArray(new byte[10]));
        }

        assertTrue(EngineTestCase.hasSnapshottedCommits(IndexShardTestCase.getEngine(indexShard)));
        restoreSourceService.closeSession(sessionUUID);
        assertFalse(EngineTestCase.hasSnapshottedCommits(IndexShardTestCase.getEngine(indexShard)));

        closeShards(indexShard);
        // Exception will be thrown if file is not closed.
    }

    public void testSessionCanTimeout() throws Exception {
        IndexShard indexShard = newStartedShard(true);

        final String sessionUUID = UUIDs.randomBase64UUID();

        restoreSourceService.openSession(sessionUUID, indexShard);

        // Session starts as not idle. First task will mark it as idle
        assertTrue(taskQueue.hasDeferredTasks());
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
        // Task is still scheduled
        assertTrue(taskQueue.hasDeferredTasks());

        // Accessing session marks it as not-idle
        try (CcrRestoreSourceService.SessionReader reader = restoreSourceService.getSessionReader(sessionUUID)) {
            // Check session exists
        }

        assertTrue(taskQueue.hasDeferredTasks());
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
        // Task is still scheduled
        assertTrue(taskQueue.hasDeferredTasks());

        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
        // Task is cancelled when the session times out
        assertFalse(taskQueue.hasDeferredTasks());

        expectThrows(IllegalArgumentException.class, () -> restoreSourceService.getSessionReader(sessionUUID));

        closeShards(indexShard);
    }
}
