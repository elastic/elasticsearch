/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.repository;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.HashSet;

public class CcrRestoreSourceServiceTests extends IndexShardTestCase {

    private CcrRestoreSourceService restoreSourceService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        restoreSourceService = new CcrRestoreSourceService(Settings.EMPTY);
    }

    public void testOpenSession() throws IOException {
        IndexShard indexShard1 = newStartedShard(true);
        IndexShard indexShard2 = newStartedShard(true);
        final String sessionUUID1 = UUIDs.randomBase64UUID();
        final String sessionUUID2 = UUIDs.randomBase64UUID();
        final String sessionUUID3 = UUIDs.randomBase64UUID();

        assertNull(restoreSourceService.getSessionsForShard(indexShard1));

        assertNotNull(restoreSourceService.openSession(sessionUUID1, indexShard1));
        HashSet<String> sessionsForShard = restoreSourceService.getSessionsForShard(indexShard1);
        assertEquals(1, sessionsForShard.size());
        assertTrue(sessionsForShard.contains(sessionUUID1));
        assertNotNull(restoreSourceService.openSession(sessionUUID2, indexShard1));
        sessionsForShard = restoreSourceService.getSessionsForShard(indexShard1);
        assertEquals(2, sessionsForShard.size());
        assertTrue(sessionsForShard.contains(sessionUUID2));

        assertNull(restoreSourceService.getSessionsForShard(indexShard2));
        assertNotNull(restoreSourceService.openSession(sessionUUID3, indexShard2));
        sessionsForShard = restoreSourceService.getSessionsForShard(indexShard2);
        assertEquals(1, sessionsForShard.size());
        assertTrue(sessionsForShard.contains(sessionUUID3));

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
        assertNull(restoreSourceService.getOngoingRestore(sessionUUID));
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

        assertEquals(2, restoreSourceService.getSessionsForShard(indexShard1).size());
        assertEquals(1, restoreSourceService.getSessionsForShard(indexShard2).size());
        assertNotNull(restoreSourceService.getOngoingRestore(sessionUUID1));
        assertNotNull(restoreSourceService.getOngoingRestore(sessionUUID2));
        assertNotNull(restoreSourceService.getOngoingRestore(sessionUUID3));

        restoreSourceService.closeSession(sessionUUID1);
        assertEquals(1, restoreSourceService.getSessionsForShard(indexShard1).size());
        assertNull(restoreSourceService.getOngoingRestore(sessionUUID1));
        assertFalse(restoreSourceService.getSessionsForShard(indexShard1).contains(sessionUUID1));
        assertTrue(restoreSourceService.getSessionsForShard(indexShard1).contains(sessionUUID2));

        restoreSourceService.closeSession(sessionUUID2);
        assertNull(restoreSourceService.getSessionsForShard(indexShard1));
        assertNull(restoreSourceService.getOngoingRestore(sessionUUID2));

        restoreSourceService.closeSession(sessionUUID3);
        assertNull(restoreSourceService.getSessionsForShard(indexShard2));
        assertNull(restoreSourceService.getOngoingRestore(sessionUUID3));

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

        assertEquals(2, restoreSourceService.getSessionsForShard(indexShard1).size());
        assertEquals(1, restoreSourceService.getSessionsForShard(indexShard2).size());

        restoreSourceService.afterIndexShardClosed(indexShard1.shardId(), indexShard1, Settings.EMPTY);

        assertNull(restoreSourceService.getSessionsForShard(indexShard1));
        assertNull(restoreSourceService.getOngoingRestore(sessionUUID1));
        assertNull(restoreSourceService.getOngoingRestore(sessionUUID2));

        restoreSourceService.closeSession(sessionUUID3);
        closeShards(indexShard1, indexShard2);
    }
}
