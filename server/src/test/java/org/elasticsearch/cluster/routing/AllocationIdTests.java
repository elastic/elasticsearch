/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.routing.RecoverySource.ExistingStoreRecoverySource;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AllocationIdTests extends ESTestCase {
    public void testShardToStarted() {
        logger.info("-- create unassigned shard");
        ShardRouting shard = ShardRouting.newUnassigned(new ShardId("test","_na_", 0), true,
            ExistingStoreRecoverySource.INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        assertThat(shard.allocationId(), nullValue());

        logger.info("-- initialize the shard");
        shard = shard.initialize("node1", null, -1);
        AllocationId allocationId = shard.allocationId();
        assertThat(allocationId, notNullValue());
        assertThat(allocationId.getId(), notNullValue());
        assertThat(allocationId.getRelocationId(), nullValue());

        logger.info("-- start the shard");
        shard = shard.moveToStarted();
        assertThat(shard.allocationId().getId(), equalTo(allocationId.getId()));
        allocationId = shard.allocationId();
        assertThat(allocationId.getId(), notNullValue());
        assertThat(allocationId.getRelocationId(), nullValue());
    }

    public void testSuccessfulRelocation() {
        logger.info("-- build started shard");
        ShardRouting shard = ShardRouting.newUnassigned(new ShardId("test","_na_", 0), true,
            ExistingStoreRecoverySource.INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        shard = shard.initialize("node1", null, -1);
        shard = shard.moveToStarted();

        AllocationId allocationId = shard.allocationId();
        logger.info("-- relocate the shard");
        shard = shard.relocate("node2", -1);
        assertThat(shard.allocationId(), not(equalTo(allocationId)));
        assertThat(shard.allocationId().getId(), equalTo(allocationId.getId()));
        assertThat(shard.allocationId().getRelocationId(), notNullValue());

        ShardRouting target = shard.getTargetRelocatingShard();
        assertThat(target.allocationId().getId(), equalTo(shard.allocationId().getRelocationId()));
        assertThat(target.allocationId().getRelocationId(), equalTo(shard.allocationId().getId()));

        logger.info("-- finalize the relocation");
        target = target.moveToStarted();
        assertThat(target.allocationId().getId(), equalTo(shard.allocationId().getRelocationId()));
        assertThat(target.allocationId().getRelocationId(), nullValue());
    }

    public void testCancelRelocation() {
        logger.info("-- build started shard");
        ShardRouting shard = ShardRouting.newUnassigned(new ShardId("test","_na_", 0), true,
            ExistingStoreRecoverySource.INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        shard = shard.initialize("node1", null, -1);
        shard = shard.moveToStarted();

        AllocationId allocationId = shard.allocationId();
        logger.info("-- relocate the shard");
        shard = shard.relocate("node2", -1);
        assertThat(shard.allocationId(), not(equalTo(allocationId)));
        assertThat(shard.allocationId().getId(), equalTo(allocationId.getId()));
        assertThat(shard.allocationId().getRelocationId(), notNullValue());
        allocationId = shard.allocationId();

        logger.info("-- cancel relocation");
        shard = shard.cancelRelocation();
        assertThat(shard.allocationId().getId(), equalTo(allocationId.getId()));
        assertThat(shard.allocationId().getRelocationId(), nullValue());
    }

    public void testMoveToUnassigned() {
        logger.info("-- build started shard");
        ShardRouting shard = ShardRouting.newUnassigned(new ShardId("test","_na_", 0), true,
            ExistingStoreRecoverySource.INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        shard = shard.initialize("node1", null, -1);
        shard = shard.moveToStarted();

        logger.info("-- move to unassigned");
        shard = shard.moveToUnassigned(
            new UnassignedInfo(
                UnassignedInfo.Reason.NODE_LEFT,
                this.getTestName(),
                null,
                0,
                System.nanoTime(),
                System.currentTimeMillis(),
                false,
                UnassignedInfo.AllocationStatus.NO_ATTEMPT,
                Collections.emptySet(),
                randomAlphaOfLength(10)
            )
        );
        assertThat(shard.allocationId(), nullValue());
    }

    public void testSerialization() throws IOException {
        AllocationId allocationId = AllocationId.newInitializing();
        if (randomBoolean()) {
            allocationId = AllocationId.newRelocation(allocationId);
        }
        BytesReference bytes = BytesReference.bytes(allocationId.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS));
        AllocationId parsedAllocationId = AllocationId.fromXContent(createParser(JsonXContent.jsonXContent, bytes));
        assertEquals(allocationId, parsedAllocationId);
    }

    public void testEquals() {
        AllocationId allocationId1 = AllocationId.newInitializing();
        AllocationId allocationId2 = AllocationId.newInitializing(allocationId1.getId());
        AllocationId allocationId3 = AllocationId.newInitializing("not a UUID");
        String s = "Some random other object";
        assertEquals(allocationId1, allocationId1);
        assertEquals(allocationId1, allocationId2);
        assertNotEquals(allocationId1, s);
        assertNotEquals(allocationId1, null);
        assertNotEquals(allocationId1, allocationId3);

        allocationId2 = AllocationId.newRelocation(allocationId1);
        assertNotEquals(allocationId1, allocationId2);
    }
}
