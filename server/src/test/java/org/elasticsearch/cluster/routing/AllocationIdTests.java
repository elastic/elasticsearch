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

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.routing.RecoverySource.StoreRecoverySource;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AllocationIdTests extends ESTestCase {
    public void testShardToStarted() {
        logger.info("-- create unassigned shard");
        ShardRouting shard = ShardRouting.newUnassigned(new ShardId("test","_na_", 0), true, StoreRecoverySource.EXISTING_STORE_INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
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
        ShardRouting shard = ShardRouting.newUnassigned(new ShardId("test","_na_", 0), true, StoreRecoverySource.EXISTING_STORE_INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
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
        ShardRouting shard = ShardRouting.newUnassigned(new ShardId("test","_na_", 0), true, StoreRecoverySource.EXISTING_STORE_INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
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
        ShardRouting shard = ShardRouting.newUnassigned(new ShardId("test","_na_", 0), true, StoreRecoverySource.EXISTING_STORE_INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        shard = shard.initialize("node1", null, -1);
        shard = shard.moveToStarted();

        logger.info("-- move to unassigned");
        shard = shard.moveToUnassigned(new UnassignedInfo(UnassignedInfo.Reason.NODE_LEFT, null));
        assertThat(shard.allocationId(), nullValue());
    }

    public void testSerialization() throws IOException {
        AllocationId allocationId = AllocationId.newInitializing();
        if (randomBoolean()) {
            allocationId = AllocationId.newRelocation(allocationId);
        }
        BytesReference bytes = allocationId.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).bytes();
        AllocationId parsedAllocationId = AllocationId.fromXContent(createParser(JsonXContent.jsonXContent, bytes));
        assertEquals(allocationId, parsedAllocationId);
    }
}
