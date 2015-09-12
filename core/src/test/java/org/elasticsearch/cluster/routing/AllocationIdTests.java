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

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.*;

/**
 */
public class AllocationIdTests extends ESTestCase {

    @Test
    public void testShardToStarted() {
        logger.info("-- create unassigned shard");
        ShardRouting shard = ShardRouting.newUnassigned("test", 0, null, true, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        assertThat(shard.allocationId(), nullValue());

        logger.info("-- initialize the shard");
        shard.initialize("node1", -1);
        AllocationId allocationId = shard.allocationId();
        assertThat(allocationId, notNullValue());
        assertThat(allocationId.getId(), notNullValue());
        assertThat(allocationId.getRelocationId(), nullValue());

        logger.info("-- start the shard");
        shard.moveToStarted();
        assertThat(shard.allocationId().getId(), equalTo(allocationId.getId()));
        allocationId = shard.allocationId();
        assertThat(allocationId.getId(), notNullValue());
        assertThat(allocationId.getRelocationId(), nullValue());
    }

    @Test
    public void testSuccessfulRelocation() {
        logger.info("-- build started shard");
        ShardRouting shard = ShardRouting.newUnassigned("test", 0, null, true, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        shard.initialize("node1", -1);
        shard.moveToStarted();

        AllocationId allocationId = shard.allocationId();
        logger.info("-- relocate the shard");
        shard.relocate("node2", -1);
        assertThat(shard.allocationId(), not(equalTo(allocationId)));
        assertThat(shard.allocationId().getId(), equalTo(allocationId.getId()));
        assertThat(shard.allocationId().getRelocationId(), notNullValue());

        ShardRouting target = shard.buildTargetRelocatingShard();
        assertThat(target.allocationId().getId(), equalTo(shard.allocationId().getRelocationId()));
        assertThat(target.allocationId().getRelocationId(), equalTo(shard.allocationId().getId()));

        logger.info("-- finalize the relocation");
        target.moveToStarted();
        assertThat(target.allocationId().getId(), equalTo(shard.allocationId().getRelocationId()));
        assertThat(target.allocationId().getRelocationId(), nullValue());
    }

    @Test
    public void testCancelRelocation() {
        logger.info("-- build started shard");
        ShardRouting shard = ShardRouting.newUnassigned("test", 0, null, true, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        shard.initialize("node1", -1);
        shard.moveToStarted();

        AllocationId allocationId = shard.allocationId();
        logger.info("-- relocate the shard");
        shard.relocate("node2", -1);
        assertThat(shard.allocationId(), not(equalTo(allocationId)));
        assertThat(shard.allocationId().getId(), equalTo(allocationId.getId()));
        assertThat(shard.allocationId().getRelocationId(), notNullValue());
        allocationId = shard.allocationId();

        logger.info("-- cancel relocation");
        shard.cancelRelocation();
        assertThat(shard.allocationId().getId(), equalTo(allocationId.getId()));
        assertThat(shard.allocationId().getRelocationId(), nullValue());
    }

    @Test
    public void testMoveToUnassigned() {
        logger.info("-- build started shard");
        ShardRouting shard = ShardRouting.newUnassigned("test", 0, null, true, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        shard.initialize("node1", -1);
        shard.moveToStarted();

        logger.info("-- move to unassigned");
        shard.moveToUnassigned(new UnassignedInfo(UnassignedInfo.Reason.NODE_LEFT, null));
        assertThat(shard.allocationId(), nullValue());
    }

    @Test
    public void testReinitializing() {
        logger.info("-- build started shard");
        ShardRouting shard = ShardRouting.newUnassigned("test", 0, null, true, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        shard.initialize("node1", -1);
        shard.moveToStarted();
        AllocationId allocationId = shard.allocationId();

        logger.info("-- reinitializing shard");
        shard.reinitializeShard();
        assertThat(shard.allocationId().getId(), notNullValue());
        assertThat(shard.allocationId().getRelocationId(), nullValue());
        assertThat(shard.allocationId().getId(), not(equalTo(allocationId.getId())));
    }
}
