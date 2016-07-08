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
package org.elasticsearch.index.seqno;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.junit.Before;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class GlobalCheckpointTests extends ESTestCase {

    GlobalCheckpointTracker checkpointService;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        checkpointService = new GlobalCheckpointTracker(
            IndexSettingsModule.newIndexSettings("test", Settings.EMPTY), SequenceNumbersService.UNASSIGNED_SEQ_NO, logger);
    }

    public void testEmptyShards() {
        assertFalse("checkpoint shouldn't be updated when the are no active shards", checkpointService.updateCheckpointOnPrimary());
        assertThat(checkpointService.getCheckpoint(), equalTo(SequenceNumbersService.UNASSIGNED_SEQ_NO));
    }

    public void testGlobalCheckpointUpdate() {
        Map<String, Long> allocations = new HashMap<>();
        Set<String> active = new HashSet<>();
        Set<String> insync = new HashSet<>();
        Set<String> tracking = new HashSet<>();
        long maxLocalCheckpoint = Long.MAX_VALUE;
        for (int i = randomIntBetween(3, 10); i > 0; i--) {
            String id = "id_" + i + "_" + randomAsciiOfLength(5);
            long localCheckpoint = randomInt(200);
            switch (randomInt(2)) {
                case 0:
                active.add(id);
                maxLocalCheckpoint = Math.min(maxLocalCheckpoint, localCheckpoint);
                    break;
                case 1:
                    insync.add(id);
                    maxLocalCheckpoint = Math.min(maxLocalCheckpoint, localCheckpoint);
                    break;
                case 2:
                    tracking.add(id);
                    break;
                default:
                    throw new IllegalStateException("you messed up your numbers, didn't you?");
            }
            allocations.put(id, localCheckpoint);
        }

        if (maxLocalCheckpoint == Long.MAX_VALUE) {
            // note: this state can not happen in practice as we always have at least one primary shard active/in sync
            // it is however nice not to assume this on this level and check we do the right thing.
            maxLocalCheckpoint = SequenceNumbersService.UNASSIGNED_SEQ_NO;
        }

        assertThat(checkpointService.getCheckpoint(), equalTo(SequenceNumbersService.UNASSIGNED_SEQ_NO));

        logger.info("--> using allocations");
        allocations.keySet().stream().forEach(aId -> {
            final String type;
            if (active.contains(aId)) {
                type = "active";
            } else if (insync.contains(aId)) {
                type = "insync";
            } else if (tracking.contains(aId)) {
                type = "tracked";
            } else {
                throw new IllegalStateException(aId + " not found in any map");
            }
            logger.info("  - [{}], local checkpoint [{}], [{}]", aId, allocations.get(aId), type);
        });

        Set<String> initializing = new HashSet<>(insync);
        initializing.addAll(tracking);

        checkpointService.updateAllocationIdsFromMaster(active, initializing);
        allocations.keySet().stream().forEach(aId -> checkpointService.updateLocalCheckpoint(aId, allocations.get(aId)));

        // make sure insync allocation count
        insync.stream().forEach(aId -> checkpointService.markAllocationIdAsInSync(aId, randomBoolean() ? 0 : allocations.get(aId)));

        assertThat(checkpointService.getCheckpoint(), equalTo(SequenceNumbersService.UNASSIGNED_SEQ_NO));

        assertThat(checkpointService.updateCheckpointOnPrimary(), equalTo(maxLocalCheckpoint != SequenceNumbersService.UNASSIGNED_SEQ_NO));
        assertThat(checkpointService.getCheckpoint(), equalTo(maxLocalCheckpoint));

        // increment checkpoints
        active.stream().forEach(aId -> allocations.put(aId, allocations.get(aId) + 1 + randomInt(4)));
        insync.stream().forEach(aId -> allocations.put(aId, allocations.get(aId) + 1 + randomInt(4)));
        allocations.keySet().stream().forEach(aId -> checkpointService.updateLocalCheckpoint(aId, allocations.get(aId)));

        // now insert an unknown active/insync id , the checkpoint shouldn't change but a refresh should be requested.
        final String extraId = "extra_" + randomAsciiOfLength(5);

        // first check that adding it without the master blessing doesn't change anything.
        checkpointService.updateLocalCheckpoint(extraId, maxLocalCheckpoint + 1 + randomInt(4));
        assertThat(checkpointService.getLocalCheckpointForAllocation(extraId), equalTo(SequenceNumbersService.UNASSIGNED_SEQ_NO));

        Set<String> newActive = new HashSet<>(active);
        newActive.add(extraId);
        checkpointService.updateAllocationIdsFromMaster(newActive, initializing);

        // we should ask for a refresh , but not update the checkpoint
        assertTrue(checkpointService.updateCheckpointOnPrimary());
        assertThat(checkpointService.getCheckpoint(), equalTo(maxLocalCheckpoint));

        // now notify for the new id
        checkpointService.updateLocalCheckpoint(extraId, maxLocalCheckpoint + 1 + randomInt(4));

        // now it should be incremented
        assertTrue(checkpointService.updateCheckpointOnPrimary());
        assertThat(checkpointService.getCheckpoint(), greaterThan(maxLocalCheckpoint));
    }
}
