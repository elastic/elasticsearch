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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

/**
 * Tests for the {@link AllocationDecision} enum.
 */
public class AllocationDecisionTests extends ESTestCase {

    /**
     * Tests serialization and deserialization.
     */
    public void testSerialization() throws IOException {
        AllocationDecision allocationDecision = randomFrom(AllocationDecision.values());
        BytesStreamOutput output = new BytesStreamOutput();
        allocationDecision.writeTo(output);
        assertEquals(allocationDecision, AllocationDecision.readFrom(output.bytes().streamInput()));
    }

    /**
     * Tests the order of values in the enum, because we depend on the natural enum sort order for sorting node decisions.
     * See {@link AbstractAllocationDecision#getNodeDecisions()}.
     */
    public void testValuesOrder() {
        assertEquals(0, AllocationDecision.YES.ordinal());
        assertEquals(1, AllocationDecision.THROTTLED.ordinal());
        assertEquals(2, AllocationDecision.NO.ordinal());
        assertEquals(3, AllocationDecision.WORSE_BALANCE.ordinal());
        assertEquals(4, AllocationDecision.AWAITING_INFO.ordinal());
        assertEquals(5, AllocationDecision.ALLOCATION_DELAYED.ordinal());
        assertEquals(6, AllocationDecision.NO_VALID_SHARD_COPY.ordinal());
        assertEquals(7, AllocationDecision.NO_ATTEMPT.ordinal());
        AllocationDecision[] decisions = AllocationDecision.values();
        Arrays.sort(decisions);
        assertEquals(AllocationDecision.YES, decisions[0]);
        assertEquals(AllocationDecision.THROTTLED, decisions[1]);
        assertEquals(AllocationDecision.NO, decisions[2]);
        assertEquals(AllocationDecision.WORSE_BALANCE, decisions[3]);
        assertEquals(AllocationDecision.AWAITING_INFO, decisions[4]);
        assertEquals(AllocationDecision.ALLOCATION_DELAYED, decisions[5]);
        assertEquals(AllocationDecision.NO_VALID_SHARD_COPY, decisions[6]);
        assertEquals(AllocationDecision.NO_ATTEMPT, decisions[7]);
    }

    /**
     * Tests getting a {@link AllocationDecision} from {@link Type}.
     */
    public void testFromDecisionType() {
        Type type = randomFrom(Type.values());
        AllocationDecision allocationDecision = AllocationDecision.fromDecisionType(type);
        AllocationDecision expected = type == Type.NO ? AllocationDecision.NO :
                                          type == Type.THROTTLE ? AllocationDecision.THROTTLED : AllocationDecision.YES;
        assertEquals(expected, allocationDecision);
    }

    /**
     * Tests getting a {@link AllocationDecision} from {@link AllocationStatus}.
     */
    public void testFromAllocationStatus() {
        AllocationStatus allocationStatus = rarely() ? null : randomFrom(AllocationStatus.values());
        AllocationDecision allocationDecision = AllocationDecision.fromAllocationStatus(allocationStatus);
        AllocationDecision expected;
        if (allocationStatus == null) {
            expected = AllocationDecision.YES;
        } else if (allocationStatus == AllocationStatus.DECIDERS_THROTTLED) {
            expected = AllocationDecision.THROTTLED;
        } else if (allocationStatus == AllocationStatus.FETCHING_SHARD_DATA) {
            expected = AllocationDecision.AWAITING_INFO;
        } else if (allocationStatus == AllocationStatus.DELAYED_ALLOCATION) {
            expected = AllocationDecision.ALLOCATION_DELAYED;
        } else if (allocationStatus == AllocationStatus.NO_VALID_SHARD_COPY) {
            expected = AllocationDecision.NO_VALID_SHARD_COPY;
        } else if (allocationStatus == AllocationStatus.NO_ATTEMPT) {
            expected = AllocationDecision.NO_ATTEMPT;
        } else {
            expected = AllocationDecision.NO;
        }
        assertEquals(expected, allocationDecision);
    }
}
