/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.cluster.routing.allocation.AllocationDecision.ADD_NOT_PREFERRED_ALLOCATION_DECISION;
import static org.elasticsearch.cluster.routing.allocation.AllocationDecision.NOT_PREFERRED;
import static org.elasticsearch.cluster.routing.allocation.AllocationDecision.REVERT_ADD_NOT_PREFERRED_ALLOCATION_DECISION;
import static org.elasticsearch.cluster.routing.allocation.AllocationDecision.YES;

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
        AllocationDecision expectedAllocationDecision = allocationDecision == NOT_PREFERRED ? YES : allocationDecision;
        assertEquals(expectedAllocationDecision, AllocationDecision.readFrom(output.bytes().streamInput()));
    }

    // A NOT_PREFERRED allocation decision was introduced under the transport version ADD_NOT_PREFERRED_ALLOCATION_DECISION, and then
    // reverted under REVERT_ADD_NOT_PREFERRED_ALLOCATION_DECISION
    // Therefore, for all transport versions before and not including ADD_NOT_PREFERRED_ALLOCATION_DECISION, and for all transport versions
    // including REVERT_ADD_NOT_PREFERRED_ALLOCATION_DECISION onwards, NOT_PREFERRED should not be returned.
    public void testSerializationWithoutNotPreferredAllocationDecision() throws IOException {
        TransportVersion transportVersion = randomFrom(
            // All transport versions up to and not including ADD_NOT_PREFERRED_ALLOCATION_DECISION
            TransportVersionUtils.randomVersionBetween(
                random(),
                TransportVersion.minimumCompatible(),
                TransportVersionUtils.getPreviousVersion(ADD_NOT_PREFERRED_ALLOCATION_DECISION)
            ),
            // All subsequent transport versions after the reversion
            TransportVersionUtils.randomVersionBetween(random(), TransportVersion.current(), REVERT_ADD_NOT_PREFERRED_ALLOCATION_DECISION)
        );
        {
            // NOT_PREFERRED should be converted to YES on writeTo.
            AllocationDecision allocationDecision = AllocationDecision.NOT_PREFERRED;
            BytesStreamOutput output = new BytesStreamOutput();
            output.setTransportVersion(transportVersion);
            allocationDecision.writeTo(output);
            assertEquals(AllocationDecision.YES, AllocationDecision.readFrom(output.bytes().streamInput()));
            StreamInput input = output.bytes().streamInput();
            input.setTransportVersion(transportVersion);
            assertEquals(AllocationDecision.YES, AllocationDecision.readFrom(input));
        }
        {
            // YES and THROTTLE are unaffected by writeTo or readFrom. The enum ID values did not change.
            AllocationDecision allocationDecision = randomFrom(AllocationDecision.YES, AllocationDecision.THROTTLED);
            BytesStreamOutput output = new BytesStreamOutput();
            output.setTransportVersion(transportVersion);
            allocationDecision.writeTo(output);
            assertEquals(allocationDecision.id, AllocationDecision.readFrom(output.bytes().streamInput()).id);
            StreamInput input = output.bytes().streamInput();
            input.setTransportVersion(transportVersion);
            assertEquals(allocationDecision, AllocationDecision.readFrom(input));
        }
        {
            // All other allocation decisions ate also unaffected
            AllocationDecision allocationDecision = AllocationDecision.values()[randomByteBetween(
                AllocationDecision.NO.id,
                AllocationDecision.NO_ATTEMPT.id
            )];
            BytesStreamOutput output = new BytesStreamOutput();
            output.setTransportVersion(transportVersion);
            allocationDecision.writeTo(output);
            assertEquals(allocationDecision.id, AllocationDecision.readFrom(output.bytes().streamInput()).id);
            StreamInput input = output.bytes().streamInput();
            input.setTransportVersion(transportVersion);
            assertEquals(allocationDecision, AllocationDecision.readFrom(input));
        }
    }

    // A NOT_PREFERRED allocation decision was introduced under the transport version ADD_NOT_PREFERRED_ALLOCATION_DECISION, and then
    // reverted under REVERT_ADD_NOT_PREFERRED_ALLOCATION_DECISION
    // Therefore, for all transport versions between these two, we expect NOT_PREFERRED to be a valid allocation decision
    public void testSerializationWithNotPreferredAllocationDecision() throws IOException {
        TransportVersion transportVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            ADD_NOT_PREFERRED_ALLOCATION_DECISION,
            TransportVersionUtils.getPreviousVersion(REVERT_ADD_NOT_PREFERRED_ALLOCATION_DECISION)
        );
        {
            AllocationDecision allocationDecision = AllocationDecision.NOT_PREFERRED;
            BytesStreamOutput output = new BytesStreamOutput();
            output.setTransportVersion(transportVersion);
            allocationDecision.writeTo(output);
            StreamInput input = output.bytes().streamInput();
            input.setTransportVersion(transportVersion);
            assertEquals(AllocationDecision.NOT_PREFERRED, AllocationDecision.readFrom(input));
        }
        {
            // YES and THROTTLE are unaffected by writeTo or readFrom. The enum ID values did not change.
            AllocationDecision allocationDecision = randomFrom(AllocationDecision.YES, AllocationDecision.THROTTLED);
            BytesStreamOutput output = new BytesStreamOutput();
            output.setTransportVersion(transportVersion);
            allocationDecision.writeTo(output);
            assertEquals(allocationDecision.id, AllocationDecision.readFrom(output.bytes().streamInput()).id);
            StreamInput input = output.bytes().streamInput();
            input.setTransportVersion(transportVersion);
            assertEquals(allocationDecision, AllocationDecision.readFrom(input));
        }
        {
            // The following enum values will get shifted -1 for backwards compatibility because NOT_PREFERRED was added and placed before
            // them. writeTo should decrease the ID to match the old enum definition. readFrom will increase it for the new definition.
            AllocationDecision allocationDecision = AllocationDecision.values()[randomByteBetween(
                AllocationDecision.NO.id,
                AllocationDecision.NO_ATTEMPT.id
            )];
            BytesStreamOutput output = new BytesStreamOutput();
            output.setTransportVersion(transportVersion);
            allocationDecision.writeTo(output);
            StreamInput input = output.bytes().streamInput();
            input.setTransportVersion(transportVersion);
            assertEquals(allocationDecision, AllocationDecision.readFrom(input));
        }
    }

    /**
     * Tests the order of values in the enum, because we depend on the natural enum sort order for sorting node decisions.
     * See {@link AbstractAllocationDecision#getNodeDecisions()}.
     */
    public void testValuesOrder() {
        assertEquals(0, AllocationDecision.YES.ordinal());
        assertEquals(1, AllocationDecision.THROTTLED.ordinal());
        assertEquals(2, AllocationDecision.NOT_PREFERRED.ordinal());
        assertEquals(3, AllocationDecision.NO.ordinal());
        assertEquals(4, AllocationDecision.WORSE_BALANCE.ordinal());
        assertEquals(5, AllocationDecision.AWAITING_INFO.ordinal());
        assertEquals(6, AllocationDecision.ALLOCATION_DELAYED.ordinal());
        assertEquals(7, AllocationDecision.NO_VALID_SHARD_COPY.ordinal());
        assertEquals(8, AllocationDecision.NO_ATTEMPT.ordinal());
        AllocationDecision[] decisions = AllocationDecision.values();
        Arrays.sort(decisions);
        assertEquals(AllocationDecision.YES, decisions[0]);
        assertEquals(AllocationDecision.THROTTLED, decisions[1]);
        assertEquals(AllocationDecision.NOT_PREFERRED, decisions[2]);
        assertEquals(AllocationDecision.NO, decisions[3]);
        assertEquals(AllocationDecision.WORSE_BALANCE, decisions[4]);
        assertEquals(AllocationDecision.AWAITING_INFO, decisions[5]);
        assertEquals(AllocationDecision.ALLOCATION_DELAYED, decisions[6]);
        assertEquals(AllocationDecision.NO_VALID_SHARD_COPY, decisions[7]);
        assertEquals(AllocationDecision.NO_ATTEMPT, decisions[8]);
    }

    /**
     * Tests getting a {@link AllocationDecision} from {@link Type}.
     */
    public void testFromDecisionType() {
        Type type = randomFrom(Type.values());
        AllocationDecision allocationDecision = AllocationDecision.fromDecisionType(type);
        AllocationDecision expected = switch (type) {
            case NO -> AllocationDecision.NO;
            case NOT_PREFERRED -> AllocationDecision.NOT_PREFERRED;
            case THROTTLE -> AllocationDecision.THROTTLED;
            case YES -> AllocationDecision.YES;
        };

        assertEquals(expected, allocationDecision);
    }

    /**
     * Tests getting a {@link AllocationDecision} from {@link AllocationStatus}.
     */
    public void testFromAllocationStatus() {
        AllocationStatus allocationStatus = rarely() ? null : randomFrom(AllocationStatus.values());
        AllocationDecision allocationDecision = AllocationDecision.fromAllocationStatus(allocationStatus);
        assertNotEquals(
            "Not-preferred should never be the reason for unassigned allocation status",
            allocationDecision,
            AllocationDecision.NOT_PREFERRED
        );
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
