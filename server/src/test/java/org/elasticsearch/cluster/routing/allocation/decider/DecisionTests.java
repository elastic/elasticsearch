/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EnumSerializationTestUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.cluster.routing.allocation.decider.Decision.Type.ALLOCATION_DECISION_NOT_PREFERRED;
import static org.elasticsearch.cluster.routing.allocation.decider.Decision.Type.NO;
import static org.elasticsearch.cluster.routing.allocation.decider.Decision.Type.NOT_PREFERRED;
import static org.elasticsearch.cluster.routing.allocation.decider.Decision.Type.THROTTLE;
import static org.elasticsearch.cluster.routing.allocation.decider.Decision.Type.YES;
import static org.hamcrest.Matchers.equalTo;

/**
 * A class for unit testing the {@link Decision} class.
 */
public class DecisionTests extends ESTestCase {

    /**
     * The {@link Decision.Type} enum at {@link Decision.Type#ALLOCATION_DECISION_NOT_PREFERRED}
     */
    private enum AllocationDecisionNotPreferredType {
        NO,
        THROTTLE,
        NOT_PREFERRED,
        YES
    }

    /**
     * The {@link Decision.Type} enum before {@link Decision.Type#ALLOCATION_DECISION_NOT_PREFERRED}
     */
    private enum OriginalType {
        NO,
        YES,
        THROTTLE
    }

    public void testTypeEnumOrder() {
        EnumSerializationTestUtils.assertEnumSerialization(Type.class, NO, NOT_PREFERRED, THROTTLE, YES);
    }

    public void testTypeComparisonOrder() {
        assertThat(
            shuffledList(Arrays.asList(Type.values())).stream().sorted(Type::compareToBetweenDecisions).toList(),
            equalTo(List.of(NO, THROTTLE, NOT_PREFERRED, YES))
        );
        assertThat(
            shuffledList(Arrays.asList(Type.values())).stream().sorted(Type::compareToBetweenNodes).toList(),
            equalTo(List.of(NO, NOT_PREFERRED, THROTTLE, YES))
        );
    }

    public void testTypeAllowed() {
        List.of(NOT_PREFERRED, YES).forEach(d -> assertTrue(d.assignmentAllowed()));
        List.of(NO, THROTTLE).forEach(d -> assertFalse(d.assignmentAllowed()));
    }

    public void testSerializationBackwardCompatibility() throws IOException {
        testReadWriteEnum(
            YES,
            AllocationDecisionNotPreferredType.class,
            AllocationDecisionNotPreferredType.YES,
            ALLOCATION_DECISION_NOT_PREFERRED
        );
        testReadWriteEnum(
            NOT_PREFERRED,
            AllocationDecisionNotPreferredType.class,
            AllocationDecisionNotPreferredType.NOT_PREFERRED,
            ALLOCATION_DECISION_NOT_PREFERRED
        );
        testReadWriteEnum(
            THROTTLE,
            AllocationDecisionNotPreferredType.class,
            AllocationDecisionNotPreferredType.THROTTLE,
            ALLOCATION_DECISION_NOT_PREFERRED
        );
        testReadWriteEnum(
            NO,
            AllocationDecisionNotPreferredType.class,
            AllocationDecisionNotPreferredType.NO,
            ALLOCATION_DECISION_NOT_PREFERRED
        );

        // Older versions, lossy serialization - remove when no longer supported
        assertFalse(TransportVersion.minimumCompatible().supports(ALLOCATION_DECISION_NOT_PREFERRED));
        // YES/NOT_PREFERRED turn into YES in those versions, both round-trip to YES
        testReadWriteEnum(YES, OriginalType.class, OriginalType.YES, TransportVersion.minimumCompatible());
        testReadWriteEnum(NOT_PREFERRED, OriginalType.class, OriginalType.YES, YES, TransportVersion.minimumCompatible());
        // THROTTLE and NO are unchanged
        testReadWriteEnum(THROTTLE, OriginalType.class, OriginalType.THROTTLE, TransportVersion.minimumCompatible());
        testReadWriteEnum(NO, OriginalType.class, OriginalType.NO, TransportVersion.minimumCompatible());
    }

    public void testMultiPrioritisation() {
        assertEffectiveDecision(NO, Decision.NO, Decision.THROTTLE, Decision.NOT_PREFERRED, Decision.YES);
        assertEffectiveDecision(THROTTLE, Decision.THROTTLE, Decision.NOT_PREFERRED, Decision.YES);
        assertEffectiveDecision(NOT_PREFERRED, Decision.NOT_PREFERRED, Decision.YES);
        assertEffectiveDecision(YES, Decision.YES);
        assertEffectiveDecision(YES);
    }

    public void testMinimumDecisionTypeThrottleOrYes() {
        assertEquals(Decision.Type.THROTTLE, Decision.minimumDecisionTypeThrottleOrYes(Decision.THROTTLE, Decision.YES));
        assertEquals(Decision.Type.THROTTLE, Decision.minimumDecisionTypeThrottleOrYes(Decision.YES, Decision.THROTTLE));
        assertEquals(Decision.Type.THROTTLE, Decision.minimumDecisionTypeThrottleOrYes(Decision.THROTTLE, Decision.THROTTLE));
        assertEquals(Decision.Type.YES, Decision.minimumDecisionTypeThrottleOrYes(Decision.YES, Decision.YES));
    }

    private void assertEffectiveDecision(Decision.Type effectiveDecision, Decision.Single... decisions) {
        final var multi = new Decision.Multi(shuffledList(List.of(decisions)));
        assertEquals(effectiveDecision, multi.type());
    }

    /**
     * Test the reading and writing of an enum to a specific transport version (assuming lossless roundtrip)
     *
     * @param value The value to write
     * @param remoteEnum The enum to use for deserialization
     * @param expectedSerialisedValue The expected deserialized value
     * @param remoteTransportVersion The transport version to use for serialization
     * @param <E> The remote enum type
     */
    private <E extends Enum<E>> void testReadWriteEnum(
        Decision.Type value,
        Class<E> remoteEnum,
        E expectedSerialisedValue,
        TransportVersion remoteTransportVersion
    ) throws IOException {
        testReadWriteEnum(value, remoteEnum, expectedSerialisedValue, value, remoteTransportVersion);
    }

    /**
     * Test the reading and writing of an enum to a specific transport version
     *
     * @param value The value to write
     * @param remoteEnum The enum to use for deserialization
     * @param expectedSerialisedValue The expected deserialized value
     * @param roundTripValue The expected deserialized value after round-tripping
     * @param remoteTransportVersion The transport version to use for serialization
     * @param <E> The remote enum type
     */
    private <E extends Enum<E>> void testReadWriteEnum(
        Decision.Type value,
        Class<E> remoteEnum,
        E expectedSerialisedValue,
        Decision.Type roundTripValue,
        TransportVersion remoteTransportVersion
    ) throws IOException {
        final var output = new BytesStreamOutput();
        output.setTransportVersion(remoteTransportVersion);
        value.writeTo(output);
        assertEquals(expectedSerialisedValue, output.bytes().streamInput().readEnum(remoteEnum));
        expectValue(roundTripValue, remoteTransportVersion, output.bytes());
    }

    /**
     * Expect a value to be deserialized when read as a specific transport version
     *
     * @param expected The {@link Decision.Type} we expect to read
     * @param readAsTransportVersion The TransportVersion to interpret the bytes as coming from
     * @param bytes The bytes to read
     */
    private void expectValue(Decision.Type expected, TransportVersion readAsTransportVersion, BytesReference bytes) throws IOException {
        final var currentValueInput = bytes.streamInput();
        currentValueInput.setTransportVersion(readAsTransportVersion);
        assertEquals(expected, Type.readFrom(currentValueInput));
    }
}
