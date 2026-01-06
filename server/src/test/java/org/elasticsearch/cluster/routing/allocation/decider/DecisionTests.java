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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EnumSerializationTestUtils;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.cluster.routing.allocation.decider.Decision.Type.ALLOCATION_DECISION_NOT_PREFERRED;
import static org.elasticsearch.cluster.routing.allocation.decider.Decision.Type.NO;
import static org.elasticsearch.cluster.routing.allocation.decider.Decision.Type.NOT_PREFERRED;
import static org.elasticsearch.cluster.routing.allocation.decider.Decision.Type.THROTTLE;
import static org.elasticsearch.cluster.routing.allocation.decider.Decision.Type.YES;

/**
 * A class for unit testing the {@link Decision} class.
 */
public class DecisionTests extends ESTestCase {

    private static final Type PREVIOUS_YES_VALUE = Type.values()[1];

    public void testTypeEnumOrder() {
        EnumSerializationTestUtils.assertEnumSerialization(Type.class, NO, NOT_PREFERRED, THROTTLE, YES);
    }

    public void testTypeHigherThan() {
        assertTrue(YES.higherThan(THROTTLE) && THROTTLE.higherThan(NOT_PREFERRED) && NOT_PREFERRED.higherThan(NO));
    }

    public void testTypeAllowed() {
        List.of(NOT_PREFERRED, YES).forEach(d -> assertTrue(d.assignmentAllowed()));
        List.of(NO, THROTTLE).forEach(d -> assertFalse(d.assignmentAllowed()));
    }

    public void testSerializationBackwardCompatibility() throws IOException {
        testReadWriteEnum(YES, YES, ALLOCATION_DECISION_NOT_PREFERRED);
        testReadWriteEnum(NOT_PREFERRED, THROTTLE, ALLOCATION_DECISION_NOT_PREFERRED);
        testReadWriteEnum(THROTTLE, NOT_PREFERRED, ALLOCATION_DECISION_NOT_PREFERRED);
        testReadWriteEnum(NO, NO, ALLOCATION_DECISION_NOT_PREFERRED);

        // Older versions, lossy serialization - remove when no longer supported
        assertFalse(TransportVersion.minimumCompatible().supports(ALLOCATION_DECISION_NOT_PREFERRED));
        // YES/NOT_PREFERRED turn into ordinal 1 which is YES in those versions, both round-trip to YES
        testReadWriteEnum(YES, PREVIOUS_YES_VALUE, TransportVersion.minimumCompatible());
        testReadWriteEnum(NOT_PREFERRED, PREVIOUS_YES_VALUE, YES, TransportVersion.minimumCompatible());
        // THROTTLE and NO are unchanged
        testReadWriteEnum(THROTTLE, THROTTLE, TransportVersion.minimumCompatible());
        testReadWriteEnum(NO, NO, TransportVersion.minimumCompatible());
    }

    private void testReadWriteEnum(Decision.Type value, Decision.Type expectedSerialisation, TransportVersion remoteTransportVersion)
        throws IOException {
        testReadWriteEnum(value, expectedSerialisation, value, remoteTransportVersion);
    }

    private void testReadWriteEnum(
        Decision.Type value,
        Decision.Type expectedSerialisation,
        Decision.Type roundTripValue,
        TransportVersion remoteTransportVersion
    ) throws IOException {
        final var output = new BytesStreamOutput();
        output.setTransportVersion(remoteTransportVersion);
        value.writeTo(output);
        expectValue(expectedSerialisation, TransportVersion.current(), output.bytes());
        expectValue(roundTripValue, remoteTransportVersion, output.bytes());
    }

    private void expectValue(Decision.Type expected, TransportVersion readAsTransportVersion, BytesReference bytes) throws IOException {
        final StreamInput currentValueInput = bytes.streamInput();
        currentValueInput.setTransportVersion(readAsTransportVersion);
        assertEquals(expected, Type.readFrom(currentValueInput));
    }
}
