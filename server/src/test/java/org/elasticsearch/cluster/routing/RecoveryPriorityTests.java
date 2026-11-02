/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

public class RecoveryPriorityTests extends ESTestCase {

    public void testSerialization() throws IOException {
        for (RecoveryPriority originalPriority : RecoveryPriority.values()) {
            RecoveryPriority copyPriority = copyInstance(
                originalPriority,
                new NamedWriteableRegistry(List.of()),
                (out, priority) -> priority.writeTo(out),
                RecoveryPriority::readFrom,
                TransportVersion.current()
            );
            assertEquals(originalPriority, copyPriority);
        }
    }

    /// A copy of the [RecoveryPriority] enum before the introduction of [RecoveryPriority#UNKNOWN] and
    /// [RecoveryPriority#UNASSIGNED_NEW_PRIMARY].
    private enum LegacyRecoveryPriority {
        UNASSIGNED_EXISTING,
        UNASSIGNED_NEW,
        RELOCATION_CAN_REMAIN_NO,
        RELOCATION_CAN_REMAIN_NOT_PREFERRED,
        RELOCATE_REBALANCING
    }

    /// Serializes a [RecoveryPriority] using a [TransportVersion] from before the introduction of
    /// [RecoveryPriority#UNKNOWN] and [RecoveryPriority#UNASSIGNED_NEW_PRIMARY], deserializes them the way that
    /// we used to before that change to get a [LegacyRecoveryPriority], and assert that it is the equivalent or closest value.
    public void testSerialization_legacyReader() throws IOException {
        for (RecoveryPriority originalPriority : RecoveryPriority.values()) {
            TransportVersion version = ShardRouting.RECOVERY_PRIORITY_TRANSPORT_VERSION;
            try (BytesStreamOutput output = new BytesStreamOutput()) {
                output.setTransportVersion(version);
                originalPriority.writeTo(output);
                try (StreamInput in = output.bytes().streamInput()) {
                    LegacyRecoveryPriority copyPriority = in.readEnum(LegacyRecoveryPriority.class);
                    LegacyRecoveryPriority expectedPriority = switch (originalPriority) {
                        case UNASSIGNED_NEW_PRIMARY, UNASSIGNED_UNEXPECTED -> LegacyRecoveryPriority.UNASSIGNED_EXISTING;
                        case UNASSIGNED_EXPECTED, UNASSIGNED_RESTORING, UNASSIGNED_PERFORMANCE_SCALING ->
                            LegacyRecoveryPriority.UNASSIGNED_NEW;
                        case RELOCATION_CAN_REMAIN_NO -> LegacyRecoveryPriority.RELOCATION_CAN_REMAIN_NO;
                        case RELOCATION_CAN_REMAIN_NOT_PREFERRED -> LegacyRecoveryPriority.RELOCATION_CAN_REMAIN_NOT_PREFERRED;
                        case RELOCATE_REBALANCING, UNKNOWN -> LegacyRecoveryPriority.RELOCATE_REBALANCING;
                    };
                    assertEquals(expectedPriority, copyPriority);
                }
            }
        }
    }

    /// Serializes a [LegacyRecoveryPriority] the way we used to before the introduction of [RecoveryPriority#UNKNOWN] and
    /// [RecoveryPriority#UNASSIGNED_NEW_PRIMARY], deserializes them to get a [RecoveryPriority] using a
    /// [TransportVersion] from before that change, and assert that it is an equivalent value.
    public void testSerialization_legacyWriter() throws IOException {
        for (LegacyRecoveryPriority originalPriority : LegacyRecoveryPriority.values()) {
            TransportVersion version = ShardRouting.RECOVERY_PRIORITY_TRANSPORT_VERSION;
            try (BytesStreamOutput output = new BytesStreamOutput()) {
                output.writeEnum(originalPriority);
                try (StreamInput in = output.bytes().streamInput()) {
                    in.setTransportVersion(version);
                    RecoveryPriority copyPriority = RecoveryPriority.readFrom(in);
                    RecoveryPriority expectedPriority = switch (originalPriority) {
                        case UNASSIGNED_EXISTING -> RecoveryPriority.UNASSIGNED_UNEXPECTED;
                        case UNASSIGNED_NEW -> RecoveryPriority.UNASSIGNED_EXPECTED;
                        case RELOCATION_CAN_REMAIN_NO -> RecoveryPriority.RELOCATION_CAN_REMAIN_NO;
                        case RELOCATION_CAN_REMAIN_NOT_PREFERRED -> RecoveryPriority.RELOCATION_CAN_REMAIN_NOT_PREFERRED;
                        case RELOCATE_REBALANCING -> RecoveryPriority.RELOCATE_REBALANCING;
                    };
                    assertEquals(expectedPriority, copyPriority);
                }
            }
        }
    }

    /// A copy of the [RecoveryPriority] enum before the introduction of
    /// [RecoveryPriority#UNASSIGNED_RESTORING] and [RecoveryPriority#UNASSIGNED_PERFORMANCE_SCALING].
    private enum RecoveryPriorityBeforeNonCriticalUnassigned {
        UNASSIGNED_NEW_PRIMARY,
        UNASSIGNED_UNEXPECTED,
        UNASSIGNED_EXPECTED,
        RELOCATION_CAN_REMAIN_NO,
        RELOCATION_CAN_REMAIN_NOT_PREFERRED,
        RELOCATE_REBALANCING,
        UNKNOWN
    }

    /// Serializes a [RecoveryPriority] using a [TransportVersion] from before the introduction of
    /// [RecoveryPriority#UNASSIGNED_RESTORING] and [RecoveryPriority#UNASSIGNED_PERFORMANCE_SCALING],
    /// deserializes them the way that we used to before that change to get a [RecoveryPriorityBeforeNonCriticalUnassigned],
    /// and assert that it is the equivalent or closest value.
    public void testSerialization_beforeNonCriticalUnassignedReader() throws IOException {
        for (final var originalPriority : RecoveryPriority.values()) {
            final var version = RecoveryPriority.RECOVERY_PRIORITY_UNKNOWN_AND_UNASSIGNED_NEW_PRIMARY_TRANSPORT_VERSION;
            try (BytesStreamOutput output = new BytesStreamOutput()) {
                output.setTransportVersion(version);
                originalPriority.writeTo(output);
                try (StreamInput in = output.bytes().streamInput()) {
                    final var copyPriority = in.readEnum(RecoveryPriorityBeforeNonCriticalUnassigned.class);
                    final var expectedPriority = switch (originalPriority) {
                        case UNASSIGNED_NEW_PRIMARY -> RecoveryPriorityBeforeNonCriticalUnassigned.UNASSIGNED_NEW_PRIMARY;
                        case UNASSIGNED_UNEXPECTED -> RecoveryPriorityBeforeNonCriticalUnassigned.UNASSIGNED_UNEXPECTED;
                        case UNASSIGNED_EXPECTED, UNASSIGNED_RESTORING, UNASSIGNED_PERFORMANCE_SCALING ->
                            RecoveryPriorityBeforeNonCriticalUnassigned.UNASSIGNED_EXPECTED;
                        case RELOCATION_CAN_REMAIN_NO -> RecoveryPriorityBeforeNonCriticalUnassigned.RELOCATION_CAN_REMAIN_NO;
                        case RELOCATION_CAN_REMAIN_NOT_PREFERRED ->
                            RecoveryPriorityBeforeNonCriticalUnassigned.RELOCATION_CAN_REMAIN_NOT_PREFERRED;
                        case RELOCATE_REBALANCING -> RecoveryPriorityBeforeNonCriticalUnassigned.RELOCATE_REBALANCING;
                        case UNKNOWN -> RecoveryPriorityBeforeNonCriticalUnassigned.UNKNOWN;
                    };
                    assertEquals(expectedPriority, copyPriority);
                }
            }
        }
    }

    /// Serializes a [RecoveryPriorityBeforeNonCriticalUnassigned] the way we used to before the introduction of
    /// [RecoveryPriority#UNASSIGNED_RESTORING] and [RecoveryPriority#UNASSIGNED_PERFORMANCE_SCALING],
    /// deserializes them to get a [RecoveryPriority] using a [TransportVersion] from before that change, and assert
    /// that it is an equivalent value.
    public void testSerialization_beforeNonCriticalUnassignedWriter() throws IOException {
        for (final var originalPriority : RecoveryPriorityBeforeNonCriticalUnassigned.values()) {
            final var version = RecoveryPriority.RECOVERY_PRIORITY_UNKNOWN_AND_UNASSIGNED_NEW_PRIMARY_TRANSPORT_VERSION;
            try (BytesStreamOutput output = new BytesStreamOutput()) {
                output.writeEnum(originalPriority);
                try (StreamInput in = output.bytes().streamInput()) {
                    in.setTransportVersion(version);
                    final var copyPriority = RecoveryPriority.readFrom(in);
                    final var expectedPriority = switch (originalPriority) {
                        case UNASSIGNED_NEW_PRIMARY -> RecoveryPriority.UNASSIGNED_NEW_PRIMARY;
                        case UNASSIGNED_UNEXPECTED -> RecoveryPriority.UNASSIGNED_UNEXPECTED;
                        case UNASSIGNED_EXPECTED -> RecoveryPriority.UNASSIGNED_EXPECTED;
                        case RELOCATION_CAN_REMAIN_NO -> RecoveryPriority.RELOCATION_CAN_REMAIN_NO;
                        case RELOCATION_CAN_REMAIN_NOT_PREFERRED -> RecoveryPriority.RELOCATION_CAN_REMAIN_NOT_PREFERRED;
                        case RELOCATE_REBALANCING -> RecoveryPriority.RELOCATE_REBALANCING;
                        case UNKNOWN -> RecoveryPriority.UNKNOWN;
                    };
                    assertEquals(expectedPriority, copyPriority);
                }
            }
        }
    }

    /// `ThrottlingRecoveryService` only peeks at the head of its queue of pending recoveries, which is only correct if
    /// every critical [RecoveryPriority] ranks above every non-critical one.
    public void testCriticalRecoveryPrioritiesRankAboveNonCriticalOnes() {
        final var priorities = RecoveryPriority.values();
        for (final var higher : priorities) {
            for (final var lower : priorities) {
                if (higher.ordinal() < lower.ordinal() && lower.isCritical()) {
                    assertTrue("critical priority " + lower + " ranks below non-critical priority " + higher, higher.isCritical());
                }
            }
        }
    }
}
