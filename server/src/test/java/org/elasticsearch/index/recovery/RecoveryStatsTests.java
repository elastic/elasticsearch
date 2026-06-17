/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.recovery;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;

public class RecoveryStatsTests extends AbstractWireSerializingTestCase<RecoveryStats> {

    @Override
    protected Writeable.Reader<RecoveryStats> instanceReader() {
        return RecoveryStats::new;
    }

    @Override
    protected RecoveryStats createTestInstance() {
        final var stats = new RecoveryStats();
        for (int i = 0; i < randomIntBetween(0, 3); i++) {
            stats.sourceRecoveryStarted();
        }
        for (int i = 0; i < randomIntBetween(0, 3); i++) {
            stats.sourceRecoveryQueued();
        }
        for (int i = 0; i < randomIntBetween(0, 3); i++) {
            stats.targetRecoveryQueued(RecoverySource.Type.PEER);
            stats.targetRecoveryDequeuedAndStarted(RecoverySource.Type.PEER);
        }
        for (int i = 0; i < randomIntBetween(0, 3); i++) {
            stats.targetRecoveryQueued(RecoverySource.Type.PEER);
        }
        for (int i = 0; i < randomIntBetween(0, 3); i++) {
            stats.targetRecoveryQueued(RecoverySource.Type.EMPTY_STORE);
            stats.targetRecoveryDequeuedAndStarted(RecoverySource.Type.EMPTY_STORE);
        }
        for (int i = 0; i < randomIntBetween(0, 3); i++) {
            stats.targetRecoveryQueued(RecoverySource.Type.EXISTING_STORE);
        }
        stats.addThrottleTime(randomNonNegativeLong());
        return stats;
    }

    @Override
    protected RecoveryStats mutateInstance(RecoveryStats instance) throws IOException {
        final var mutated = new RecoveryStats();
        mutated.add(instance);
        return switch (between(0, 6)) {
            case 0 -> {
                if (randomBoolean() && mutated.currentAsSource() > 0) {
                    mutated.sourceRecoveryCompleted();
                } else {
                    mutated.sourceRecoveryStarted();
                }
                yield mutated;
            }
            case 1 -> {
                if (randomBoolean() && mutated.currentAsSourceQueued() > 0) {
                    mutated.sourceQueuedRecoveryDiscarded();
                } else {
                    mutated.sourceRecoveryQueued();
                }
                yield mutated;
            }
            case 2 -> {
                if (randomBoolean() && mutated.currentAsTarget() > 0) {
                    mutated.targetRecoveryCompleted(RecoverySource.Type.PEER);
                } else {
                    mutated.targetRecoveryQueued(RecoverySource.Type.PEER);
                    mutated.targetRecoveryDequeuedAndStarted(RecoverySource.Type.PEER);
                }
                yield mutated;
            }
            case 3 -> {
                if (randomBoolean() && mutated.currentAsTargetQueued() > 0) {
                    mutated.targetQueuedRecoveryDiscarded(RecoverySource.Type.PEER);
                } else {
                    mutated.targetRecoveryQueued(RecoverySource.Type.PEER);
                }
                yield mutated;
            }
            case 4 -> {
                if (randomBoolean() && mutated.currentFromStore() > 0) {
                    mutated.targetRecoveryCompleted(RecoverySource.Type.EXISTING_STORE);
                } else {
                    mutated.targetRecoveryQueued(RecoverySource.Type.EXISTING_STORE);
                    mutated.targetRecoveryDequeuedAndStarted(RecoverySource.Type.EXISTING_STORE);
                }
                yield mutated;
            }
            case 5 -> {
                if (randomBoolean() && mutated.currentFromStoreQueued() > 0) {
                    mutated.targetQueuedRecoveryDiscarded(RecoverySource.Type.EMPTY_STORE);
                } else {
                    mutated.targetRecoveryQueued(RecoverySource.Type.EMPTY_STORE);
                }
                yield mutated;
            }
            default -> {
                mutated.addThrottleTime(randomLongBetween(1, Long.MAX_VALUE - instance.throttleTime().nanos()));
                yield mutated;
            }
        };
    }

    public void testBwc() throws IOException {
        final TransportVersion sourceQueuedVersion = TransportVersion.fromName("recovery_source_queued_stats");
        final TransportVersion storeAndQueuedVersion = TransportVersion.fromName("recovery_store_and_target_queued_stats");

        // Before source_queued_stats
        var stats = new RecoveryStats();
        stats.sourceRecoveryQueued();
        stats.sourceRecoveryStarted();
        stats.targetRecoveryQueued(RecoverySource.Type.PEER);
        stats.targetRecoveryQueued(RecoverySource.Type.PEER);
        stats.targetRecoveryDequeuedAndStarted(RecoverySource.Type.PEER);
        stats.addThrottleTime(randomNonNegativeLong());

        var deserialized = roundTrip(stats, TransportVersionUtils.randomVersionNotSupporting(sourceQueuedVersion));
        assertEquals(stats.currentAsSource(), deserialized.currentAsSource());
        assertEquals(stats.currentAsTarget(), deserialized.currentAsTarget());
        assertEquals(stats.throttleTime(), deserialized.throttleTime());
        assertEquals(0, deserialized.currentAsSourceQueued());
        assertEquals(0, deserialized.currentAsTargetQueued());
        assertEquals(0, deserialized.currentFromStore());
        assertEquals(0, deserialized.currentFromStoreQueued());

        // At source_queued_stats
        stats = new RecoveryStats();
        stats.sourceRecoveryQueued();
        stats.sourceRecoveryDequeuedAndStarted();
        stats.sourceRecoveryQueued();
        stats.targetRecoveryQueued(RecoverySource.Type.PEER);
        stats.targetRecoveryDequeuedAndStarted(RecoverySource.Type.PEER);
        stats.addThrottleTime(randomNonNegativeLong());

        deserialized = roundTrip(stats, sourceQueuedVersion);
        assertEquals(stats.currentAsSource(), deserialized.currentAsSource());
        assertEquals(stats.currentAsSourceQueued(), deserialized.currentAsSourceQueued());
        assertEquals(stats.currentAsTarget(), deserialized.currentAsTarget());
        assertEquals(stats.throttleTime(), deserialized.throttleTime());
        assertEquals(0, deserialized.currentAsTargetQueued());
        assertEquals(0, deserialized.currentFromStore());
        assertEquals(0, deserialized.currentFromStoreQueued());

        // At or after store_and_queued_stats
        stats = new RecoveryStats();
        stats.sourceRecoveryQueued();
        stats.sourceRecoveryDequeuedAndStarted();
        stats.sourceRecoveryQueued();
        stats.targetRecoveryQueued(RecoverySource.Type.PEER);
        stats.targetRecoveryDequeuedAndStarted(RecoverySource.Type.PEER);
        stats.targetRecoveryQueued(RecoverySource.Type.PEER);
        stats.targetRecoveryQueued(RecoverySource.Type.EXISTING_STORE);
        stats.targetRecoveryDequeuedAndStarted(RecoverySource.Type.EXISTING_STORE);
        stats.targetRecoveryQueued(RecoverySource.Type.EXISTING_STORE);
        stats.addThrottleTime(randomNonNegativeLong());

        assertEquals(stats, roundTrip(stats, TransportVersionUtils.randomVersionSupporting(storeAndQueuedVersion)));
    }

    private static RecoveryStats roundTrip(RecoveryStats stats, TransportVersion version) throws IOException {
        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            stats.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                in.setTransportVersion(version);
                return new RecoveryStats(in);
            }
        }
    }
}
