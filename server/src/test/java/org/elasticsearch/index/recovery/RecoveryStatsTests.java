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
            stats.incCurrentAsSource();
        }
        for (int i = 0; i < randomIntBetween(0, 3); i++) {
            stats.incCurrentAsTarget();
        }
        for (int i = 0; i < randomIntBetween(0, 3); i++) {
            stats.incCurrentAsSourceQueued();
        }
        stats.addThrottleTime(randomNonNegativeLong());
        return stats;
    }

    @Override
    protected RecoveryStats mutateInstance(RecoveryStats instance) throws IOException {
        final var mutated = new RecoveryStats();
        mutated.add(instance);
        return switch (between(0, 3)) {
            case 0 -> {
                if (randomBoolean() && mutated.currentAsSource() > 0) {
                    mutated.decCurrentAsSource();
                } else {
                    mutated.incCurrentAsSource();
                }
                yield mutated;
            }
            case 1 -> {
                if (randomBoolean() && mutated.currentAsTarget() > 0) {
                    mutated.decCurrentAsTarget();
                } else {
                    mutated.incCurrentAsTarget();
                }
                yield mutated;
            }
            case 2 -> {
                if (randomBoolean() && mutated.currentAsSourceQueued() > 0) {
                    mutated.decCurrentAsSourceQueued();
                } else {
                    mutated.incCurrentAsSourceQueued();
                }
                yield mutated;
            }
            default -> {
                mutated.addThrottleTime(randomLongBetween(1, Long.MAX_VALUE - instance.throttleTime().nanos()));
                yield mutated;
            }
        };
    }

    public void testBwcBeforeQueuedAsSourceStats() throws IOException {
        final TransportVersion queuedAsSourceVersion = TransportVersion.fromName("recovery_source_queued_stats");
        final TransportVersion beforeQueuedAsSource = TransportVersionUtils.randomVersionNotSupporting(queuedAsSourceVersion);
        final TransportVersion atOrAfterQueuedAsSource = TransportVersionUtils.randomVersionSupporting(queuedAsSourceVersion);

        final var stats = new RecoveryStats();
        stats.incCurrentAsSource();
        stats.incCurrentAsTarget();
        stats.incCurrentAsSourceQueued();
        stats.addThrottleTime(randomNonNegativeLong());

        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(beforeQueuedAsSource);
            stats.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                in.setTransportVersion(beforeQueuedAsSource);
                final var deserialized = new RecoveryStats(in);
                assertEquals(stats.currentAsSource(), deserialized.currentAsSource());
                assertEquals(stats.currentAsTarget(), deserialized.currentAsTarget());
                assertEquals(stats.throttleTime(), deserialized.throttleTime());
                assertEquals(0, deserialized.currentAsSourceQueued());
            }
        }

        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(atOrAfterQueuedAsSource);
            stats.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                in.setTransportVersion(atOrAfterQueuedAsSource);
                final var deserialized = new RecoveryStats(in);
                assertEquals(stats.currentAsSource(), deserialized.currentAsSource());
                assertEquals(stats.currentAsTarget(), deserialized.currentAsTarget());
                assertEquals(stats.throttleTime(), deserialized.throttleTime());
                assertEquals(stats.currentAsSourceQueued(), deserialized.currentAsSourceQueued());
            }
        }
    }
}
