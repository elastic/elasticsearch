/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.slm;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.slm.SnapshotInvocationRecordTests.randomSnapshotInvocationRecord;

public class SnapshotLifecyclePolicyMetadataTests extends AbstractXContentSerializingTestCase<SnapshotLifecyclePolicyMetadata> {
    private String policyId;

    @Override
    protected SnapshotLifecyclePolicyMetadata doParseInstance(XContentParser parser) throws IOException {
        return SnapshotLifecyclePolicyMetadata.PARSER.apply(parser, policyId);
    }

    @Override
    protected SnapshotLifecyclePolicyMetadata createTestInstance() {
        policyId = randomAlphaOfLength(5);
        return createRandomPolicyMetadata(policyId);
    }

    private static Map<String, String> randomHeaders() {
        Map<String, String> headers = new HashMap<>();
        int headerCount = randomIntBetween(1, 10);
        for (int i = 0; i < headerCount; i++) {
            headers.put(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10));
        }
        return headers;
    }

    @Override
    protected Writeable.Reader<SnapshotLifecyclePolicyMetadata> instanceReader() {
        return SnapshotLifecyclePolicyMetadata::new;
    }

    @Override
    protected SnapshotLifecyclePolicyMetadata mutateInstance(SnapshotLifecyclePolicyMetadata instance) {
        return switch (between(0, 5)) {
            case 0 -> SnapshotLifecyclePolicyMetadata.builder(instance)
                .setPolicy(randomValueOtherThan(instance.getPolicy(), () -> randomSnapshotLifecyclePolicy(randomAlphaOfLength(10))))
                .build();
            case 1 -> SnapshotLifecyclePolicyMetadata.builder(instance)
                .setVersion(randomValueOtherThan(instance.getVersion(), ESTestCase::randomNonNegativeLong))
                .build();
            case 2 -> SnapshotLifecyclePolicyMetadata.builder(instance)
                .setModifiedDate(randomValueOtherThan(instance.getModifiedDate(), ESTestCase::randomNonNegativeLong))
                .build();
            case 3 -> SnapshotLifecyclePolicyMetadata.builder(instance)
                .setHeaders(randomValueOtherThan(instance.getHeaders(), SnapshotLifecyclePolicyMetadataTests::randomHeaders))
                .build();
            case 4 -> SnapshotLifecyclePolicyMetadata.builder(instance)
                .setLastSuccess(
                    randomValueOtherThan(instance.getLastSuccess(), SnapshotInvocationRecordTests::randomSnapshotInvocationRecord)
                )
                .build();
            case 5 -> SnapshotLifecyclePolicyMetadata.builder(instance)
                .setLastFailure(
                    randomValueOtherThan(instance.getLastFailure(), SnapshotInvocationRecordTests::randomSnapshotInvocationRecord)
                )
                .build();
            default -> throw new AssertionError("failure, got illegal switch case");
        };
    }

    public static SnapshotLifecyclePolicyMetadata createRandomPolicyMetadata(String policyId) {
        SnapshotLifecyclePolicyMetadata.Builder builder = SnapshotLifecyclePolicyMetadata.builder()
            .setPolicy(randomSnapshotLifecyclePolicy(policyId))
            .setVersion(randomNonNegativeLong())
            .setModifiedDate(randomModifiedTime());
        if (randomBoolean()) {
            builder.setHeaders(randomHeaders());
        }
        boolean hasSuccess = randomBoolean();
        if (hasSuccess) {
            builder.setLastSuccess(randomSnapshotInvocationRecord());
            builder.setInvocationsSinceLastSuccess(0L);
        }
        if (randomBoolean()) {
            builder.setLastFailure(randomSnapshotInvocationRecord());
            if (hasSuccess) {
                builder.setInvocationsSinceLastSuccess(randomLongBetween(1, Long.MAX_VALUE));
            }
        }
        return builder.build();
    }

    public static SnapshotLifecyclePolicy randomSnapshotLifecyclePolicy(String policyId) {
        Map<String, Object> config = new HashMap<>();
        for (int i = 0; i < randomIntBetween(2, 5); i++) {
            config.put(randomAlphaOfLength(4), randomAlphaOfLength(4));
        }

        return new SnapshotLifecyclePolicy(
            policyId,
            randomAlphaOfLength(4),
            randomSchedule(),
            randomAlphaOfLength(4),
            config,
            randomRetention()
        );
    }

    public static SnapshotRetentionConfiguration randomRetention() {
        return rarely()
            ? null
            : new SnapshotRetentionConfiguration(
                rarely() ? null : randomTimeValue(),
                rarely() ? null : randomIntBetween(1, 10),
                rarely() ? null : randomIntBetween(15, 30)
            );
    }

    public static String randomCronSchedule() {
        return randomIntBetween(0, 59) + " " + randomIntBetween(0, 59) + " " + randomIntBetween(0, 12) + " * * ?";
    }

    public static String randomTimeValueString() {
        // restrict to intervals greater than slm.minimum_interval value of 15 minutes
        Duration minInterval = Duration.ofMinutes(15);
        Map<String, Long> unitMinVal = Map.of(
            "nanos",
            minInterval.toNanos(),
            "micros",
            minInterval.toNanos() * 1000,
            "ms",
            minInterval.toMillis(),
            "s",
            minInterval.toSeconds(),
            "m",
            minInterval.toMinutes(),
            "h",
            minInterval.toHours(),
            "d",
            minInterval.toDays()
        );
        var unit = randomFrom(unitMinVal.keySet());
        long minVal = Math.max(1, unitMinVal.get(unit));
        long value = randomLongBetween(minVal, 1000 * minVal);
        return value + unit;
    }

    public static String randomSchedule() {
        return randomBoolean() ? randomCronSchedule() : randomTimeValueString();
    }

    public static long randomModifiedTime() {
        // if modified time is after the current time, validation will fail
        return randomLongBetween(0, Clock.systemUTC().millis());
    }
}
