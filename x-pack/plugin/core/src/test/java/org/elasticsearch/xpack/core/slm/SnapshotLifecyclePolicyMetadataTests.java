/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.slm;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.slm.SnapshotInvocationRecordTests.randomSnapshotInvocationRecord;

public class SnapshotLifecyclePolicyMetadataTests extends AbstractSerializingTestCase<SnapshotLifecyclePolicyMetadata> {
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
        int headerCount = randomIntBetween(1,10);
        for (int i = 0; i < headerCount; i++) {
            headers.put(randomAlphaOfLengthBetween(5,10), randomAlphaOfLengthBetween(5,10));
        }
        return headers;
    }

    @Override
    protected Writeable.Reader<SnapshotLifecyclePolicyMetadata> instanceReader() {
        return SnapshotLifecyclePolicyMetadata::new;
    }

    @Override
    protected SnapshotLifecyclePolicyMetadata mutateInstance(SnapshotLifecyclePolicyMetadata instance) throws IOException {
        switch (between(0, 5)) {
            case 0:
                return SnapshotLifecyclePolicyMetadata.builder(instance)
                    .setPolicy(randomValueOtherThan(instance.getPolicy(), () -> randomSnapshotLifecyclePolicy(randomAlphaOfLength(10))))
                    .build();
            case 1:
                return SnapshotLifecyclePolicyMetadata.builder(instance)
                    .setVersion(randomValueOtherThan(instance.getVersion(), ESTestCase::randomNonNegativeLong))
                    .build();
            case 2:
                return SnapshotLifecyclePolicyMetadata.builder(instance)
                    .setModifiedDate(randomValueOtherThan(instance.getModifiedDate(), ESTestCase::randomNonNegativeLong))
                    .build();
            case 3:
                return SnapshotLifecyclePolicyMetadata.builder(instance)
                    .setHeaders(randomValueOtherThan(instance.getHeaders(), SnapshotLifecyclePolicyMetadataTests::randomHeaders))
                    .build();
            case 4:
                return SnapshotLifecyclePolicyMetadata.builder(instance)
                    .setLastSuccess(randomValueOtherThan(instance.getLastSuccess(),
                        SnapshotInvocationRecordTests::randomSnapshotInvocationRecord))
                    .build();
            case 5:
                return SnapshotLifecyclePolicyMetadata.builder(instance)
                    .setLastFailure(randomValueOtherThan(instance.getLastFailure(),
                        SnapshotInvocationRecordTests::randomSnapshotInvocationRecord))
                    .build();
            default:
                throw new AssertionError("failure, got illegal switch case");
        }
    }

    public static SnapshotLifecyclePolicyMetadata createRandomPolicyMetadata(String policyId) {
        SnapshotLifecyclePolicyMetadata.Builder builder = SnapshotLifecyclePolicyMetadata.builder()
            .setPolicy(randomSnapshotLifecyclePolicy(policyId))
            .setVersion(randomNonNegativeLong())
            .setModifiedDate(randomNonNegativeLong());
        if (randomBoolean()) {
            builder.setHeaders(randomHeaders());
        }
        if (randomBoolean()) {
            builder.setLastSuccess(randomSnapshotInvocationRecord());
        }
        if (randomBoolean()) {
            builder.setLastFailure(randomSnapshotInvocationRecord());
        }
        return builder.build();
    }

    public static SnapshotLifecyclePolicy randomSnapshotLifecyclePolicy(String policyId) {
        Map<String, Object> config = new HashMap<>();
        for (int i = 0; i < randomIntBetween(2, 5); i++) {
            config.put(randomAlphaOfLength(4), randomAlphaOfLength(4));
        }
        return new SnapshotLifecyclePolicy(policyId,
            randomAlphaOfLength(4),
            randomSchedule(),
            randomAlphaOfLength(4),
            config,
            randomRetention());
    }

    public static SnapshotRetentionConfiguration randomRetention() {
        return rarely() ? null : new SnapshotRetentionConfiguration(
            rarely() ? null : TimeValue.parseTimeValue(randomTimeValue(), "random retention generation"),
            rarely() ? null : randomIntBetween(1, 10),
            rarely() ? null : randomIntBetween(15, 30));
    }

    public static String randomSchedule() {
        return randomIntBetween(0, 59) + " " +
            randomIntBetween(0, 59) + " " +
            randomIntBetween(0, 12) + " * * ?";
    }
}
