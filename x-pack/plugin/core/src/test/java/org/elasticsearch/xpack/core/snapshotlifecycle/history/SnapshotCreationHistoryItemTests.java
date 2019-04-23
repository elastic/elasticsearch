/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.snapshotlifecycle.history;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SnapshotCreationHistoryItemTests extends AbstractSerializingTestCase<SnapshotCreationHistoryItem> {

    @Override
    protected SnapshotCreationHistoryItem doParseInstance(XContentParser parser) throws IOException {
        return SnapshotCreationHistoryItem.parse(parser, this.getClass().getCanonicalName());
    }

    @Override
    protected Writeable.Reader<SnapshotCreationHistoryItem> instanceReader() {
        return SnapshotCreationHistoryItem::new;
    }

    @Override
    protected SnapshotCreationHistoryItem createTestInstance() {
        long timestamp = randomNonNegativeLong();
        String policyId = randomAlphaOfLengthBetween(5, 10);
        String repository = randomAlphaOfLengthBetween(5, 10);
        String snapshotName = randomAlphaOfLengthBetween(5, 10);
        String operation = randomAlphaOfLengthBetween(5, 10);
        boolean success = randomBoolean();
        Map<String, Object> snapshotConfig = randomSnapshotConfiguration();
        String errorDetails = randomBoolean() ? null : randomAlphaOfLengthBetween(10, 20);

        return new SnapshotCreationHistoryItem(timestamp, policyId, repository, snapshotName, operation, success, snapshotConfig,
            errorDetails);
    }

    @Override
    protected SnapshotCreationHistoryItem mutateInstance(SnapshotCreationHistoryItem instance) {
        final int branch = between(0, 7);
        switch (branch) {
            case 0: // New timestamp
                return new SnapshotCreationHistoryItem(
                    randomValueOtherThan(instance.getTimestamp(), ESTestCase::randomNonNegativeLong),
                    instance.getPolicyId(), instance.getRepository(), instance.getSnapshotName(), instance.getOperation(),
                    instance.isSuccess(), instance.getSnapshotConfiguration(), instance.getErrorDetails());
            case 1: // new policyId
                return new SnapshotCreationHistoryItem(instance.getTimestamp(),
                    randomValueOtherThan(instance.getPolicyId(), () -> randomAlphaOfLengthBetween(5, 10)),
                    instance.getSnapshotName(), instance.getRepository(), instance.getOperation(), instance.isSuccess(),
                    instance.getSnapshotConfiguration(), instance.getErrorDetails());
            case 2: // new repo name
                return new SnapshotCreationHistoryItem(instance.getTimestamp(), instance.getPolicyId(), instance.getSnapshotName(),
                    randomValueOtherThan(instance.getRepository(), () -> randomAlphaOfLengthBetween(5, 10)),
                    instance.getOperation(), instance.isSuccess(), instance.getSnapshotConfiguration(), instance.getErrorDetails());
            case 3:
                return new SnapshotCreationHistoryItem(instance.getTimestamp(), instance.getPolicyId(), instance.getRepository(),
                    randomValueOtherThan(instance.getSnapshotName(), () -> randomAlphaOfLengthBetween(5, 10)),
                    instance.getOperation(), instance.isSuccess(), instance.getSnapshotConfiguration(), instance.getErrorDetails());
            case 4:
                return new SnapshotCreationHistoryItem(instance.getTimestamp(), instance.getPolicyId(), instance.getRepository(),
                    instance.getSnapshotName(),
                    randomValueOtherThan(instance.getOperation(), () -> randomAlphaOfLengthBetween(5, 10)),
                    instance.isSuccess(), instance.getSnapshotConfiguration(), instance.getErrorDetails());
            case 5:
                return new SnapshotCreationHistoryItem(instance.getTimestamp(), instance.getPolicyId(), instance.getRepository(),
                    instance.getSnapshotName(),
                    instance.getOperation(),
                    instance.isSuccess() == false,
                    instance.getSnapshotConfiguration(), instance.getErrorDetails());
            case 6:
                return new SnapshotCreationHistoryItem(instance.getTimestamp(), instance.getPolicyId(), instance.getRepository(),
                    instance.getSnapshotName(), instance.getOperation(), instance.isSuccess(),
                    randomValueOtherThan(instance.getSnapshotConfiguration(),
                        SnapshotCreationHistoryItemTests::randomSnapshotConfiguration),
                    instance.getErrorDetails());
            case 7:
                return new SnapshotCreationHistoryItem(instance.getTimestamp(), instance.getPolicyId(), instance.getRepository(),
                    instance.getSnapshotName(), instance.getOperation(), instance.isSuccess(), instance.getSnapshotConfiguration(),
                    randomValueOtherThan(instance.getErrorDetails(), () -> randomAlphaOfLengthBetween(10, 20)));
            default:
                throw new IllegalArgumentException("illegal randomization: " + branch);
        }
    }

    public static Map<String, Object> randomSnapshotConfiguration() {
        Map<String, Object> configuration = new HashMap<>();
        configuration.put("indices", Arrays.asList(generateRandomStringArray(1, 10, false, false)));
        if (frequently()) {
            configuration.put("ignore_unavailable", randomBoolean());
        }
        if (frequently()) {
            configuration.put("include_global_state", randomBoolean());
        }
        if (frequently()) {
            configuration.put("partial", randomBoolean());
        }
        return configuration;
    }
}
