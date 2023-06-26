/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm.history;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SnapshotHistoryItemTests extends AbstractXContentSerializingTestCase<SnapshotHistoryItem> {

    @Override
    protected SnapshotHistoryItem doParseInstance(XContentParser parser) throws IOException {
        return SnapshotHistoryItem.parse(parser, this.getClass().getCanonicalName());
    }

    @Override
    protected Writeable.Reader<SnapshotHistoryItem> instanceReader() {
        return SnapshotHistoryItem::new;
    }

    @Override
    protected SnapshotHistoryItem createTestInstance() {
        long timestamp = randomNonNegativeLong();
        String policyId = randomAlphaOfLengthBetween(5, 10);
        String repository = randomAlphaOfLengthBetween(5, 10);
        String snapshotName = randomAlphaOfLengthBetween(5, 10);
        String operation = randomAlphaOfLengthBetween(5, 10);
        boolean success = randomBoolean();
        Map<String, Object> snapshotConfig = randomSnapshotConfiguration();
        String errorDetails = randomBoolean() ? null : randomAlphaOfLengthBetween(10, 20);

        return new SnapshotHistoryItem(timestamp, policyId, repository, snapshotName, operation, success, snapshotConfig, errorDetails);
    }

    @Override
    protected SnapshotHistoryItem mutateInstance(SnapshotHistoryItem instance) {
        final int branch = between(0, 7);
        return switch (branch) {
            case 0 -> // New timestamp
                new SnapshotHistoryItem(
                    randomValueOtherThan(instance.getTimestamp(), ESTestCase::randomNonNegativeLong),
                    instance.getPolicyId(),
                    instance.getRepository(),
                    instance.getSnapshotName(),
                    instance.getOperation(),
                    instance.isSuccess(),
                    instance.getSnapshotConfiguration(),
                    instance.getErrorDetails()
                );
            case 1 -> // new policyId
                new SnapshotHistoryItem(
                    instance.getTimestamp(),
                    randomValueOtherThan(instance.getPolicyId(), () -> randomAlphaOfLengthBetween(5, 10)),
                    instance.getSnapshotName(),
                    instance.getRepository(),
                    instance.getOperation(),
                    instance.isSuccess(),
                    instance.getSnapshotConfiguration(),
                    instance.getErrorDetails()
                );
            case 2 -> // new repo name
                new SnapshotHistoryItem(
                    instance.getTimestamp(),
                    instance.getPolicyId(),
                    instance.getSnapshotName(),
                    randomValueOtherThan(instance.getRepository(), () -> randomAlphaOfLengthBetween(5, 10)),
                    instance.getOperation(),
                    instance.isSuccess(),
                    instance.getSnapshotConfiguration(),
                    instance.getErrorDetails()
                );
            case 3 -> new SnapshotHistoryItem(
                instance.getTimestamp(),
                instance.getPolicyId(),
                instance.getRepository(),
                randomValueOtherThan(instance.getSnapshotName(), () -> randomAlphaOfLengthBetween(5, 10)),
                instance.getOperation(),
                instance.isSuccess(),
                instance.getSnapshotConfiguration(),
                instance.getErrorDetails()
            );
            case 4 -> new SnapshotHistoryItem(
                instance.getTimestamp(),
                instance.getPolicyId(),
                instance.getRepository(),
                instance.getSnapshotName(),
                randomValueOtherThan(instance.getOperation(), () -> randomAlphaOfLengthBetween(5, 10)),
                instance.isSuccess(),
                instance.getSnapshotConfiguration(),
                instance.getErrorDetails()
            );
            case 5 -> new SnapshotHistoryItem(
                instance.getTimestamp(),
                instance.getPolicyId(),
                instance.getRepository(),
                instance.getSnapshotName(),
                instance.getOperation(),
                instance.isSuccess() == false,
                instance.getSnapshotConfiguration(),
                instance.getErrorDetails()
            );
            case 6 -> new SnapshotHistoryItem(
                instance.getTimestamp(),
                instance.getPolicyId(),
                instance.getRepository(),
                instance.getSnapshotName(),
                instance.getOperation(),
                instance.isSuccess(),
                randomValueOtherThan(instance.getSnapshotConfiguration(), SnapshotHistoryItemTests::randomSnapshotConfiguration),
                instance.getErrorDetails()
            );
            case 7 -> new SnapshotHistoryItem(
                instance.getTimestamp(),
                instance.getPolicyId(),
                instance.getRepository(),
                instance.getSnapshotName(),
                instance.getOperation(),
                instance.isSuccess(),
                instance.getSnapshotConfiguration(),
                randomValueOtherThan(instance.getErrorDetails(), () -> randomAlphaOfLengthBetween(10, 20))
            );
            default -> throw new IllegalArgumentException("illegal randomization: " + branch);
        };
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
