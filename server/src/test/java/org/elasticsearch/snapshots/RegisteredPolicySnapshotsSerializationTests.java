/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RegisteredPolicySnapshotsSerializationTests extends AbstractChunkedSerializingTestCase<RegisteredPolicySnapshots> {
    @Override
    protected RegisteredPolicySnapshots doParseInstance(XContentParser parser) throws IOException {
        return RegisteredPolicySnapshots.parse(parser);
    }

    @Override
    protected Writeable.Reader<RegisteredPolicySnapshots> instanceReader() {
        return RegisteredPolicySnapshots::new;
    }

    @Override
    protected RegisteredPolicySnapshots createTestInstance() {
        return randomRegisteredPolicySnapshots();
    }

    @Override
    protected RegisteredPolicySnapshots mutateInstance(RegisteredPolicySnapshots instance) throws IOException {
        if (instance.getSnapshots().isEmpty()) {
            return new RegisteredPolicySnapshots(List.of(randomPolicySnapshot()));
        }

        final int randIndex = between(0, instance.getSnapshots().size() - 1);
        final RegisteredPolicySnapshots.PolicySnapshot policySnapshot = instance.getSnapshots().get(randIndex);

        String policy = policySnapshot.getPolicy();
        String snapshotName = policySnapshot.getSnapshotId().getName();
        String snapshotUUID = policySnapshot.getSnapshotId().getUUID();

        switch (between(0, 2)) {
            case 0 -> {
                policy = randomPolicy();
            }
            case 1 -> {
                snapshotName = randomValueOtherThan(snapshotName, ESTestCase::randomIdentifier);
            }
            case 2 -> {
                snapshotUUID = randomValueOtherThan(snapshotName, ESTestCase::randomUUID);
            }
            default -> throw new AssertionError("failure, got illegal switch case");
        }

        List<RegisteredPolicySnapshots.PolicySnapshot> newSnapshots = new ArrayList<>(instance.getSnapshots());
        newSnapshots.set(randIndex, new RegisteredPolicySnapshots.PolicySnapshot(policy, new SnapshotId(snapshotName, snapshotUUID)));
        return new RegisteredPolicySnapshots(newSnapshots);
    }

    private RegisteredPolicySnapshots randomRegisteredPolicySnapshots() {
        final List<RegisteredPolicySnapshots.PolicySnapshot> snapshots = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(0, 20); i++) {
            snapshots.add(randomPolicySnapshot());
        }
        return new RegisteredPolicySnapshots(snapshots);
    }

    private String randomPolicy() {
        return "policy-" + randomIntBetween(0, 20);
    }

    private RegisteredPolicySnapshots.PolicySnapshot randomPolicySnapshot() {
        SnapshotId snapshotId = new SnapshotId(randomIdentifier(), randomUUID());
        return new RegisteredPolicySnapshots.PolicySnapshot(randomPolicy(), snapshotId);
    }
}
