/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RegisteredPolicySnapshotsSerializationTests extends AbstractChunkedSerializingTestCase<RegisteredPolicySnapshots> {
    public void testMaybeAdd() {
        {
            RegisteredPolicySnapshots.Builder builder = new RegisteredPolicySnapshots.Builder(RegisteredPolicySnapshots.EMPTY);
            var snap = new SnapshotId(randomAlphaOfLength(10), randomUUID());

            builder.addIfSnapshotIsSLMInitiated(null, snap);
            builder.addIfSnapshotIsSLMInitiated(Map.of(), snap);
            builder.addIfSnapshotIsSLMInitiated(Map.of("not_policy", "policy-10"), snap);
            builder.addIfSnapshotIsSLMInitiated(Map.of(SnapshotsService.POLICY_ID_METADATA_FIELD, 5), snap);

            // immutable map in Map.of doesn't allows nulls
            var meta = new HashMap<String, Object>();
            meta.put(SnapshotsService.POLICY_ID_METADATA_FIELD, null);
            builder.addIfSnapshotIsSLMInitiated(meta, snap);

            RegisteredPolicySnapshots registered = builder.build();
            assertTrue(registered.getSnapshots().isEmpty());
        }

        {
            RegisteredPolicySnapshots.Builder builder = new RegisteredPolicySnapshots.Builder(RegisteredPolicySnapshots.EMPTY);
            var snap = new SnapshotId(randomAlphaOfLength(10), randomUUID());
            builder.addIfSnapshotIsSLMInitiated(Map.of(SnapshotsService.POLICY_ID_METADATA_FIELD, "cheddar"), snap);
            RegisteredPolicySnapshots registered = builder.build();
            assertEquals(List.of(new RegisteredPolicySnapshots.PolicySnapshot("cheddar", snap)), registered.getSnapshots());
        }
    }

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
                policy = randomValueOtherThan(policy, this::randomPolicy);
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
