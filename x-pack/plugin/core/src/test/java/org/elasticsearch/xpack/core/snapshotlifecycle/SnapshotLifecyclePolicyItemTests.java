/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.snapshotlifecycle;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotLifecyclePolicyMetadataTests.createRandomPolicy;
import static org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotLifecyclePolicyMetadataTests.createRandomPolicyMetadata;

public class SnapshotLifecyclePolicyItemTests extends AbstractWireSerializingTestCase<SnapshotLifecyclePolicyItem> {

    @Override
    protected SnapshotLifecyclePolicyItem createTestInstance() {
        return new SnapshotLifecyclePolicyItem(createRandomPolicyMetadata(randomAlphaOfLengthBetween(5, 10)));
    }

    @Override
    protected SnapshotLifecyclePolicyItem mutateInstance(SnapshotLifecyclePolicyItem instance) {
        switch (between(0, 4)) {
            case 0:
                String newPolicyId = randomValueOtherThan(instance.getPolicy().getId(), () -> randomAlphaOfLengthBetween(5, 10));
                return new SnapshotLifecyclePolicyItem(createRandomPolicy(newPolicyId),
                    instance.getVersion(),
                    instance.getModifiedDate(),
                    instance.getLastSuccess(),
                    instance.getLastFailure());
            case 1:
                return new SnapshotLifecyclePolicyItem(instance.getPolicy(),
                    randomValueOtherThan(instance.getVersion(), ESTestCase::randomNonNegativeLong),
                    instance.getModifiedDate(),
                    instance.getLastSuccess(),
                    instance.getLastFailure());
            case 2:
                return new SnapshotLifecyclePolicyItem(instance.getPolicy(),
                    instance.getVersion(),
                    randomValueOtherThan(instance.getModifiedDate(), ESTestCase::randomNonNegativeLong),
                    instance.getLastSuccess(),
                    instance.getLastFailure());
            case 3:
                return new SnapshotLifecyclePolicyItem(instance.getPolicy(),
                    instance.getVersion(),
                    instance.getModifiedDate(),
                    randomValueOtherThan(instance.getLastSuccess(),
                        SnapshotInvocationRecordTests::randomSnapshotInvocationRecord),
                    instance.getLastFailure());
            case 4:
                return new SnapshotLifecyclePolicyItem(instance.getPolicy(),
                    instance.getVersion(),
                    instance.getModifiedDate(),
                    instance.getLastSuccess(),
                    randomValueOtherThan(instance.getLastFailure(),
                        SnapshotInvocationRecordTests::randomSnapshotInvocationRecord));
            default:
                throw new AssertionError("failure, got illegal switch case");
        }
    }

    @Override
    protected Writeable.Reader<SnapshotLifecyclePolicyItem> instanceReader() {
        return SnapshotLifecyclePolicyItem::new;
    }
}
