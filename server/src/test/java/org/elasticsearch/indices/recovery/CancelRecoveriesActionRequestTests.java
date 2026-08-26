/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CancelRecoveriesActionRequestTests extends AbstractWireSerializingTestCase<CancelRecoveriesAction.Request> {

    @Override
    protected Writeable.Reader<CancelRecoveriesAction.Request> instanceReader() {
        return CancelRecoveriesAction.Request::new;
    }

    @Override
    protected CancelRecoveriesAction.Request createTestInstance() {
        return new CancelRecoveriesAction.Request(randomNonNegativeLong(), randomNonNegativeLong(), randomCancellations());
    }

    @Override
    protected CancelRecoveriesAction.Request mutateInstance(CancelRecoveriesAction.Request instance) throws IOException {
        return switch (between(0, 2)) {
            case 0 -> new CancelRecoveriesAction.Request(
                randomValueOtherThan(instance.term(), ESTestCase::randomNonNegativeLong),
                instance.clusterStateVersion(),
                instance.cancellations()
            );
            case 1 -> new CancelRecoveriesAction.Request(
                instance.term(),
                randomValueOtherThan(instance.clusterStateVersion(), ESTestCase::randomNonNegativeLong),
                instance.cancellations()
            );
            default -> new CancelRecoveriesAction.Request(
                instance.term(),
                instance.clusterStateVersion(),
                randomValueOtherThan(instance.cancellations(), this::randomCancellations)
            );
        };
    }

    private List<ShardRecoveryCancellation> randomCancellations() {
        final int size = randomIntBetween(0, 5);
        final var cancellations = new ArrayList<ShardRecoveryCancellation>(size);
        for (int i = 0; i < size; i++) {
            cancellations.add(
                new ShardRecoveryCancellation(
                    new ShardId(randomIdentifier(), UUIDs.randomBase64UUID(), i),
                    UUIDs.randomBase64UUID(),
                    randomBoolean()
                )
            );
        }
        return cancellations;
    }
}
