/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.memory.StatelessMemoryMetricsService.ShardMergeMemoryEstimate;
import org.elasticsearch.xpack.stateless.memory.TransportPublishMergeMemoryEstimate.Request;

import java.io.IOException;

public class PublishMergeMemoryEstimateRequestSerializationTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        return new Request(
            randomNonNegativeLong(),
            randomIdentifier(),
            new ShardMergeMemoryEstimate(randomIdentifier(), randomNonNegativeLong())
        );
    }

    @Override
    protected Request mutateInstance(Request instance) throws IOException {
        int i = randomIntBetween(0, 3);
        return switch (i) {
            case 0 -> new Request(
                randomValueOtherThan(instance.getSeqNo(), ESTestCase::randomNonNegativeLong),
                instance.getNodeEphemeralId(),
                instance.getEstimate()
            );
            case 1 -> new Request(
                instance.getSeqNo(),
                randomValueOtherThan(instance.getNodeEphemeralId(), ESTestCase::randomIdentifier),
                instance.getEstimate()
            );
            case 2 -> new Request(
                instance.getSeqNo(),
                instance.getNodeEphemeralId(),
                new ShardMergeMemoryEstimate(
                    instance.getEstimate().mergeId(),
                    randomValueOtherThan(instance.getEstimate().estimateInBytes(), ESTestCase::randomNonNegativeLong)
                )
            );
            case 3 -> new Request(
                instance.getSeqNo(),
                instance.getNodeEphemeralId(),
                new ShardMergeMemoryEstimate(
                    randomValueOtherThan(instance.getEstimate().mergeId(), ESTestCase::randomIdentifier),
                    instance.getEstimate().estimateInBytes()
                )
            );
            default -> throw new IllegalStateException("invalid mutate branch");
        };
    }
}
