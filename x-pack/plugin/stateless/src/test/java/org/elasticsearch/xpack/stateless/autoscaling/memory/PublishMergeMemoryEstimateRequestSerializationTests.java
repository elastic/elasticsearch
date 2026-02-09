/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.autoscaling.memory;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.autoscaling.memory.MergeMemoryEstimateCollector.ShardMergeMemoryEstimate;
import org.elasticsearch.xpack.stateless.autoscaling.memory.TransportPublishMergeMemoryEstimate.Request;

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
