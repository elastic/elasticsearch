/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.EOFException;
import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class PublishIndexingOperationsHeapMemoryRequirementsRequestSerializationTests extends AbstractWireSerializingTestCase<
    TransportPublishIndexingOperationsHeapMemoryRequirements.Request> {
    @Override
    protected Writeable.Reader<TransportPublishIndexingOperationsHeapMemoryRequirements.Request> instanceReader() {
        return TransportPublishIndexingOperationsHeapMemoryRequirements.Request::new;
    }

    @Override
    protected TransportPublishIndexingOperationsHeapMemoryRequirements.Request createTestInstance() {
        return new TransportPublishIndexingOperationsHeapMemoryRequirements.Request(
            randomLongBetween(0, Long.MAX_VALUE),
            randomLongBetween(0, Long.MAX_VALUE)
        );
    }

    @Override
    protected TransportPublishIndexingOperationsHeapMemoryRequirements.Request mutateInstance(
        TransportPublishIndexingOperationsHeapMemoryRequirements.Request instance
    ) {
        if (randomBoolean()) {
            return new TransportPublishIndexingOperationsHeapMemoryRequirements.Request(
                randomValueOtherThan(instance.getMinimumRequiredHeapInBytes(), () -> randomLongBetween(0, Long.MAX_VALUE)),
                instance.getLargestOperationSizeInBytes()
            );
        }
        return new TransportPublishIndexingOperationsHeapMemoryRequirements.Request(
            instance.getMinimumRequiredHeapInBytes(),
            randomValueOtherThan(instance.getLargestOperationSizeInBytes(), () -> randomLongBetween(0, Long.MAX_VALUE))
        );
    }

    public void testTransportVersionBeforePublishLargestIndexingOperationSize() throws IOException {
        long heap = randomLongBetween(0, Long.MAX_VALUE);
        long operationSize = randomLongBetween(0, Long.MAX_VALUE);
        var request = new TransportPublishIndexingOperationsHeapMemoryRequirements.Request(heap, operationSize);

        final TransportVersion before = TransportVersionUtils.randomVersionNotSupporting(
            TransportPublishIndexingOperationsHeapMemoryRequirements.PUBLISH_LARGEST_INDEXING_OPERATION_SIZE
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(before);
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(before);
                var read = new TransportPublishIndexingOperationsHeapMemoryRequirements.Request(in);
                assertThat(read.getMinimumRequiredHeapInBytes(), equalTo(heap));
                assertThat(read.getLargestOperationSizeInBytes(), equalTo(0L));
                expectThrows(EOFException.class, in::readByte);
            }
        }
    }
}
