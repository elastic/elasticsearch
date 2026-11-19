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

import java.io.EOFException;
import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class PublishIndexingOperationsHeapMemoryRequirementsRequestSerializationTests extends AbstractWireSerializingTestCase<
    TransportPublishIndexingOperationsHeapMemoryRequirements.Request> {
    @Override
    protected Writeable.Reader<TransportPublishIndexingOperationsHeapMemoryRequirements.Request> instanceReader() {
        return TransportPublishIndexingOperationsHeapMemoryRequirements.Request::new;
    }

    @Override
    protected TransportPublishIndexingOperationsHeapMemoryRequirements.Request createTestInstance() {
        if (randomBoolean()) {
            return new TransportPublishIndexingOperationsHeapMemoryRequirements.Request(randomLongBetween(0, Long.MAX_VALUE));
        }
        return new TransportPublishIndexingOperationsHeapMemoryRequirements.Request(
            randomLongBetween(0, Long.MAX_VALUE),
            randomLongBetween(0, Long.MAX_VALUE)
        );
    }

    @Override
    protected TransportPublishIndexingOperationsHeapMemoryRequirements.Request mutateInstance(
        TransportPublishIndexingOperationsHeapMemoryRequirements.Request instance
    ) throws IOException {
        if (instance.hasLargestOperationSize() && randomBoolean()) {
            return new TransportPublishIndexingOperationsHeapMemoryRequirements.Request(
                instance.getMinimumRequiredHeapInBytes(),
                randomValueOtherThan(instance.getLargestOperationSizeInBytes(), () -> randomLongBetween(0, Long.MAX_VALUE))
            );
        }
        long heap = randomValueOtherThan(instance.getMinimumRequiredHeapInBytes(), () -> randomLongBetween(0, Long.MAX_VALUE));
        if (instance.hasLargestOperationSize()) {
            return new TransportPublishIndexingOperationsHeapMemoryRequirements.Request(heap, instance.getLargestOperationSizeInBytes());
        }
        return new TransportPublishIndexingOperationsHeapMemoryRequirements.Request(heap);
    }

    public void testOlderTransportVersionOmitsRawOperationSize() throws IOException {
        long heap = randomLongBetween(0, Long.MAX_VALUE);
        long operationSize = randomLongBetween(0, Long.MAX_VALUE);
        var request = new TransportPublishIndexingOperationsHeapMemoryRequirements.Request(heap, operationSize);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersion.minimumCompatible());
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(TransportVersion.minimumCompatible());
                var read = new TransportPublishIndexingOperationsHeapMemoryRequirements.Request(in);
                assertThat(read.getMinimumRequiredHeapInBytes(), equalTo(heap));
                assertThat(read.hasLargestOperationSize(), is(false));
                assertThat(read.getLargestOperationSizeInBytes(), equalTo(0L));
                expectThrows(EOFException.class, in::readByte);
            }
        }
    }
}
