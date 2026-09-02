/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class PublishIndexingOperationsHeapMemoryRequirementsRequestSerializationTests extends AbstractWireSerializingTestCase<
    TransportPublishIndexingOperationsHeapMemoryRequirements.Request> {
    @Override
    protected Writeable.Reader<TransportPublishIndexingOperationsHeapMemoryRequirements.Request> instanceReader() {
        return TransportPublishIndexingOperationsHeapMemoryRequirements.Request::new;
    }

    @Override
    protected TransportPublishIndexingOperationsHeapMemoryRequirements.Request createTestInstance() {
        return new TransportPublishIndexingOperationsHeapMemoryRequirements.Request(randomLongBetween(0, Long.MAX_VALUE));
    }

    @Override
    protected TransportPublishIndexingOperationsHeapMemoryRequirements.Request mutateInstance(
        TransportPublishIndexingOperationsHeapMemoryRequirements.Request instance
    ) throws IOException {
        return new TransportPublishIndexingOperationsHeapMemoryRequirements.Request(
            randomValueOtherThan(instance.getMinimumRequiredHeapInBytes(), () -> randomLongBetween(0, Long.MAX_VALUE))
        );
    }
}
