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

public class PublishHeapMemoryUsageRequestSerializationTests extends AbstractWireSerializingTestCase<PublishHeapMemoryMetricsRequest> {
    @Override
    protected Writeable.Reader<PublishHeapMemoryMetricsRequest> instanceReader() {
        return PublishHeapMemoryMetricsRequest::new;
    }

    @Override
    protected PublishHeapMemoryMetricsRequest createTestInstance() {
        return new PublishHeapMemoryMetricsRequest(HeapMemoryUsageTests.randomHeapMemoryUsage());
    }

    @Override
    protected PublishHeapMemoryMetricsRequest mutateInstance(PublishHeapMemoryMetricsRequest instance) throws IOException {
        return new PublishHeapMemoryMetricsRequest(HeapMemoryUsageTests.mutate(instance.getHeapMemoryUsage()));
    }
}
