/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logstash.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class DeletePipelineRequestTests extends AbstractWireSerializingTestCase<DeletePipelineRequest> {

    @Override
    protected Writeable.Reader<DeletePipelineRequest> instanceReader() {
        return DeletePipelineRequest::new;
    }

    @Override
    protected DeletePipelineRequest createTestInstance() {
        return new DeletePipelineRequest(randomAlphaOfLengthBetween(2, 10));
    }

    @Override
    protected DeletePipelineRequest mutateInstance(DeletePipelineRequest instance) {
        return new DeletePipelineRequest(instance.id() + randomAlphaOfLength(1));
    }
}
