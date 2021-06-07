/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logstash.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class DeletePipelineResponseTests extends AbstractWireSerializingTestCase<DeletePipelineResponse> {

    @Override
    protected Writeable.Reader<DeletePipelineResponse> instanceReader() {
        return DeletePipelineResponse::new;
    }

    @Override
    protected DeletePipelineResponse createTestInstance() {
        return new DeletePipelineResponse(randomBoolean());
    }

    @Override
    protected DeletePipelineResponse mutateInstance(DeletePipelineResponse instance) {
        // return a response with the opposite boolean value
        return new DeletePipelineResponse(instance.isDeleted() == false);
    }
}
