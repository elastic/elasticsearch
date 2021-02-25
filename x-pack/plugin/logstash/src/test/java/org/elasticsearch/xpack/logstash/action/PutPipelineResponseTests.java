/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logstash.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class PutPipelineResponseTests extends AbstractWireSerializingTestCase<PutPipelineResponse> {

    @Override
    protected Writeable.Reader<PutPipelineResponse> instanceReader() {
        return PutPipelineResponse::new;
    }

    @Override
    protected PutPipelineResponse createTestInstance() {
        return new PutPipelineResponse(randomFrom(RestStatus.OK, RestStatus.CREATED));
    }

    @Override
    protected PutPipelineResponse mutateInstance(PutPipelineResponse instance) {
        if (instance.status() == RestStatus.OK) {
            return new PutPipelineResponse(RestStatus.CREATED);
        }
        return new PutPipelineResponse(RestStatus.OK);
    }
}
