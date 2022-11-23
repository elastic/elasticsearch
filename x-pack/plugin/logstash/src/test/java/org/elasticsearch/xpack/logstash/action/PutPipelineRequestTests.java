/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logstash.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentType;

public class PutPipelineRequestTests extends AbstractWireSerializingTestCase<PutPipelineRequest> {

    @Override
    protected Writeable.Reader<PutPipelineRequest> instanceReader() {
        return PutPipelineRequest::new;
    }

    @Override
    protected PutPipelineRequest createTestInstance() {
        return new PutPipelineRequest(randomAlphaOfLength(2), randomAlphaOfLengthBetween(10, 100), randomFrom(XContentType.values()));
    }

    @Override
    protected PutPipelineRequest mutateInstance(PutPipelineRequest instance) {
        return new PutPipelineRequest(
            instance.id() + randomAlphaOfLength(1),
            instance.source() + randomAlphaOfLength(1),
            instance.xContentType()
        );
    }
}
