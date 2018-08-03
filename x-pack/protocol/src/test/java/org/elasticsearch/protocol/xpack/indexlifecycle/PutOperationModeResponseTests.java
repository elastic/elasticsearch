/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.protocol.xpack.indexlifecycle;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

import java.io.IOException;

public class PutOperationModeResponseTests extends AbstractStreamableXContentTestCase<PutOperationModeResponse> {

    @Override
    protected PutOperationModeResponse createBlankInstance() {
        return new PutOperationModeResponse();
    }

    @Override
    protected PutOperationModeResponse createTestInstance() {
        return new PutOperationModeResponse(randomBoolean());
    }

    @Override
    protected PutOperationModeResponse mutateInstance(PutOperationModeResponse instance) throws IOException {
        return new PutOperationModeResponse(instance.isAcknowledged() == false);
    }

    @Override
    protected PutOperationModeResponse doParseInstance(XContentParser parser) throws IOException {
        return PutOperationModeResponse.fromXContent(parser);
    }

}
