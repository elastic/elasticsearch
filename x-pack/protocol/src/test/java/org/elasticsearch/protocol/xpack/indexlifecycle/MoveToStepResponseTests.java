/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 *
 */
package org.elasticsearch.protocol.xpack.indexlifecycle;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

import java.io.IOException;

public class MoveToStepResponseTests extends AbstractStreamableXContentTestCase<MoveToStepResponse> {

    @Override
    protected MoveToStepResponse createTestInstance() {
        return new MoveToStepResponse(randomBoolean());
    }

    @Override
    protected MoveToStepResponse createBlankInstance() {
        return new MoveToStepResponse();
    }

    @Override
    protected MoveToStepResponse mutateInstance(MoveToStepResponse response) {
        return new MoveToStepResponse(response.isAcknowledged() == false);
    }

    @Override
    protected MoveToStepResponse doParseInstance(XContentParser parser) throws IOException {
        return MoveToStepResponse.fromXContent(parser);
    }

}
