/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

public class RefreshJobMemoryRequirementActionRequestTests
    extends AbstractStreamableXContentTestCase<RefreshJobMemoryRequirementAction.Request> {

    @Override
    protected RefreshJobMemoryRequirementAction.Request createTestInstance() {
        return new RefreshJobMemoryRequirementAction.Request(randomAlphaOfLengthBetween(1, 20));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected RefreshJobMemoryRequirementAction.Request createBlankInstance() {
        return new RefreshJobMemoryRequirementAction.Request();
    }

    @Override
    protected RefreshJobMemoryRequirementAction.Request doParseInstance(XContentParser parser) {
        return RefreshJobMemoryRequirementAction.Request.parseRequest(null, parser);
    }
}
