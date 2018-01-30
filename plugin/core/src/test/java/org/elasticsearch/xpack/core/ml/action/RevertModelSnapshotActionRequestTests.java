/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction.Request;

public class RevertModelSnapshotActionRequestTests extends AbstractStreamableXContentTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        RevertModelSnapshotAction.Request request =
                new RevertModelSnapshotAction.Request(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        if (randomBoolean()) {
            request.setDeleteInterveningResults(randomBoolean());
        }
        return request;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected Request createBlankInstance() {
        return new RevertModelSnapshotAction.Request();
    }

    @Override
    protected Request doParseInstance(XContentParser parser) {
        return RevertModelSnapshotAction.Request.parseRequest(null, null, parser);
    }
}
