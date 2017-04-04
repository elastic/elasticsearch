/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.action.RevertModelSnapshotAction.Request;
import org.elasticsearch.xpack.ml.support.AbstractStreamableXContentTestCase;

public class RevertModelSnapshotActionRequestTests extends AbstractStreamableXContentTestCase<RevertModelSnapshotAction.Request> {

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
    protected Request createBlankInstance() {
        return new RevertModelSnapshotAction.Request();
    }

    @Override
    protected Request parseInstance(XContentParser parser) {
        return RevertModelSnapshotAction.Request.parseRequest(null, null, parser);
    }
}
