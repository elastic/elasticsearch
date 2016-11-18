/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.action;

import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.prelert.action.RevertModelSnapshotAction.Request;
import org.elasticsearch.xpack.prelert.support.AbstractStreamableXContentTestCase;

public class RevertModelSnapshotActionRequestTests extends AbstractStreamableXContentTestCase<RevertModelSnapshotAction.Request> {

    @Override
    protected Request createTestInstance() {
        RevertModelSnapshotAction.Request request = new RevertModelSnapshotAction.Request(randomAsciiOfLengthBetween(1, 20));
        if (randomBoolean()) {
            request.setDescription(randomAsciiOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            request.setTime(randomAsciiOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            request.setSnapshotId(randomAsciiOfLengthBetween(1, 20));
        }
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
    protected Request parseInstance(XContentParser parser, ParseFieldMatcher matcher) {
        return RevertModelSnapshotAction.Request.parseRequest(null, parser, () -> matcher);
    }

}
