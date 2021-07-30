/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;


public class RevertModelSnapshotRequestTests extends AbstractXContentTestCase<RevertModelSnapshotRequest> {

    @Override
    protected RevertModelSnapshotRequest createTestInstance() {
        String jobId = randomAlphaOfLengthBetween(1, 20);
        String snapshotId = randomAlphaOfLengthBetween(1, 20);
        RevertModelSnapshotRequest request = new RevertModelSnapshotRequest(jobId, snapshotId);
        if (randomBoolean()) {
            request.setDeleteInterveningResults(randomBoolean());
        }

        return request;
    }

    @Override
    protected RevertModelSnapshotRequest doParseInstance(XContentParser parser) throws IOException {
        return RevertModelSnapshotRequest.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
