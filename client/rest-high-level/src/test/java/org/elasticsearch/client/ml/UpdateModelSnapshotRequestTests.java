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


public class UpdateModelSnapshotRequestTests extends AbstractXContentTestCase<UpdateModelSnapshotRequest> {

    @Override
    protected UpdateModelSnapshotRequest createTestInstance() {
        String jobId = randomAlphaOfLengthBetween(1, 20);
        String snapshotId = randomAlphaOfLengthBetween(1, 20);
        UpdateModelSnapshotRequest request = new UpdateModelSnapshotRequest(jobId, snapshotId);
        if (randomBoolean()) {
            request.setDescription(String.valueOf(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            request.setRetain(randomBoolean());
        }

        return request;
    }

    @Override
    protected UpdateModelSnapshotRequest doParseInstance(XContentParser parser) throws IOException {
        return UpdateModelSnapshotRequest.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
