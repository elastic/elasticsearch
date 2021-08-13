/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;


public class GetModelSnapshotsRequestTests extends AbstractXContentTestCase<GetModelSnapshotsRequest> {

    @Override
    protected GetModelSnapshotsRequest createTestInstance() {
        GetModelSnapshotsRequest request = new GetModelSnapshotsRequest(randomAlphaOfLengthBetween(1, 20));
        if (randomBoolean()) {
            request.setSnapshotId(String.valueOf(randomNonNegativeLong()));
        }
        else {
            if (randomBoolean()) {
                request.setStart(String.valueOf(randomLong()));
            }
            if (randomBoolean()) {
                request.setEnd(String.valueOf(randomLong()));
            }
            if (randomBoolean()) {
                int from = randomInt(10000);
                int size = randomInt(10000);
                request.setPageParams(new PageParams(from, size));
            }
            if (randomBoolean()) {
                request.setSort("description");
            }
            if (randomBoolean()) {
                request.setDesc(randomBoolean());
            }
        }
        return request;
    }

    @Override
    protected GetModelSnapshotsRequest doParseInstance(XContentParser parser) throws IOException {
        return GetModelSnapshotsRequest.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
