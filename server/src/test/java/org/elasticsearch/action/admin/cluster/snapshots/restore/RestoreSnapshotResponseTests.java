/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.restore;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RestoreSnapshotResponseTests extends AbstractXContentTestCase<RestoreSnapshotResponse> {

    @Override
    protected RestoreSnapshotResponse createTestInstance() {
        if (randomBoolean()) {
            String name = randomRealisticUnicodeOfCodepointLengthBetween(1, 30);
            List<String> indices = new ArrayList<>();
            indices.add("test0");
            indices.add("test1");
            int totalShards = randomIntBetween(1, 1000);
            int successfulShards = randomIntBetween(0, totalShards);
            return new RestoreSnapshotResponse(new RestoreInfo(name, indices, totalShards, successfulShards));
        } else {
            return new RestoreSnapshotResponse((RestoreInfo) null);
        }
    }

    @Override
    protected RestoreSnapshotResponse doParseInstance(XContentParser parser) throws IOException {
        return RestoreSnapshotResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
