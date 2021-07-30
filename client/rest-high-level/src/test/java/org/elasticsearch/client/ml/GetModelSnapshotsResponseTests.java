/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.job.process.ModelSnapshot;
import org.elasticsearch.client.ml.job.process.ModelSnapshotTests;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetModelSnapshotsResponseTests extends AbstractXContentTestCase<GetModelSnapshotsResponse> {

    @Override
    protected GetModelSnapshotsResponse createTestInstance() {
        int listSize = randomInt(10);
        List<ModelSnapshot.Builder> modelSnapshots = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            modelSnapshots.add(ModelSnapshotTests.createRandomizedBuilder());
        }
        return new GetModelSnapshotsResponse(modelSnapshots, listSize);
    }

    @Override
    protected GetModelSnapshotsResponse doParseInstance(XContentParser parser) throws IOException {
        return GetModelSnapshotsResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
