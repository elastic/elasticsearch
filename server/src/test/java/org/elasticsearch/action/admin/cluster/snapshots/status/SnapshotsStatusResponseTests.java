/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class SnapshotsStatusResponseTests extends AbstractXContentTestCase<SnapshotsStatusResponse> {

    @Override
    protected SnapshotsStatusResponse doParseInstance(XContentParser parser) throws IOException {
        return SnapshotsStatusResponse.fromXContent(parser);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // Do not place random fields in the indices field or shards field since their fields correspond to names.
        return (s) -> s.endsWith("shards") || s.endsWith("indices");
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected SnapshotsStatusResponse createTestInstance() {
        SnapshotStatusTests statusBuilder = new SnapshotStatusTests();
        List<SnapshotStatus> snapshotStatuses = new ArrayList<>();
        for (int idx = 0; idx < randomIntBetween(0, 5); idx++) {
            snapshotStatuses.add(statusBuilder.createTestInstance());
        }
        return new SnapshotsStatusResponse(snapshotStatuses);
    }
}
