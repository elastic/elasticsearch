/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;

public class SnapshotsStatusResponseTests extends AbstractChunkedSerializingTestCase<SnapshotsStatusResponse> {

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

    @Override
    protected Writeable.Reader<SnapshotsStatusResponse> instanceReader() {
        return SnapshotsStatusResponse::new;
    }

    public void testChunkCount() {
        final var instance = createTestInstance();
        // open and close chunk
        int chunksExpected = 2;
        for (SnapshotStatus snapshot : instance.getSnapshots()) {
            // open and close chunk + one chunk per index
            chunksExpected += 2 + snapshot.getIndices().size();
        }
        final var iterator = instance.toXContentChunked(EMPTY_PARAMS);
        int chunksSeen = 0;
        while (iterator.hasNext()) {
            iterator.next();
            chunksSeen++;
        }
        assertEquals(chunksExpected, chunksSeen);
    }
}
