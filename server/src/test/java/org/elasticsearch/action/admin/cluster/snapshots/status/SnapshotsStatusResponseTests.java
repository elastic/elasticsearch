/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class SnapshotsStatusResponseTests extends AbstractChunkedSerializingTestCase<SnapshotsStatusResponse> {

    private static final ConstructingObjectParser<SnapshotsStatusResponse, Void> PARSER = new ConstructingObjectParser<>(
        "snapshots_status_response",
        true,
        (Object[] parsedObjects) -> {
            @SuppressWarnings("unchecked")
            List<SnapshotStatus> snapshots = (List<SnapshotStatus>) parsedObjects[0];
            return new SnapshotsStatusResponse(snapshots);
        }
    );
    static {
        PARSER.declareObjectArray(constructorArg(), SnapshotStatusTests.PARSER, new ParseField("snapshots"));
    }

    @Override
    protected SnapshotsStatusResponse doParseInstance(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
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
    protected SnapshotsStatusResponse mutateInstance(SnapshotsStatusResponse instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<SnapshotsStatusResponse> instanceReader() {
        return SnapshotsStatusResponse::new;
    }

    public void testChunking() {
        AbstractChunkedSerializingTestCase.assertChunkCount(
            createTestInstance(),
            instance -> 2 + instance.getSnapshots().stream().mapToInt(i -> 2 + i.getIndices().size()).sum()
        );
    }
}
