/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RestoreInProgressTests extends ESTestCase {
    public void testChunking() throws IOException {
        final var ripBuilder = new RestoreInProgress.Builder();
        final var entryCount = between(0, 5);
        for (int i = 0; i < entryCount; i++) {
            ripBuilder.add(
                new RestoreInProgress.Entry(
                    "uuid-" + i,
                    new Snapshot(randomAlphaOfLength(10), new SnapshotId(randomAlphaOfLength(10), randomAlphaOfLength(10))),
                    randomFrom(RestoreInProgress.State.values()),
                    randomBoolean(),
                    List.of(),
                    Map.of()
                )
            );
        }

        AbstractChunkedSerializingTestCase.assertChunkCount(ripBuilder.build(), ignored -> entryCount + 2);
    }
}
