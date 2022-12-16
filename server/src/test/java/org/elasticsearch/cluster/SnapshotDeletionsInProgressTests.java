/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class SnapshotDeletionsInProgressTests extends ESTestCase {
    public void testXContent() throws IOException {
        SnapshotDeletionsInProgress sdip = SnapshotDeletionsInProgress.of(
            List.of(
                new SnapshotDeletionsInProgress.Entry(
                    Collections.emptyList(),
                    "repo",
                    736694267638L,
                    0,
                    SnapshotDeletionsInProgress.State.STARTED
                )
            )
        );

        try (XContentBuilder builder = jsonBuilder()) {
            builder.humanReadable(true);
            builder.startObject();
            ChunkedToXContent.wrapAsXContentObject(sdip).toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            String json = Strings.toString(builder);
            assertThat(json, equalTo(XContentHelper.stripWhitespace("""
                {
                  "snapshot_deletions": [
                    {
                      "repository": "repo",
                      "snapshots": [],
                      "start_time": "1993-05-06T13:17:47.638Z",
                      "start_time_millis": 736694267638,
                      "repository_state_id": 0,
                      "state": "STARTED"
                    }
                  ]
                }""")));
        }
    }

    public void testChunking() throws IOException {
        final var instance = SnapshotDeletionsInProgress.of(
            randomList(
                10,
                () -> new SnapshotDeletionsInProgress.Entry(
                    Collections.emptyList(),
                    randomAlphaOfLength(10),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomFrom(SnapshotDeletionsInProgress.State.values())
                )
            )
        );

        int chunkCount = 0;
        try (var builder = jsonBuilder()) {
            builder.startObject();
            final var iterator = instance.toXContentChunked(EMPTY_PARAMS);
            while (iterator.hasNext()) {
                iterator.next().toXContent(builder, ToXContent.EMPTY_PARAMS);
                chunkCount += 1;
            }
            builder.endObject();
        } // closing the builder verifies that the XContent is well-formed

        assertEquals(instance.getEntries().size() + 2, chunkCount);
    }
}
