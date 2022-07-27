/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

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
            sdip.toXContent(builder, ToXContent.EMPTY_PARAMS);
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
}
