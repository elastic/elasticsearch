/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.tasks;

import org.elasticsearch.cluster.service.PendingClusterTask;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class PendingClusterTasksResponseTests extends ESTestCase {
    public void testPendingClusterTasksResponseChunking() throws IOException {
        final var tasks = new ArrayList<PendingClusterTask>();
        for (int i = between(0, 10); i > 0; i--) {
            tasks.add(
                new PendingClusterTask(
                    randomNonNegativeLong(),
                    randomFrom(Priority.values()),
                    new Text(randomAlphaOfLengthBetween(1, 10)),
                    randomNonNegativeLong(),
                    randomBoolean()
                )
            );
        }

        int chunkCount = 0;
        try (XContentBuilder builder = jsonBuilder()) {
            final var iterator = new PendingClusterTasksResponse(tasks).toXContentChunked(ToXContent.EMPTY_PARAMS);
            while (iterator.hasNext()) {
                iterator.next().toXContent(builder, ToXContent.EMPTY_PARAMS);
                chunkCount += 1;
            }
        } // closing the builder verifies that the XContent is well-formed

        assertEquals(tasks.size() + 2, chunkCount);
    }
}
