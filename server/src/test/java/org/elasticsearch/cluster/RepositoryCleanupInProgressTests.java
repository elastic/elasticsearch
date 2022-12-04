/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class RepositoryCleanupInProgressTests extends ESTestCase {
    public void testChunking() throws IOException {
        final var instance = new RepositoryCleanupInProgress(
            randomList(10, () -> new RepositoryCleanupInProgress.Entry(randomAlphaOfLength(10), randomNonNegativeLong()))
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

        assertEquals(instance.entries().size() + 2, chunkCount);
    }
}
