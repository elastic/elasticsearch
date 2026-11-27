/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.transport.BytesRefRecycler;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.greaterThan;

public class RestListTasksActionRetentionTests extends ESTestCase {

    public void testResponseKeepsTheNodeRepliesRetainedUntilTheRestResponseIsClosed() {
        final String groupBy = randomFrom("nodes", "parents", "none");
        final FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), randomBoolean());
        final AtomicBoolean released = new AtomicBoolean();
        final ListTasksResponse response = new ListTasksResponse(List.of(), List.of(), List.of());
        response.setNodeRepliesRetainer(() -> () -> released.set(true));

        RestListTasksAction.listTasksResponseListener(() -> DiscoveryNodes.EMPTY_NODES, groupBy, channel).onResponse(response);

        assertEquals(1, channel.responses().get());
        assertFalse(released.get());
        channel.capturedResponse().close();
        assertTrue(released.get());
    }

    public void testClosingTheRestResponseAfterPartOfTheBodyWasWrittenReleasesTheNodeReplies() throws IOException {
        final String groupBy = randomFrom("nodes", "parents", "none");
        final FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), randomBoolean());
        final AtomicBoolean released = new AtomicBoolean();
        final ListTasksResponse response = new ListTasksResponse(List.of(), List.of(), List.of());
        response.setNodeRepliesRetainer(() -> () -> released.set(true));

        RestListTasksAction.listTasksResponseListener(() -> DiscoveryNodes.EMPTY_NODES, groupBy, channel).onResponse(response);

        final ChunkedRestResponseBodyPart body = channel.capturedResponse().chunkedContent();
        assertFalse(body.isPartComplete());
        try (ReleasableBytesReference chunk = body.encodeChunk(1, BytesRefRecycler.NON_RECYCLING_INSTANCE)) {
            assertThat(chunk.length(), greaterThan(0));
        }
        assertFalse(released.get());
        channel.capturedResponse().close();
        assertTrue(released.get());
    }
}
