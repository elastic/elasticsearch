/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.SendRequestTransportException;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;

public class AbstractLookupServiceTests extends ESTestCase {

    private static final DiscoveryNode NODE = DiscoveryNodeUtils.create("node");

    public void testShardCopyUnavailableFailuresAreRetried() {
        List<Exception> failures = List.of(
            new RemoteTransportException("test", new NodeClosedException(NODE)),
            new ConnectTransportException(NODE, "test"),
            new NodeDisconnectedException(NODE, "test"),
            new SendRequestTransportException(NODE, "test", new NodeNotConnectedException(NODE, "test"))
        );
        for (Exception e : failures) {
            assertTrue(e.toString(), AbstractLookupService.isShardCopyUnavailable(e));
        }
    }

    public void testOtherFailuresAreNotRetried() {
        List<Exception> failures = List.of(
            new RemoteTransportException("test", new EsRejectedExecutionException("test", false)),
            new RemoteTransportException("test", new TaskCancelledException("test")),
            new RemoteTransportException("test", new CircuitBreakingException("test", CircuitBreaker.Durability.TRANSIENT)),
            new IllegalArgumentException("test")
        );
        for (Exception e : failures) {
            assertFalse(e.toString(), AbstractLookupService.isShardCopyUnavailable(e));
        }
    }

    public void testLookupRequestReportsShardReadFromWire() throws IOException {
        ShardId shardId = new ShardId("other-index", randomUUID(), 0);
        LookupFromIndexService.LookupRequest sent = new LookupFromIndexService.LookupRequest(
            "session",
            shardId,
            "lookup-alias",
            new Page(0),
            null,
            List.of(),
            List.of(),
            Source.EMPTY,
            null,
            null,
            null,
            null,
            false,
            null
        );
        BytesStreamOutput out = new BytesStreamOutput();
        sent.writeTo(out);
        StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), new NamedWriteableRegistry(List.of()));
        LookupFromIndexService.LookupRequest received = LookupFromIndexService.LookupRequest.readFrom(
            in,
            TestBlockFactory.getNonBreakingInstance()
        );
        try {
            assertThat(received.indices(), arrayContaining("lookup-alias"));
            assertThat(received.shards(), contains(shardId));
        } finally {
            received.decRef();
        }
    }
}
