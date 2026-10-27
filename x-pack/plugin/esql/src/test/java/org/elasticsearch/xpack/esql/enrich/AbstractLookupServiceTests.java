/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;

public class AbstractLookupServiceTests extends ESTestCase {

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
