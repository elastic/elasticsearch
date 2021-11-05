/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.resync;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.Charset;

import static org.hamcrest.Matchers.equalTo;

public class ResyncReplicationRequestTests extends ESTestCase {

    public void testSerialization() throws IOException {
        final byte[] bytes = "{}".getBytes(Charset.forName("UTF-8"));
        final Translog.Index index = new Translog.Index("id", 0, randomNonNegativeLong(), randomNonNegativeLong(), bytes, null, -1);
        final ShardId shardId = new ShardId(new Index("index", "uuid"), 0);
        final ResyncReplicationRequest before = new ResyncReplicationRequest(shardId, 42L, 100, new Translog.Operation[] { index });

        final BytesStreamOutput out = new BytesStreamOutput();
        before.writeTo(out);

        final StreamInput in = out.bytes().streamInput();
        final ResyncReplicationRequest after = new ResyncReplicationRequest(in);

        assertThat(after, equalTo(before));
    }

}
