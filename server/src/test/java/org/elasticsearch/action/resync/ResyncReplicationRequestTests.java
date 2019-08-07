/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
        final Translog.Index index = new Translog.Index("type", "id", 0, randomNonNegativeLong(),
            randomNonNegativeLong(), bytes, null, -1);
        final ShardId shardId = new ShardId(new Index("index", "uuid"), 0);
        final ResyncReplicationRequest before = new ResyncReplicationRequest(shardId, 42L, 100, new Translog.Operation[]{index});

        final BytesStreamOutput out = new BytesStreamOutput();
        before.writeTo(out);

        final StreamInput in = out.bytes().streamInput();
        final ResyncReplicationRequest after = new ResyncReplicationRequest(in);

        assertThat(after, equalTo(before));
    }

}
