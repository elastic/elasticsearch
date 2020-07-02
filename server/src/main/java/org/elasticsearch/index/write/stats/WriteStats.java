/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.write.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class WriteStats implements Writeable, ToXContentFragment {

    private long pendingWriteBytes = 0;
    private long pendingReplicaWriteBytes = 0;

    public WriteStats(StreamInput in) throws IOException {
        pendingWriteBytes = in.readVLong();
        pendingReplicaWriteBytes = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(pendingWriteBytes);
        out.writeVLong(pendingReplicaWriteBytes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("write");
        builder.field("pending_write_bytes", pendingWriteBytes);
        builder.field("pending_replica_write_bytes", pendingReplicaWriteBytes);
        return builder.endObject();
    }

    public long getPendingWriteBytes() {
        return pendingWriteBytes;
    }

    public long getPendingReplicaWriteBytes() {
        return pendingReplicaWriteBytes;
    }
}
