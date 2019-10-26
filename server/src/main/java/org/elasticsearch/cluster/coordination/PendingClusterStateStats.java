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

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Class encapsulating stats about the PendingClusterStatsQueue
 */
public class PendingClusterStateStats implements Writeable, ToXContentFragment {

    private final int total;
    private final int pending;
    private final int committed;

    public PendingClusterStateStats(int total, int pending, int committed) {
        this.total = total;
        this.pending = pending;
        this.committed = committed;
    }

    public PendingClusterStateStats(StreamInput in) throws IOException {
        total = in.readVInt();
        pending = in.readVInt();
        committed = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(total);
        out.writeVInt(pending);
        out.writeVInt(committed);
    }

    public int getCommitted() {
        return committed;
    }

    public int getPending() {
        return pending;
    }

    public int getTotal() {
        return total;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.QUEUE);
        builder.field(Fields.TOTAL, total);
        builder.field(Fields.PENDING, pending);
        builder.field(Fields.COMMITTED, committed);
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String QUEUE = "cluster_state_queue";
        static final String TOTAL = "total";
        static final String PENDING = "pending";
        static final String COMMITTED = "committed";
    }

    @Override
    public String toString() {
        return "PendingClusterStateStats(total=" + total + ", pending=" + pending + ", committed=" + committed + ")";
    }
}
