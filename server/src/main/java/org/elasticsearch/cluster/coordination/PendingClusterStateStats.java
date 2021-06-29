/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
