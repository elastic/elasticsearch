/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.hotthreads;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class NodesHotThreadsRequest extends BaseNodesRequest<NodesHotThreadsRequest> {

    int threads = 3;
    String type = "cpu";
    TimeValue interval = new TimeValue(500, TimeUnit.MILLISECONDS);
    int snapshots = 10;
    boolean ignoreIdleThreads = true;

    // for serialization
    public NodesHotThreadsRequest(StreamInput in) throws IOException {
        super(in);
        threads = in.readInt();
        ignoreIdleThreads = in.readBoolean();
        type = in.readString();
        interval = in.readTimeValue();
        snapshots = in.readInt();
    }

    /**
     * Get hot threads from nodes based on the nodes ids specified. If none are passed, hot
     * threads for all nodes is used.
     */
    public NodesHotThreadsRequest(String... nodesIds) {
        super(nodesIds);
    }

    public int threads() {
        return this.threads;
    }

    public NodesHotThreadsRequest threads(int threads) {
        this.threads = threads;
        return this;
    }

    public boolean ignoreIdleThreads() {
        return this.ignoreIdleThreads;
    }

    public NodesHotThreadsRequest ignoreIdleThreads(boolean ignoreIdleThreads) {
        this.ignoreIdleThreads = ignoreIdleThreads;
        return this;
    }

    public NodesHotThreadsRequest type(String type) {
        this.type = type;
        return this;
    }

    public String type() {
        return this.type;
    }

    public NodesHotThreadsRequest interval(TimeValue interval) {
        this.interval = interval;
        return this;
    }

    public TimeValue interval() {
        return this.interval;
    }

    public int snapshots() {
        return this.snapshots;
    }

    public NodesHotThreadsRequest snapshots(int snapshots) {
        this.snapshots = snapshots;
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(threads);
        out.writeBoolean(ignoreIdleThreads);
        out.writeString(type);
        out.writeTimeValue(interval);
        out.writeInt(snapshots);
    }
}
