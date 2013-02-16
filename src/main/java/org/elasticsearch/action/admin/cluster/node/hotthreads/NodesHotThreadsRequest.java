/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.admin.cluster.node.hotthreads;

import org.elasticsearch.action.support.nodes.NodesOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 */
public class NodesHotThreadsRequest extends NodesOperationRequest<NodesHotThreadsRequest> {

    int threads = 3;
    String type = "cpu";
    TimeValue interval = new TimeValue(500, TimeUnit.MILLISECONDS);
    int snapshots = 10;

    /**
     * Get hot threads from nodes based on the nodes ids specified. If none are passed, hot
     * threads for all nodes is used.
     */
    public NodesHotThreadsRequest(String... nodesIds) {
        super(nodesIds);
    }

    public int getThreads() {
        return this.threads;
    }

    public NodesHotThreadsRequest setThreads(int threads) {
        this.threads = threads;
        return this;
    }

    public NodesHotThreadsRequest setType(String type) {
        this.type = type;
        return this;
    }

    public String getType() {
        return this.type;
    }

    public NodesHotThreadsRequest setInterval(TimeValue interval) {
        this.interval = interval;
        return this;
    }

    public TimeValue getInterval() {
        return this.interval;
    }

    public int getSnapshots() {
        return this.snapshots;
    }

    public NodesHotThreadsRequest setSnapshots(int snapshots) {
        this.snapshots = snapshots;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        threads = in.readInt();
        type = in.readString();
        interval = TimeValue.readTimeValue(in);
        snapshots = in.readInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(threads);
        out.writeString(type);
        interval.writeTo(out);
        out.writeInt(snapshots);
    }
}
