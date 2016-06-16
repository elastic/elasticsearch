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

package org.elasticsearch.action.admin.cluster.health;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ClusterHealthRequest extends MasterNodeReadRequest<ClusterHealthRequest> implements IndicesRequest.Replaceable {

    private String[] indices;
    private TimeValue timeout = new TimeValue(30, TimeUnit.SECONDS);
    private ClusterHealthStatus waitForStatus;
    private int waitForRelocatingShards = -1;
    private int waitForActiveShards = -1;
    private String waitForNodes = "";
    private Priority waitForEvents = null;

    public ClusterHealthRequest() {
    }

    public ClusterHealthRequest(String... indices) {
        this.indices = indices;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public ClusterHealthRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.lenientExpandOpen();
    }

    public TimeValue timeout() {
        return timeout;
    }

    public ClusterHealthRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        if (masterNodeTimeout == DEFAULT_MASTER_NODE_TIMEOUT) {
            masterNodeTimeout = timeout;
        }
        return this;
    }

    public ClusterHealthRequest timeout(String timeout) {
        return this.timeout(TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".timeout"));
    }

    public ClusterHealthStatus waitForStatus() {
        return waitForStatus;
    }

    public ClusterHealthRequest waitForStatus(ClusterHealthStatus waitForStatus) {
        this.waitForStatus = waitForStatus;
        return this;
    }

    public ClusterHealthRequest waitForGreenStatus() {
        return waitForStatus(ClusterHealthStatus.GREEN);
    }

    public ClusterHealthRequest waitForYellowStatus() {
        return waitForStatus(ClusterHealthStatus.YELLOW);
    }

    public int waitForRelocatingShards() {
        return waitForRelocatingShards;
    }

    public ClusterHealthRequest waitForRelocatingShards(int waitForRelocatingShards) {
        this.waitForRelocatingShards = waitForRelocatingShards;
        return this;
    }

    public int waitForActiveShards() {
        return waitForActiveShards;
    }

    public ClusterHealthRequest waitForActiveShards(int waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
        return this;
    }

    public String waitForNodes() {
        return waitForNodes;
    }

    /**
     * Waits for N number of nodes. Use "12" for exact mapping, "&gt;12" and "&lt;12" for range.
     */
    public ClusterHealthRequest waitForNodes(String waitForNodes) {
        this.waitForNodes = waitForNodes;
        return this;
    }

    public ClusterHealthRequest waitForEvents(Priority waitForEvents) {
        this.waitForEvents = waitForEvents;
        return this;
    }

    public Priority waitForEvents() {
        return this.waitForEvents;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        if (size == 0) {
            indices = Strings.EMPTY_ARRAY;
        } else {
            indices = new String[size];
            for (int i = 0; i < indices.length; i++) {
                indices[i] = in.readString();
            }
        }
        timeout = new TimeValue(in);
        if (in.readBoolean()) {
            waitForStatus = ClusterHealthStatus.fromValue(in.readByte());
        }
        waitForRelocatingShards = in.readInt();
        waitForActiveShards = in.readInt();
        waitForNodes = in.readString();
        if (in.readBoolean()) {
            waitForEvents = Priority.readFrom(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (indices == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(indices.length);
            for (String index : indices) {
                out.writeString(index);
            }
        }
        timeout.writeTo(out);
        if (waitForStatus == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeByte(waitForStatus.value());
        }
        out.writeInt(waitForRelocatingShards);
        out.writeInt(waitForActiveShards);
        out.writeString(waitForNodes);
        if (waitForEvents == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Priority.writeTo(waitForEvents, out);
        }
    }
}
