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

package org.elasticsearch.action.admin.cluster.health;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.unit.TimeValue.readTimeValue;

/**
 *
 */
public class ClusterHealthRequest extends MasterNodeOperationRequest<ClusterHealthRequest> {

    private String[] indices;

    private TimeValue timeout = new TimeValue(30, TimeUnit.SECONDS);

    private ClusterHealthStatus waitForStatus;

    private int waitForRelocatingShards = -1;

    private int waitForActiveShards = -1;

    private String waitForNodes = "";

    private boolean local = false;

    ClusterHealthRequest() {
    }

    public ClusterHealthRequest(String... indices) {
        this.indices = indices;
    }

    public String[] getIndices() {
        return indices;
    }

    public ClusterHealthRequest setIndices(String[] indices) {
        this.indices = indices;
        return this;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    public ClusterHealthRequest setTimeout(TimeValue timeout) {
        this.timeout = timeout;
        if (masterNodeTimeout == DEFAULT_MASTER_NODE_TIMEOUT) {
            masterNodeTimeout = timeout;
        }
        return this;
    }

    public ClusterHealthRequest setTimeout(String timeout) {
        return setTimeout(TimeValue.parseTimeValue(timeout, null));
    }

    public ClusterHealthStatus getWaitForStatus() {
        return waitForStatus;
    }

    public ClusterHealthRequest setWaitForStatus(ClusterHealthStatus waitForStatus) {
        this.waitForStatus = waitForStatus;
        return this;
    }

    public ClusterHealthRequest setWaitForGreenStatus() {
        return setWaitForStatus(ClusterHealthStatus.GREEN);
    }

    public ClusterHealthRequest setWaitForYellowStatus() {
        return setWaitForStatus(ClusterHealthStatus.YELLOW);
    }

    public int getWaitForRelocatingShards() {
        return waitForRelocatingShards;
    }

    public ClusterHealthRequest setWaitForRelocatingShards(int waitForRelocatingShards) {
        this.waitForRelocatingShards = waitForRelocatingShards;
        return this;
    }

    public int getWaitForActiveShards() {
        return waitForActiveShards;
    }

    public ClusterHealthRequest setWaitForActiveShards(int waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
        return this;
    }

    public String getWaitForNodes() {
        return waitForNodes;
    }

    /**
     * Waits for N number of nodes. Use "12" for exact mapping, ">12" and "<12" for range.
     */
    public ClusterHealthRequest setWaitForNodes(String waitForNodes) {
        this.waitForNodes = waitForNodes;
        return this;
    }

    public ClusterHealthRequest setLocal(boolean local) {
        this.local = local;
        return this;
    }

    public boolean isLocal() {
        return this.local;
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
        timeout = readTimeValue(in);
        if (in.readBoolean()) {
            waitForStatus = ClusterHealthStatus.fromValue(in.readByte());
        }
        waitForRelocatingShards = in.readInt();
        waitForActiveShards = in.readInt();
        waitForNodes = in.readString();
        local = in.readBoolean();
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
        out.writeBoolean(local);
    }
}
