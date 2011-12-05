/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.action.admin.cluster.ping.broadcast;

import org.elasticsearch.action.support.broadcast.BroadcastOperationRequest;
import org.elasticsearch.action.support.broadcast.BroadcastOperationThreading;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class BroadcastPingRequest extends BroadcastOperationRequest {

    @Nullable protected String queryHint;

    BroadcastPingRequest() {
    }

    public BroadcastPingRequest(String... indices) {
        super(indices);
        this.queryHint = null;
    }

    @Override public BroadcastPingRequest operationThreading(BroadcastOperationThreading operationThreading) {
        super.operationThreading(operationThreading);
        return this;
    }

    @Override public BroadcastPingRequest listenerThreaded(boolean threadedListener) {
        super.listenerThreaded(threadedListener);
        return this;
    }

    public String queryHint() {
        return this.queryHint;
    }

    public BroadcastPingRequest queryHint(String queryHint) {
        this.queryHint = queryHint;
        return this;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.readBoolean()) {
            queryHint = in.readUTF();
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (queryHint == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(queryHint);
        }
    }
}