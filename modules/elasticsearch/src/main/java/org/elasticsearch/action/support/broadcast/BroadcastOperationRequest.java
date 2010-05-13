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

package org.elasticsearch.action.support.broadcast;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.util.Strings;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public abstract class BroadcastOperationRequest implements ActionRequest {

    protected String[] indices;

    @Nullable protected String queryHint;

    private boolean listenerThreaded = false;
    private BroadcastOperationThreading operationThreading = BroadcastOperationThreading.SINGLE_THREAD;

    protected BroadcastOperationRequest() {

    }

    protected BroadcastOperationRequest(String[] indices, @Nullable String queryHint) {
        this.indices = indices;
        this.queryHint = queryHint;
    }

    public String[] indices() {
        return indices;
    }

    public BroadcastOperationRequest indices(String[] indices) {
        this.indices = indices;
        return this;
    }

    public String queryHint() {
        return queryHint;
    }

    @Override public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * Should the listener be called on a separate thread if needed.
     */
    @Override public boolean listenerThreaded() {
        return this.listenerThreaded;
    }

    /**
     * Should the listener be called on a separate thread if needed.
     */
    @Override public BroadcastOperationRequest listenerThreaded(boolean listenerThreaded) {
        this.listenerThreaded = listenerThreaded;
        return this;
    }

    /**
     * Controls the operation threading model.
     */
    public BroadcastOperationThreading operationThreading() {
        return operationThreading;
    }

    /**
     * Controls the operation threading model.
     */
    public BroadcastOperationRequest operationThreading(BroadcastOperationThreading operationThreading) {
        this.operationThreading = operationThreading;
        return this;
    }

    /**
     * Controls the operation threading model.
     */
    public BroadcastOperationRequest operationThreading(String operationThreading) {
        return operationThreading(BroadcastOperationThreading.fromString(operationThreading, this.operationThreading));
    }

    protected void beforeLocalFork() {

    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        if (indices == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(indices.length);
            for (String index : indices) {
                out.writeUTF(index);
            }
        }
        if (queryHint == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(queryHint);
        }
        out.writeByte(operationThreading.id());
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        if (size == 0) {
            indices = Strings.EMPTY_ARRAY;
        } else {
            indices = new String[size];
            for (int i = 0; i < indices.length; i++) {
                indices[i] = in.readUTF();
            }
        }
        if (in.readBoolean()) {
            queryHint = in.readUTF();
        }
        operationThreading = BroadcastOperationThreading.fromId(in.readByte());
    }
}
