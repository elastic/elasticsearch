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
import org.elasticsearch.util.Nullable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class BroadcastOperationRequest implements ActionRequest {

    private String[] indices;

    @Nullable protected String queryHint;

    private boolean listenerThreaded = false;
    private BroadcastOperationThreading operationThreading = BroadcastOperationThreading.SINGLE_THREAD;

    protected BroadcastOperationRequest() {

    }

    protected BroadcastOperationRequest(String[] indices, @Nullable String queryHint) {
        this.indices = indices;
    }

    public String[] indices() {
        return indices;
    }

    public String queryHint() {
        return queryHint;
    }

    @Override public ActionRequestValidationException validate() {
        return null;
    }

    @Override public boolean listenerThreaded() {
        return this.listenerThreaded;
    }

    @Override public BroadcastOperationRequest listenerThreaded(boolean listenerThreaded) {
        this.listenerThreaded = listenerThreaded;
        return this;
    }

    public BroadcastOperationThreading operationThreading() {
        return operationThreading;
    }

    public BroadcastOperationRequest operationThreading(BroadcastOperationThreading operationThreading) {
        this.operationThreading = operationThreading;
        return this;
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeInt(indices.length);
        for (String index : indices) {
            out.writeUTF(index);
        }
        if (queryHint == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(queryHint);
        }
        out.writeByte(operationThreading.id());
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        indices = new String[in.readInt()];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = in.readUTF();
        }
        if (in.readBoolean()) {
            queryHint = in.readUTF();
        }
        operationThreading = BroadcastOperationThreading.fromId(in.readByte());
    }
}
