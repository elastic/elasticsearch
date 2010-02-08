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

package org.elasticsearch.action.support.shards;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.util.Strings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class ShardsOperationRequest implements ActionRequest {

    private String[] indices;

    private boolean listenerThreaded = false;
    private ShardsOperationThreading operationThreading = ShardsOperationThreading.SINGLE_THREAD;

    protected ShardsOperationRequest() {
    }

    protected ShardsOperationRequest(String... indices) {
        this.indices = indices == null ? Strings.EMPTY_ARRAY : indices;
    }

    public String[] indices() {
        return this.indices;
    }

    @Override public ActionRequestValidationException validate() {
        return null;
    }

    @Override public boolean listenerThreaded() {
        return this.listenerThreaded;
    }

    @Override public ShardsOperationRequest listenerThreaded(boolean listenerThreaded) {
        this.listenerThreaded = listenerThreaded;
        return this;
    }

    public ShardsOperationThreading operationThreading() {
        return operationThreading;
    }

    public ShardsOperationRequest operationThreading(ShardsOperationThreading operationThreading) {
        this.operationThreading = operationThreading;
        return this;
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeInt(indices.length);
        for (String index : indices) {
            out.writeUTF(index);
        }
        out.writeByte(operationThreading.id());
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        indices = new String[in.readInt()];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = in.readUTF();
        }
        operationThreading = ShardsOperationThreading.fromId(in.readByte());
    }
}
