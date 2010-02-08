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

package org.elasticsearch.action.support.nodes;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.util.Strings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class NodesOperationRequest implements ActionRequest {

    public static String[] ALL_NODES = Strings.EMPTY_ARRAY;

    private String[] nodesIds;

    private boolean listenerThreaded = false;

    protected NodesOperationRequest() {

    }

    protected NodesOperationRequest(String... nodesIds) {
        this.nodesIds = nodesIds;
    }

    @Override public NodesOperationRequest listenerThreaded(boolean listenerThreaded) {
        this.listenerThreaded = listenerThreaded;
        return this;
    }

    @Override public boolean listenerThreaded() {
        return this.listenerThreaded;
    }

    public String[] nodesIds() {
        return nodesIds;
    }

    @Override public ActionRequestValidationException validate() {
        return null;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        nodesIds = new String[in.readInt()];
        for (int i = 0; i < nodesIds.length; i++) {
            nodesIds[i] = in.readUTF();
        }
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        if (nodesIds == null) {
            out.writeInt(0);
        } else {
            out.writeInt(nodesIds.length);
            for (String nodeId : nodesIds) {
                out.writeUTF(nodeId);
            }
        }
    }
}
