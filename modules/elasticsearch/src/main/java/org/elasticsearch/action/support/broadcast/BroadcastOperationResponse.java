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

import org.elasticsearch.action.ActionResponse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class BroadcastOperationResponse implements ActionResponse {

    private int successfulShards;

    private int failedShards;

    protected BroadcastOperationResponse() {
    }

    protected BroadcastOperationResponse(int successfulShards, int failedShards) {
        this.successfulShards = successfulShards;
        this.failedShards = failedShards;
    }

    public int totalShards() {
        return successfulShards + failedShards;
    }

    public int successfulShards() {
        return successfulShards;
    }

    public int failedShards() {
        return failedShards;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        successfulShards = in.readInt();
        failedShards = in.readInt();
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeInt(successfulShards);
        out.writeInt(failedShards);
    }
}
