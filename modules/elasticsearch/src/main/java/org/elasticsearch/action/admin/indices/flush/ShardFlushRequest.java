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

package org.elasticsearch.action.admin.indices.flush;

import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class ShardFlushRequest extends ShardReplicationOperationRequest {

    private int shardId;

    public ShardFlushRequest(IndexFlushRequest indexFlushRequest, int shardId) {
        this(indexFlushRequest.index(), shardId);
        timeout = indexFlushRequest.timeout();
    }

    public ShardFlushRequest(String index, int shardId) {
        this.index = index;
        this.shardId = shardId;
    }

    ShardFlushRequest() {
    }

    public int shardId() {
        return this.shardId;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        shardId = in.readInt();
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(shardId);
    }
}