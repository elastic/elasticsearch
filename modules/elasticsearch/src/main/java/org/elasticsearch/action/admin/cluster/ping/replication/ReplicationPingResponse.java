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

package org.elasticsearch.action.admin.cluster.ping.replication;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.util.io.Streamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author kimchy (Shay Banon)
 */
public class ReplicationPingResponse implements ActionResponse, Streamable {

    private Map<String, IndexReplicationPingResponse> responses = new HashMap<String, IndexReplicationPingResponse>();

    ReplicationPingResponse() {

    }

    public Map<String, IndexReplicationPingResponse> indices() {
        return responses;
    }

    public IndexReplicationPingResponse index(String index) {
        return responses.get(index);
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            IndexReplicationPingResponse response = new IndexReplicationPingResponse();
            response.readFrom(in);
            responses.put(response.index(), response);
        }
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeInt(responses.size());
        for (IndexReplicationPingResponse response : responses.values()) {
            response.writeTo(out);
        }
    }
}