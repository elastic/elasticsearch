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

package org.elasticsearch.action.admin.indices.gateway.snapshot;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Reponse for the gateway snapshot action.
 *
 * @author kimchy (shay.banon)
 */
public class GatewaySnapshotResponse implements ActionResponse, Streamable, Iterable<IndexGatewaySnapshotResponse> {

    private Map<String, IndexGatewaySnapshotResponse> indices = new HashMap<String, IndexGatewaySnapshotResponse>();

    GatewaySnapshotResponse() {

    }

    @Override public Iterator<IndexGatewaySnapshotResponse> iterator() {
        return indices.values().iterator();
    }

    /**
     * A map of index level responses of the gateway snapshot operation.
     */
    public Map<String, IndexGatewaySnapshotResponse> indices() {
        return indices;
    }

    /**
     * A map of index level responses of the gateway snapshot operation.
     */
    public Map<String, IndexGatewaySnapshotResponse> getIndices() {
        return indices();
    }

    /**
     * The index level gateway snapshot response for the given index.
     */
    public IndexGatewaySnapshotResponse index(String index) {
        return indices.get(index);
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            IndexGatewaySnapshotResponse response = new IndexGatewaySnapshotResponse();
            response.readFrom(in);
            indices.put(response.index(), response);
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(indices.size());
        for (IndexGatewaySnapshotResponse indexGatewaySnapshotResponse : indices.values()) {
            indexGatewaySnapshotResponse.writeTo(out);
        }
    }
}