/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.action.admin.indices.dangling;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Used when querying every node in the cluster for dangling indices, in response to a list request.
 */
public class NodeDanglingIndicesResponse extends BaseNodeResponse {
    private final List<IndexMetaData> indexMetaData;

    public List<IndexMetaData> getDanglingIndices() {
        return this.indexMetaData;
    }

    public NodeDanglingIndicesResponse(DiscoveryNode node, List<IndexMetaData> indexMetaData) {
        super(node);
        this.indexMetaData = indexMetaData;
    }

    protected NodeDanglingIndicesResponse(StreamInput in) throws IOException {
        super(in);

        final int size = in.readInt();
        this.indexMetaData = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            this.indexMetaData.add(IndexMetaData.readFrom(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeInt(this.indexMetaData.size());
        for (IndexMetaData indexMetaData : this.indexMetaData) {
            indexMetaData.writeTo(out);
        }
    }
}
