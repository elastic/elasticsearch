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

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Represents a request to import a particular dangling index, specified
 * by its UUID and optionally the node ID, if the dangling index exists on
 * more than one node. The {@link #acceptDataLoss} flag must also be
 * explicitly set to true, or later validation will fail.
 */
public class ImportDanglingIndexRequest extends BaseNodesRequest<ImportDanglingIndexRequest> {
    private final String indexUUID;
    private final boolean acceptDataLoss;
    private final String nodeId;

    public ImportDanglingIndexRequest(StreamInput in) throws IOException {
        super(in);
        this.indexUUID = in.readString();
        this.acceptDataLoss = in.readBoolean();
        this.nodeId = in.readOptionalString();
    }

    public ImportDanglingIndexRequest(String indexUUID, boolean acceptDataLoss, @Nullable String nodeId) {
        super(new String[0]);
        this.indexUUID = Strings.requireNonEmpty(indexUUID, "indexUUID cannot be null or empty");
        this.acceptDataLoss = acceptDataLoss;
        this.nodeId = nodeId;
    }

    public String getIndexUUID() {
        return indexUUID;
    }

    public String getNodeId() {
        return nodeId;
    }

    public boolean isAcceptDataLoss() {
        return acceptDataLoss;
    }

    @Override
    public String toString() {
        return String.format(
            "ImportDanglingIndexRequest{indexUUID='%s', acceptDataLoss=%s, nodeId='%s'}",
            indexUUID,
            acceptDataLoss,
            nodeId
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(this.indexUUID);
        out.writeBoolean(this.acceptDataLoss);
        out.writeOptionalString(this.nodeId);
    }
}
