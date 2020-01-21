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

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
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
    private String indexUUID;
    private boolean acceptDataLoss;
    private String nodeId;

    public ImportDanglingIndexRequest(StreamInput in) throws IOException {
        super(in);
        this.indexUUID = in.readString();
        this.acceptDataLoss = in.readBoolean();
        this.nodeId = in.readOptionalString();
    }

    public ImportDanglingIndexRequest() {
        super(new String[0]);
    }

    public ImportDanglingIndexRequest(String indexUUID, boolean acceptDataLoss) {
        this();
        this.indexUUID = indexUUID;
        this.acceptDataLoss = acceptDataLoss;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (this.indexUUID == null || this.indexUUID.isEmpty()) {
            ActionRequestValidationException e = new ActionRequestValidationException();
            e.addValidationError("No index UUID specified");
            return e;
        }

        // acceptDataLoss is validated later in the transport action, so that the API call can
        // be made to check that the UUID exists.

        return null;
    }

    public String getIndexUUID() {
        return indexUUID;
    }

    public void setIndexUUID(String indexUUID) {
        this.indexUUID = indexUUID;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public boolean isAcceptDataLoss() {
        return acceptDataLoss;
    }

    public void setAcceptDataLoss(boolean acceptDataLoss) {
        this.acceptDataLoss = acceptDataLoss;
    }

    @Override
    public String toString() {
        return "import dangling index";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(this.indexUUID);
        out.writeBoolean(this.acceptDataLoss);
        out.writeOptionalString(this.nodeId);
    }
}
