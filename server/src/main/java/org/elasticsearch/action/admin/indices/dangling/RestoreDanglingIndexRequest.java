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
import java.util.Map;

/**
 * Represents a request to restore a particular dangling index, specified
 * by its UUID and optionally the node ID, if the dangling index exists on
 * more than one node. The {@link #acceptDataLoss} flag must also be
 * explicitly set to true, or later validation will fail.
 */
public class RestoreDanglingIndexRequest extends BaseNodesRequest<RestoreDanglingIndexRequest> {
    private String indexUuid;
    private boolean acceptDataLoss;
    private String nodeId;

    public RestoreDanglingIndexRequest(StreamInput in) throws IOException {
        super(in);
        this.indexUuid = in.readString();
        this.acceptDataLoss = in.readBoolean();
        this.nodeId = in.readOptionalString();
    }

    public RestoreDanglingIndexRequest() {
        super(new String[0]);
    }

    public RestoreDanglingIndexRequest(String indexUuid, boolean acceptDataLoss) {
        this();
        this.indexUuid = indexUuid;
        this.acceptDataLoss = acceptDataLoss;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (this.indexUuid == null || this.indexUuid.isEmpty()) {
            ActionRequestValidationException e = new ActionRequestValidationException();
            e.addValidationError("No index UUID specified");
            return e;
        }

        return null;
    }

    public String getIndexUuid() {
        return indexUuid;
    }

    public void setIndexUuid(String indexUuid) {
        this.indexUuid = indexUuid;
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
        return "restore dangling index";
    }

    public void source(Map<String, Object> source) {
        source.forEach((name, value) -> {
            switch (name) {
                case "accept_data_loss":
                    if (value instanceof Boolean) {
                        this.acceptDataLoss = (boolean) value;
                    } else {
                        throw new IllegalArgumentException("malformed accept_data_loss");
                    }
                    break;

                case "node_id":
                    if (value instanceof String) {
                        this.setNodeId((String) value);
                    } else {
                        throw new IllegalArgumentException("malformed node_id");
                    }
                    break;

                default:
                    throw new IllegalArgumentException("Unknown parameter " + name);
            }
        });
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(this.indexUuid);
        out.writeBoolean(this.acceptDataLoss);
        out.writeOptionalString(this.nodeId);
    }
}
