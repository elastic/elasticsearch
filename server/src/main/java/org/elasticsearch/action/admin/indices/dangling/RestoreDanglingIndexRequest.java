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

public class RestoreDanglingIndexRequest extends BaseNodesRequest<RestoreDanglingIndexRequest> {
    private String indexUuid;
    private String nodeId;
    private String renameTo;

    public RestoreDanglingIndexRequest(StreamInput in) throws IOException {
        super(in);
        this.indexUuid = in.readString();
        this.nodeId = in.readOptionalString();
        this.renameTo = in.readOptionalString();
    }

    public RestoreDanglingIndexRequest() {
        super(new String[0]);
    }

    @Override
    public ActionRequestValidationException validate() {
        if (this.indexUuid == null) {
            ActionRequestValidationException e = new ActionRequestValidationException();
            e.addValidationError("No index ID specified");
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

    public String getRenameTo() {
        return renameTo;
    }

    public void setRenameTo(String renameTo) {
        this.renameTo = renameTo;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
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
                        if ((boolean) value == false) {
                            throw new IllegalArgumentException("accept_data_loss must be set to true");
                        }
                    } else {
                        throw new IllegalArgumentException("malformed accept_data_loss");
                    }
                    break;

                case "rename_to":
                    if (value instanceof String) {
                        this.setRenameTo((String) value);
                    } else {
                        throw new IllegalArgumentException("malformed rename_to");
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
        out.writeOptionalString(this.nodeId);
        out.writeOptionalString(this.renameTo);
    }
}
