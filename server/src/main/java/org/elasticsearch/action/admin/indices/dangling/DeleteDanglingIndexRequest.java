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
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Map;

/**
 * Represents a request to delete a particular dangling index, specified by its UUID. The {@link #acceptDataLoss}
 * flag must also be explicitly set to true, or later validation will fail.
 */
public class DeleteDanglingIndexRequest extends MasterNodeRequest<DeleteDanglingIndexRequest> {
    private String indexUuid;
    private boolean acceptDataLoss = false;

    public DeleteDanglingIndexRequest(StreamInput in) throws IOException {
        super(in);
        this.indexUuid = in.readString();
        this.acceptDataLoss = in.readBoolean();
    }

    public DeleteDanglingIndexRequest() {
        super();
    }

    public DeleteDanglingIndexRequest(String indexUuid, boolean acceptDataLoss) {
        super();
        this.indexUuid = indexUuid;
        this.acceptDataLoss = acceptDataLoss;
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

    public boolean isAcceptDataLoss() {
        return acceptDataLoss;
    }

    public void setAcceptDataLoss(boolean acceptDataLoss) {
        this.acceptDataLoss = acceptDataLoss;
    }

    @Override
    public String toString() {
        return "delete dangling index";
    }

    public void source(Map<String, Object> source) {
        source.forEach((name, value) -> {
            if ("accept_data_loss".equals(name)) {
                if (value instanceof Boolean) {
                    this.acceptDataLoss = (boolean) value;
                } else {
                    throw new IllegalArgumentException("malformed accept_data_loss");
                }
            } else {
                throw new IllegalArgumentException("Unknown parameter " + name);
            }
        });
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(this.indexUuid);
        out.writeBoolean(this.acceptDataLoss);
    }
}
