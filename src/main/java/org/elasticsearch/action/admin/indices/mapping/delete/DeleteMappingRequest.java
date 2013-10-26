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

package org.elasticsearch.action.admin.indices.mapping.delete;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Represents a request to delete a mapping
 */
public class DeleteMappingRequest extends AcknowledgedRequest<DeleteMappingRequest> {

    private String[] indices;

    private String type;

    DeleteMappingRequest() {
    }

    /**
     * Constructs a new delete mapping request against one or more indices. If nothing is set then
     * it will be executed against all indices.
     */
    public DeleteMappingRequest(String... indices) {
        this.indices = indices;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (type == null) {
            validationException = addValidationError("mapping type is missing", validationException);
        }
        return validationException;
    }

    /**
     * Sets the indices this delete mapping operation will execute on.
     */
    public DeleteMappingRequest indices(String[] indices) {
        this.indices = indices;
        return this;
    }

    /**
     * The indices the mappings will be removed from.
     */
    public String[] indices() {
        return indices;
    }

    /**
     * The mapping type.
     */
    public String type() {
        return type;
    }

    /**
     * The type of the mappings to remove.
     */
    public DeleteMappingRequest type(String type) {
        this.type = type;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        indices = new String[in.readVInt()];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = in.readString();
        }
        if (in.readBoolean()) {
            type = in.readString();
        }
        readTimeout(in, Version.V_0_90_6);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (indices == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(indices.length);
            for (String index : indices) {
                out.writeString(index);
            }
        }
        if (type == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeString(type);
        }
        writeTimeout(out, Version.V_0_90_6);
    }
}
