/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.exists.types;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 */
public class TypesExistsRequest extends MasterNodeOperationRequest<TypesExistsRequest> {

    private String[] indices;
    private String[] types;

    private IgnoreIndices ignoreIndices = IgnoreIndices.NONE;

    TypesExistsRequest() {
    }

    public TypesExistsRequest(String[] indices, String... types) {
        this.indices = indices;
        this.types = types;
    }

    public String[] indices() {
        return indices;
    }

    public void indices(String[] indices) {
        this.indices = indices;
    }

    public String[] types() {
        return types;
    }

    public void types(String[] types) {
        this.types = types;
    }

    public IgnoreIndices ignoreIndices() {
        return ignoreIndices;
    }

    public TypesExistsRequest ignoreIndices(IgnoreIndices ignoreIndices) {
        this.ignoreIndices = ignoreIndices;
        return this;
    }

    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indices == null) { // Specifying '*' via rest api results in an empty array
            validationException = addValidationError("index/indices is missing", validationException);
        }
        if (types == null || types.length == 0) {
            validationException = addValidationError("type/types is missing", validationException);
        }

        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        out.writeStringArray(types);
        out.writeByte(ignoreIndices.id());
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        indices = in.readStringArray();
        types = in.readStringArray();
        ignoreIndices = IgnoreIndices.fromId(in.readByte());
    }
}
