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
package org.elasticsearch.action.admin.indices.exists.types;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 */
public class TypesExistsRequest extends MasterNodeReadOperationRequest<TypesExistsRequest> implements IndicesRequest.Replaceable {

    private String[] indices;
    private String[] types;

    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();

    TypesExistsRequest() {
    }

    public TypesExistsRequest(String[] indices, String... types) {
        this.indices = indices;
        this.types = types;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public TypesExistsRequest indices(String[] indices) {
        this.indices = indices;
        return this;
    }

    public String[] types() {
        return types;
    }

    public void types(String[] types) {
        this.types = types;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public TypesExistsRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
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
        indicesOptions.writeIndicesOptions(out);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        indices = in.readStringArray();
        types = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
    }
}
