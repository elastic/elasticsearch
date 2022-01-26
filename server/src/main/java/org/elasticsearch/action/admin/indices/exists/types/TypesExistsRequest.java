/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.exists.types;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class TypesExistsRequest extends MasterNodeReadRequest<TypesExistsRequest> implements IndicesRequest.Replaceable {

    private String[] indices;
    private String[] types;

    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();

    public TypesExistsRequest() {}

    public TypesExistsRequest(String[] indices, String... types) {
        this.indices = indices;
        this.types = types;
    }

    public TypesExistsRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        types = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        out.writeStringArray(types);
        indicesOptions.writeIndicesOptions(out);
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public TypesExistsRequest indices(String... indices) {
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
}
