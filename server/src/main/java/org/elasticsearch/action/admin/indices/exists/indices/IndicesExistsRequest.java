/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.exists.indices;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class IndicesExistsRequest extends MasterNodeReadRequest<IndicesExistsRequest> implements IndicesRequest.Replaceable {

    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, false, true, true);

    // for serialization
    public IndicesExistsRequest() {

    }

    public IndicesExistsRequest(String... indices) {
        this.indices = indices;
    }

    public IndicesExistsRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesExistsRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public IndicesExistsRequest expandWilcardsOpen(boolean expandWildcardsOpen) {
        this.indicesOptions = IndicesOptions.fromOptions(
            indicesOptions.ignoreUnavailable(),
            indicesOptions.allowNoIndices(),
            expandWildcardsOpen,
            indicesOptions.expandWildcardsClosed()
        );
        return this;
    }

    public IndicesExistsRequest expandWilcardsClosed(boolean expandWildcardsClosed) {
        this.indicesOptions = IndicesOptions.fromOptions(
            indicesOptions.ignoreUnavailable(),
            indicesOptions.allowNoIndices(),
            indicesOptions.expandWildcardsOpen(),
            expandWildcardsClosed
        );
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indices == null || indices.length == 0) {
            validationException = addValidationError("index/indices is missing", validationException);
        }
        return validationException;
    }
}
