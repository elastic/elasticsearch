/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.delete;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to delete an index. Best created with {@link org.elasticsearch.client.internal.Requests#deleteIndexRequest(String)}.
 */
public class DeleteIndexRequest extends AcknowledgedRequest<DeleteIndexRequest> implements IndicesRequest.Replaceable {

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.fromOptions(
        false,
        true,
        true,
        true,
        false,
        false,
        true,
        false
    );

    private String[] indices;
    // Delete index should work by default on both open and closed indices.
    private IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;

    public DeleteIndexRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
    }

    public DeleteIndexRequest() {}

    /**
     * Constructs a new delete index request for the specified index.
     *
     * @param index The index to delete. Use "_all" to delete all indices.
     */
    public DeleteIndexRequest(String index) {
        this.indices = new String[] { index };
    }

    /**
     * Constructs a new delete index request for the specified indices.
     *
     * @param indices The indices to delete. Use "_all" to delete all indices.
     */
    public DeleteIndexRequest(String... indices) {
        this.indices = indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public DeleteIndexRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (CollectionUtils.isEmpty(indices)) {
            validationException = addValidationError("index / indices is missing", validationException);
        }
        return validationException;
    }

    @Override
    public DeleteIndexRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * The index to delete.
     */
    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
    }
}
