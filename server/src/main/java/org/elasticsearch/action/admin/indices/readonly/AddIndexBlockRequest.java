/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.readonly;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to add a block to an index.
 */
public class AddIndexBlockRequest extends AcknowledgedRequest<AddIndexBlockRequest> implements IndicesRequest.Replaceable {

    private final APIBlock block;
    private String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();

    public AddIndexBlockRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        block = APIBlock.readFrom(in);
    }

    /**
     * Constructs a new request for the specified block and indices
     */
    public AddIndexBlockRequest(APIBlock block, String... indices) {
        this.block = Objects.requireNonNull(block);
        this.indices = Objects.requireNonNull(indices);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (CollectionUtils.isEmpty(indices)) {
            validationException = addValidationError("index is missing", validationException);
        }
        if (block == APIBlock.READ_ONLY_ALLOW_DELETE) {
            validationException = addValidationError("read_only_allow_delete block is for internal use only", validationException);
        }
        return validationException;
    }

    /**
     * Returns the indices to be blocked
     */
    @Override
    public String[] indices() {
        return indices;
    }

    /**
     * Sets the indices to be blocked
     * @param indices the indices to be blocked
     * @return the request itself
     */
    @Override
    public AddIndexBlockRequest indices(String... indices) {
        this.indices = Objects.requireNonNull(indices);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal with wildcard expressions.
     * For example indices that don't exist.
     *
     * @return the desired behaviour regarding indices to ignore and wildcard indices expressions
     */
    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal wild wildcard expressions.
     * For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices to ignore and wildcard indices expressions
     * @return the request itself
     */
    public AddIndexBlockRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    /**
     * Returns the block to be added
     */
    public APIBlock getBlock() {
        return block;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        block.writeTo(out);
    }
}
