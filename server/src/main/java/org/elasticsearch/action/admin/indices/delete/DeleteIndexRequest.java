/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to delete an index.
 */
public class DeleteIndexRequest extends AcknowledgedRequest<DeleteIndexRequest> implements IndicesRequest.Replaceable {

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.builder()
        .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS)
        .wildcardOptions(
            IndicesOptions.WildcardOptions.builder()
                .matchOpen(true)
                .matchClosed(true)
                .allowEmptyExpressions(true)
                .resolveAliases(false)
                .build()
        )
        .gatekeeperOptions(
            IndicesOptions.GatekeeperOptions.builder()
                .allowAliasToMultipleIndices(false)
                .allowClosedIndices(true)
                .ignoreThrottled(false)
                .allowFailureIndices(true)
                .build()
        )
        .build();

    private String[] indices;
    // Delete index should work by default on both open and closed indices.
    private IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;

    public DeleteIndexRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
    }

    public DeleteIndexRequest() {
        super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, DEFAULT_ACK_TIMEOUT);
    }

    /**
     * Constructs a new delete index request for the specified index.
     *
     * @param index The index to delete. Use "_all" to delete all indices.
     */
    public DeleteIndexRequest(String index) {
        super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, DEFAULT_ACK_TIMEOUT);
        this.indices = new String[] { index };
    }

    /**
     * Constructs a new delete index request for the specified indices.
     *
     * @param indices The indices to delete. Use "_all" to delete all indices.
     */
    public DeleteIndexRequest(String... indices) {
        super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, DEFAULT_ACK_TIMEOUT);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeleteIndexRequest that = (DeleteIndexRequest) o;
        return Arrays.equals(indices, that.indices) && Objects.equals(indicesOptions, that.indicesOptions);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(indicesOptions);
        result = 31 * result + Arrays.hashCode(indices);
        return result;
    }
}
