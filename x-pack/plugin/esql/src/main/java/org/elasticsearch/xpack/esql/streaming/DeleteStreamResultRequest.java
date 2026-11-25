/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to delete one stored ES|QL stream, the counterpart of {@code DeleteAsyncResultRequest}.
 * <p>
 * It names exactly one stream by the id the stream POST returned (the encoded form of a {@link StreamId}).
 */
public class DeleteStreamResultRequest extends UntypedActionRequest {

    private final String id;

    public DeleteStreamResultRequest(String id) {
        this.id = id;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }

    @Override
    public ActionRequestValidationException validate() {
        if (Strings.hasLength(id) == false) {
            return addValidationError("id is required", null);
        }
        return null;
    }

    public String id() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeleteStreamResultRequest request = (DeleteStreamResultRequest) o;
        return Objects.equals(id, request.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
