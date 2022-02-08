/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.profile;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class SearchProfilesRequest extends ActionRequest {

    private final Set<String> dataKeys;
    private final String query;
    private final int size;

    public SearchProfilesRequest(Set<String> dataKeys, String query, int size) {
        this.dataKeys = Objects.requireNonNull(dataKeys, "data parameter must not be null");
        this.query = Objects.requireNonNull(query, "query must not be null");
        this.size = size;
    }

    public SearchProfilesRequest(StreamInput in) throws IOException {
        super(in);
        this.dataKeys = in.readSet(StreamInput::readString);
        this.query = in.readOptionalString();
        this.size = in.readVInt();
    }

    public Set<String> getDataKeys() {
        return dataKeys;
    }

    public String getQuery() {
        return query;
    }

    public int getSize() {
        return size;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringCollection(dataKeys);
        out.writeOptionalString(query);
        out.writeVInt(size);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (size < 0) {
            validationException = addValidationError("[size] parameter cannot be negative but was [" + size + "]", validationException);
        }
        return validationException;
    }
}
