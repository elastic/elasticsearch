/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request to retrieve roles from the security index
 */
public class GetRolesRequest extends ActionRequest {

    private String[] names = Strings.EMPTY_ARRAY;

    public GetRolesRequest(StreamInput in) throws IOException {
        super(in);
        names = in.readStringArray();
    }

    public GetRolesRequest() {}

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (names == null) {
            validationException = addValidationError("role is missing", validationException);
        }
        return validationException;
    }

    public void names(String... names) {
        this.names = names;
    }

    public String[] names() {
        return names;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(names);
    }
}
