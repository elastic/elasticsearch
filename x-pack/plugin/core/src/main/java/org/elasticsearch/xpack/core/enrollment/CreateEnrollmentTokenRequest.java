/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.enrollment;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Represents a request to create an enrollment token based on the provided information.
 */

public class CreateEnrollmentTokenRequest extends ActionRequest {

    public CreateEnrollmentTokenRequest(StreamInput in) throws IOException {
        super(in);
    }

    public CreateEnrollmentTokenRequest() {
        super();
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public String toString() {
        return "get_enrollment_token";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}
