/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.enrollment;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * Response containing the enrollment token string that was generated from an enrollment token creation request.
 */
public class CreateEnrollmentTokenResponse  extends ActionResponse {
    private static final ParseField ENROLLMENT_TOKEN = new ParseField("enrollment_token");

    private String enrollmentTokenString;

    public CreateEnrollmentTokenResponse(StreamInput in) throws IOException {
        super(in);
        enrollmentTokenString = in.readString();
    }

    public CreateEnrollmentTokenResponse(String enrollmentTokenString) {
        this.enrollmentTokenString = Objects.requireNonNull(enrollmentTokenString);
    }

    public String getEnrollmentToken() {
        return enrollmentTokenString;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(enrollmentTokenString);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateEnrollmentTokenResponse that = (CreateEnrollmentTokenResponse) o;
        return Objects.equals(enrollmentTokenString, that.enrollmentTokenString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(enrollmentTokenString);
    }
}
