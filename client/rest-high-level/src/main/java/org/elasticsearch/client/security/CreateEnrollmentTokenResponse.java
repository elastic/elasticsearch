/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class CreateEnrollmentTokenResponse {
    private String enrollmentToken;

    public CreateEnrollmentTokenResponse(String enrollmentToken) {
        this.enrollmentToken = enrollmentToken;
    }

    public String getEnrollmentToken() {
        return enrollmentToken;
    }

    private static final ParseField ENROLLMENT_TOKEN = new ParseField("enrollment_token");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<CreateEnrollmentTokenResponse, Void> PARSER =
        new ConstructingObjectParser<>(CreateEnrollmentTokenResponse.class.getName(), true,
            a -> new CreateEnrollmentTokenResponse((String) a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ENROLLMENT_TOKEN);
    }

    public static CreateEnrollmentTokenResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateEnrollmentTokenResponse that = (CreateEnrollmentTokenResponse) o;
        return enrollmentToken.equals(that.enrollmentToken);
    }

    @Override public int hashCode() {
        return Objects.hash(enrollmentToken);
    }
}
