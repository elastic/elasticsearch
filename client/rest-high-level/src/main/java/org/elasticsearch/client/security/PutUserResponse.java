/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Response when adding a user to the native realm. Returns a
 * single boolean field for whether the user was created or updated.
 */
public final class PutUserResponse {

    private final boolean created;

    public PutUserResponse(boolean created) {
        this.created = created;
    }

    public boolean isCreated() {
        return created;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PutUserResponse that = (PutUserResponse) o;
        return created == that.created;
    }

    @Override
    public int hashCode() {
        return Objects.hash(created);
    }

    private static final ConstructingObjectParser<PutUserResponse, Void> PARSER = new ConstructingObjectParser<>("put_user_response",
        true, args -> new PutUserResponse((boolean) args[0]));

    static {
        PARSER.declareBoolean(constructorArg(), new ParseField("created"));
    }

    public static PutUserResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
