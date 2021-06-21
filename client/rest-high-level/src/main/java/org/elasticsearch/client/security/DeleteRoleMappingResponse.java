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
 * Response when deleting a role mapping. If the mapping is successfully
 * deleted, the response returns a boolean field "found" {@code true}.
 * Otherwise, "found" is set to {@code false}.
 */
public final class DeleteRoleMappingResponse {

    private final boolean found;

    public DeleteRoleMappingResponse(boolean deleted) {
        this.found = deleted;
    }

    public boolean isFound() {
        return found;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final DeleteRoleMappingResponse that = (DeleteRoleMappingResponse) o;
        return found == that.found;
    }

    @Override
    public int hashCode() {
        return Objects.hash(found);
    }

    private static final ConstructingObjectParser<DeleteRoleMappingResponse, Void> PARSER = new ConstructingObjectParser<>(
            "delete_role_mapping_response", true, args -> new DeleteRoleMappingResponse((boolean) args[0]));
    static {
        PARSER.declareBoolean(constructorArg(), new ParseField("found"));
    }

    public static DeleteRoleMappingResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
