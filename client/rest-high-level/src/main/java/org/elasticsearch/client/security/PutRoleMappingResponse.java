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
 * Response when adding/updating a role mapping. Returns a boolean field for
 * whether the role mapping was created or updated.
 */
public final class PutRoleMappingResponse {

    private final boolean created;

    public PutRoleMappingResponse(boolean created) {
        this.created = created;
    }

    public boolean isCreated() {
        return created;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final PutRoleMappingResponse that = (PutRoleMappingResponse) o;
        return created == that.created;
    }

    @Override
    public int hashCode() {
        return Objects.hash(created);
    }

    private static final ConstructingObjectParser<PutRoleMappingResponse, Void> PARSER = new ConstructingObjectParser<>(
            "put_role_mapping_response", true, args -> new PutRoleMappingResponse((boolean) args[0]));
    static {
        ConstructingObjectParser<Boolean, Void> roleMappingParser = new ConstructingObjectParser<>(
                "put_role_mapping_response.role_mapping", true, args -> (Boolean) args[0]);
        roleMappingParser.declareBoolean(constructorArg(), new ParseField("created"));
        PARSER.declareObject(constructorArg(), roleMappingParser::parse, new ParseField("role_mapping"));
    }

    public static PutRoleMappingResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
