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

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Response for a role being deleted from the native realm
 */
public final class DeleteRoleResponse {

    private final boolean found;

    public DeleteRoleResponse(boolean found) {
        this.found = found;
    }

    public boolean isFound() {
        return this.found;
    }

    private static final ConstructingObjectParser<DeleteRoleResponse, Void> PARSER = new ConstructingObjectParser<>("delete_role_response",
        true, args -> new DeleteRoleResponse((boolean) args[0]));

    static {
        PARSER.declareBoolean(constructorArg(), new ParseField("found"));
    }

    public static DeleteRoleResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
