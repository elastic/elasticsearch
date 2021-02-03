/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Response for a user being deleted from the native realm
 */
public final class DeleteUserResponse extends AcknowledgedResponse {

    private static final String PARSE_FIELD_NAME = "found";

    private static final ConstructingObjectParser<DeleteUserResponse, Void> PARSER = AcknowledgedResponse
        .generateParser("delete_user_response", DeleteUserResponse::new, PARSE_FIELD_NAME);

    public DeleteUserResponse(boolean acknowledged) {
        super(acknowledged);
    }

    public static DeleteUserResponse fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    protected String getFieldName() {
        return PARSE_FIELD_NAME;
    }
}
