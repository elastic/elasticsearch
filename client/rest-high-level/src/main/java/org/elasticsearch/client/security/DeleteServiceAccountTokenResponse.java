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
 * Response for a service account token deletion
 */
public final class DeleteServiceAccountTokenResponse extends AcknowledgedResponse {

    private static final String PARSE_FIELD_NAME = "found";

    private static final ConstructingObjectParser<DeleteServiceAccountTokenResponse, Void> PARSER = AcknowledgedResponse
        .generateParser("delete_service_account_token_response", DeleteServiceAccountTokenResponse::new, PARSE_FIELD_NAME);

    public DeleteServiceAccountTokenResponse(boolean acknowledged) {
        super(acknowledged);
    }

    public static DeleteServiceAccountTokenResponse fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    protected String getFieldName() {
        return PARSE_FIELD_NAME;
    }
}
