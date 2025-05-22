/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

/**
 * A listener which converts a successful {@link ActionResponse.Empty} action response into a {@code 200 OK} REST response with empty body.
 */
public final class EmptyResponseListener extends RestResponseListener<ActionResponse.Empty> {
    public EmptyResponseListener(RestChannel channel) {
        super(channel);
    }

    @Override
    public RestResponse buildResponse(ActionResponse.Empty ignored) throws Exception {
        // Content-type header is not required for an empty body but some clients may expect it; the empty body is a valid text/plain entity
        // so we use that here.
        return new RestResponse(RestStatus.OK, RestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY);
    }

    /**
     * Capability name for APIs that previously would return an invalid zero-byte {@code application/json} response so that the YAML test
     * runner can avoid those APIs.
     */
    public static final String PLAIN_TEXT_EMPTY_RESPONSE_CAPABILITY_NAME = "plain_text_empty_response";
}
