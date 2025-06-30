/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.response;

import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;

import java.nio.charset.StandardCharsets;

public class MixedbreadErrorResponse extends ErrorResponse {
    public MixedbreadErrorResponse(String message) {
        super(message);
    }

    public static ErrorResponse fromResponse(HttpResult response) {
        try {
            String errorMessage = new String(response.body(), StandardCharsets.UTF_8);
            return new MixedbreadErrorResponse(errorMessage);
        } catch (Exception e) {
            // swallow the error
        }
        return ErrorResponse.UNDEFINED_ERROR;
    }
}
