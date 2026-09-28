/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.response;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorParserContract;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorResponseUtils;

import java.io.IOException;
import java.util.Optional;

/**
 * Parses the OCI Generative AI error body, which looks like {@code {"code": "NotAuthorizedOrNotFound", "message": "..."}}.
 *
 * @see <a href="https://docs.oracle.com/en-us/iaas/Content/API/References/apierrors.htm">OCI API errors</a>
 */
public class OciGenAiErrorResponseEntity extends UnifiedChatCompletionErrorResponse {

    public static final String OCI_GENAI_ERROR_TYPE = "oci_genai_error";
    private static final String CODE_FIELD = "code";
    private static final String MESSAGE_FIELD = "message";

    public static final UnifiedChatCompletionErrorParserContract ERROR_PARSER = UnifiedChatCompletionErrorResponseUtils
        .createErrorParserWithGenericParser(OciGenAiErrorResponseEntity::doParse);

    private OciGenAiErrorResponseEntity(String errorMessage, @Nullable String code) {
        super(errorMessage, OCI_GENAI_ERROR_TYPE, code, null);
    }

    public static UnifiedChatCompletionErrorResponse fromResponse(HttpResult result) {
        return ERROR_PARSER.parse(result);
    }

    private static Optional<UnifiedChatCompletionErrorResponse> doParse(XContentParser parser) throws IOException {
        var responseMap = parser.map();
        if (responseMap.get(MESSAGE_FIELD) instanceof String message) {
            var code = responseMap.get(CODE_FIELD);
            return Optional.of(new OciGenAiErrorResponseEntity(message, code == null ? null : code.toString()));
        }
        return Optional.empty();
    }
}
