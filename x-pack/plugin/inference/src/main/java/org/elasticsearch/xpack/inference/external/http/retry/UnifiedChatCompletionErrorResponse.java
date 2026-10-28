/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.util.Objects;
import java.util.Optional;

public class UnifiedChatCompletionErrorResponse extends ErrorResponse {

    // Default for testing
    static final ConstructingObjectParser<Optional<UnifiedChatCompletionErrorResponse>, Void> ERROR_OBJECT_PARSER =
        new ConstructingObjectParser<>("streaming_error", true, args -> Optional.ofNullable((UnifiedChatCompletionErrorResponse) args[0]));
    private static final ConstructingObjectParser<UnifiedChatCompletionErrorResponse, Void> ERROR_BODY_PARSER =
        new ConstructingObjectParser<>(
            "streaming_error",
            true,
            args -> new UnifiedChatCompletionErrorResponse((String) args[0], (String) args[1], (String) args[2], (String) args[3])
        );

    static {
        ERROR_BODY_PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("message"));
        ERROR_BODY_PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("type"));
        ERROR_BODY_PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), new ParseField("code"));
        ERROR_BODY_PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), new ParseField("param"));

        ERROR_OBJECT_PARSER.declareObjectOrNull(
            ConstructingObjectParser.optionalConstructorArg(),
            ERROR_BODY_PARSER,
            null,
            new ParseField("error")
        );
    }

    public static final UnifiedChatCompletionErrorParserContract ERROR_PARSER = UnifiedChatCompletionErrorResponseUtils
        .createErrorParserWithObjectParser(ERROR_OBJECT_PARSER);
    public static final UnifiedChatCompletionErrorResponse UNDEFINED_ERROR = new UnifiedChatCompletionErrorResponse();

    /**
     * Standard error response parser.
     * @param response The error response as an HttpResult
     */
    public static UnifiedChatCompletionErrorResponse fromHttpResult(HttpResult response) {
        return ERROR_PARSER.parse(response);
    }

    @Nullable
    private final String code;
    @Nullable
    private final String param;
    private final String type;

    public UnifiedChatCompletionErrorResponse(String errorMessage, String type, @Nullable String code, @Nullable String param) {
        super(errorMessage);
        this.code = code;
        this.param = param;
        this.type = Objects.requireNonNull(type);
    }

    private UnifiedChatCompletionErrorResponse() {
        super(false);
        this.code = null;
        this.param = null;
        this.type = "unknown";
    }

    @Nullable
    public String code() {
        return code;
    }

    @Nullable
    public String param() {
        return param;
    }

    public String type() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        UnifiedChatCompletionErrorResponse that = (UnifiedChatCompletionErrorResponse) o;
        return Objects.equals(code, that.code) && Objects.equals(param, that.param) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), code, param, type);
    }
}
