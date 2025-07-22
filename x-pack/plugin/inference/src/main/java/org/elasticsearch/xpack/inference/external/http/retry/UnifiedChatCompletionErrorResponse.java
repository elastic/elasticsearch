/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;

public class UnifiedChatCompletionErrorResponse extends ErrorResponse {
    public static final UnifiedChatCompletionErrorParser ERROR_PARSER = new UnifiedChatCompletionErrorParserWrapper();
    public static final UnifiedChatCompletionErrorResponse UNDEFINED_ERROR = new UnifiedChatCompletionErrorResponse();

    private static final ConstructingObjectParser<Optional<UnifiedChatCompletionErrorResponse>, Void> CONSTRUCTING_OBJECT_PARSER =
        new ConstructingObjectParser<>("streaming_error", true, args -> Optional.ofNullable((UnifiedChatCompletionErrorResponse) args[0]));
    private static final ConstructingObjectParser<UnifiedChatCompletionErrorResponse, Void> ERROR_BODY_PARSER =
        new ConstructingObjectParser<>(
            "streaming_error",
            true,
            args -> new UnifiedChatCompletionErrorResponse((String) args[0], (String) args[1], (String) args[2], (String) args[3])
        );

    static {
        ERROR_BODY_PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("message"));
        ERROR_BODY_PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), new ParseField("code"));
        ERROR_BODY_PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), new ParseField("param"));
        ERROR_BODY_PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("type"));

        CONSTRUCTING_OBJECT_PARSER.declareObjectOrNull(
            ConstructingObjectParser.optionalConstructorArg(),
            ERROR_BODY_PARSER,
            null,
            new ParseField("error")
        );
    }

    public static class UnifiedChatCompletionErrorParserWrapper implements UnifiedChatCompletionErrorParser {

        @Override
        public UnifiedChatCompletionErrorResponse parse(HttpResult result) {
            return fromHttpResult(result);
        }

        @Override
        public UnifiedChatCompletionErrorResponse parse(String response) {
            return executeObjectParser(CONSTRUCTING_OBJECT_PARSER, createStringXContentParserFunction(response));
        }
    }

    public static Callable<XContentParser> createHttpResultXContentParserFunction(HttpResult response) {
        return () -> XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, response.body());
    }

    public static Callable<XContentParser> createStringXContentParserFunction(String response) {
        return () -> XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, response);
    }

    /**
     * Standard error response parser. This can be overridden for those subclasses that
     * have a different error response structure.
     * @param response The error response as an HttpResult
     */
    public static UnifiedChatCompletionErrorResponse fromHttpResult(HttpResult response) {
        return executeObjectParser(CONSTRUCTING_OBJECT_PARSER, createHttpResultXContentParserFunction(response));
    }

    public static UnifiedChatCompletionErrorResponse executeObjectParser(
        ConstructingObjectParser<Optional<UnifiedChatCompletionErrorResponse>, Void> objectParser,
        Callable<XContentParser> createXContentParser
    ) {
        return executeGenericParser((parser) -> objectParser.apply(parser, null), createXContentParser);
    }

    public static <E extends Exception> UnifiedChatCompletionErrorResponse executeGenericParser(
        CheckedFunction<XContentParser, Optional<UnifiedChatCompletionErrorResponse>, E> genericParser,
        Callable<XContentParser> createXContentParser
    ) {
        try (XContentParser parser = createXContentParser.call()) {
            return genericParser.apply(parser).orElse(UnifiedChatCompletionErrorResponse.UNDEFINED_ERROR);
        } catch (Exception e) {
            // swallow the error
        }

        return UnifiedChatCompletionErrorResponse.UNDEFINED_ERROR;
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
