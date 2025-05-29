/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.streaming;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;

import java.util.Objects;
import java.util.Optional;

public class StreamingErrorResponse extends ErrorResponse {
    private static final ConstructingObjectParser<Optional<ErrorResponse>, Void> ERROR_PARSER = new ConstructingObjectParser<>(
        "streaming_error",
        true,
        args -> Optional.ofNullable((StreamingErrorResponse) args[0])
    );
    private static final ConstructingObjectParser<StreamingErrorResponse, Void> ERROR_BODY_PARSER = new ConstructingObjectParser<>(
        "streaming_error",
        true,
        args -> new StreamingErrorResponse((String) args[0], (String) args[1], (String) args[2], (String) args[3])
    );

    static {
        ERROR_BODY_PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("message"));
        ERROR_BODY_PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), new ParseField("code"));
        ERROR_BODY_PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), new ParseField("param"));
        ERROR_BODY_PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("type"));

        ERROR_PARSER.declareObjectOrNull(
            ConstructingObjectParser.optionalConstructorArg(),
            ERROR_BODY_PARSER,
            null,
            new ParseField("error")
        );
    }

    /**
     * Standard error response parser. This can be overridden for those subclasses that
     * have a different error response structure.
     * @param response The error response as an HttpResult
     */
    public static ErrorResponse fromResponse(HttpResult response) {
        try (
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, response.body())
        ) {
            return ERROR_PARSER.apply(parser, null).orElse(ErrorResponse.UNDEFINED_ERROR);
        } catch (Exception e) {
            // swallow the error
        }

        return ErrorResponse.UNDEFINED_ERROR;
    }

    /**
     * Standard error response parser. This can be overridden for those subclasses that
     * have a different error response structure.
     * @param response The error response as a string
     */
    public static ErrorResponse fromString(String response) {
        try (
            XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, response)
        ) {
            return ERROR_PARSER.apply(parser, null).orElse(ErrorResponse.UNDEFINED_ERROR);
        } catch (Exception e) {
            // swallow the error
        }

        return ErrorResponse.UNDEFINED_ERROR;
    }

    @Nullable
    private final String code;
    @Nullable
    private final String param;
    private final String type;

    StreamingErrorResponse(String errorMessage, @Nullable String code, @Nullable String param, String type) {
        super(errorMessage);
        this.code = code;
        this.param = param;
        this.type = Objects.requireNonNull(type);
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
}
