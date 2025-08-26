/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.Callable;

public class UnifiedChatCompletionErrorResponseUtils {

    /**
     * Creates a {@link UnifiedChatCompletionErrorParserContract} that parses the error response as a string.
     * This is useful for cases where the error response is too complicated to parse.
     *
     * @param type The type of the error, used for categorization.
     * @return A {@link UnifiedChatCompletionErrorParserContract} instance.
     */
    public static UnifiedChatCompletionErrorParserContract createErrorParserWithStringify(String type) {
        return new UnifiedChatCompletionErrorParserContract() {
            @Override
            public UnifiedChatCompletionErrorResponse parse(HttpResult result) {
                try {
                    String errorMessage = new String(result.body(), StandardCharsets.UTF_8);
                    return new UnifiedChatCompletionErrorResponse(errorMessage, type, null, null);
                } catch (Exception e) {
                    // swallow the error
                }

                return UnifiedChatCompletionErrorResponse.UNDEFINED_ERROR;
            }

            @Override
            public UnifiedChatCompletionErrorResponse parse(String result) {
                return new UnifiedChatCompletionErrorResponse(result, type, null, null);
            }
        };
    }

    /**
     * Creates a {@link UnifiedChatCompletionErrorParserContract} that uses a {@link ConstructingObjectParser} to parse the error response.
     * This is useful for cases where the error response can be parsed into an object.
     *
     * @param objectParser The {@link ConstructingObjectParser} to use for parsing the error response.
     * @return A {@link UnifiedChatCompletionErrorParserContract} instance.
     */
    public static UnifiedChatCompletionErrorParserContract createErrorParserWithObjectParser(
        ConstructingObjectParser<Optional<UnifiedChatCompletionErrorResponse>, Void> objectParser
    ) {
        return new UnifiedChatCompletionErrorParser<>((parser) -> objectParser.apply(parser, null));
    }

    /**
     * Creates a {@link UnifiedChatCompletionErrorParserContract} that uses a generic parser function to parse the error response.
     * This is useful for cases where the error response can be parsed using custom logic, typically when parsing from a map.
     *
     * @param genericParser The function that takes an {@link XContentParser} and returns an
     * {@link Optional<UnifiedChatCompletionErrorResponse>}.
     * @param <E> The type of exception that the parser can throw.
     * @return A {@link UnifiedChatCompletionErrorParserContract} instance.
     */
    public static <E extends Exception> UnifiedChatCompletionErrorParserContract createErrorParserWithGenericParser(
        CheckedFunction<XContentParser, Optional<UnifiedChatCompletionErrorResponse>, E> genericParser
    ) {
        return new UnifiedChatCompletionErrorParser<>(genericParser);
    }

    private record UnifiedChatCompletionErrorParser<E extends Exception>(
        CheckedFunction<XContentParser, Optional<UnifiedChatCompletionErrorResponse>, E> genericParser
    ) implements UnifiedChatCompletionErrorParserContract {

        @Override
        public UnifiedChatCompletionErrorResponse parse(HttpResult result) {
            return executeGenericParser(genericParser, createHttpResultXContentParserFunction(result));
        }

        @Override
        public UnifiedChatCompletionErrorResponse parse(String result) {
            return executeGenericParser(genericParser, createStringXContentParserFunction(result));
        }

    }

    private static Callable<XContentParser> createHttpResultXContentParserFunction(HttpResult response) {
        return () -> XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, response.body());
    }

    private static Callable<XContentParser> createStringXContentParserFunction(String response) {
        return () -> XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, response);
    }

    private static <E extends Exception> UnifiedChatCompletionErrorResponse executeGenericParser(
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

    private UnifiedChatCompletionErrorResponseUtils() {}
}
