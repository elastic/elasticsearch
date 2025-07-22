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
import java.util.function.Function;

public class UnifiedChatCompletionErrorResponseUtils {

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

    public static UnifiedChatCompletionErrorParserContract createErrorParserWithObjectParser(
        ConstructingObjectParser<Optional<UnifiedChatCompletionErrorResponse>, Void> objectParser
    ) {
        return new UnifiedChatCompletionErrorParser<>(
            UnifiedChatCompletionErrorResponseUtils::createHttpResultXContentParserFunction,
            UnifiedChatCompletionErrorResponseUtils::createStringXContentParserFunction,
            (parser) -> objectParser.apply(parser, null)
        );
    }

    public static <E extends Exception> UnifiedChatCompletionErrorParserContract createErrorParserWithGenericParser(
        CheckedFunction<XContentParser, Optional<UnifiedChatCompletionErrorResponse>, E> genericParser
    ) {
        return new UnifiedChatCompletionErrorParser<>(
            UnifiedChatCompletionErrorResponseUtils::createHttpResultXContentParserFunction,
            UnifiedChatCompletionErrorResponseUtils::createStringXContentParserFunction,
            genericParser
        );
    }

    private record UnifiedChatCompletionErrorParser<E extends Exception>(
        Function<HttpResult, Callable<XContentParser>> httpResultXContentParsingFunctionCreator,
        Function<String, Callable<XContentParser>> stringXContentParsingFunctionCreator,
        CheckedFunction<XContentParser, Optional<UnifiedChatCompletionErrorResponse>, E> genericParser
    ) implements UnifiedChatCompletionErrorParserContract {

        @Override
        public UnifiedChatCompletionErrorResponse parse(HttpResult result) {
            return executeGenericParser(genericParser, httpResultXContentParsingFunctionCreator.apply(result));
        }

        @Override
        public UnifiedChatCompletionErrorResponse parse(String result) {
            return executeGenericParser(genericParser, stringXContentParsingFunctionCreator.apply(result));
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
}
