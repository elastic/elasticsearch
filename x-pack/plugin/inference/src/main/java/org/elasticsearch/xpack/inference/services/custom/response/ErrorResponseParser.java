/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.response;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.MapPathExtractor;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.ERROR_PARSER;
import static org.elasticsearch.xpack.inference.services.custom.response.BaseCustomResponseParser.toType;

public class ErrorResponseParser implements ToXContentFragment, Function<HttpResult, ErrorResponse> {

    private static final Logger logger = LogManager.getLogger(ErrorResponseParser.class);
    public static final String MESSAGE_PATH = "path";

    private final String messagePath;
    private final String inferenceId;

    public static ErrorResponseParser fromMap(
        Map<String, Object> responseParserMap,
        String scope,
        String inferenceId,
        ValidationException validationException
    ) {
        var path = extractRequiredString(responseParserMap, MESSAGE_PATH, String.join(".", scope, ERROR_PARSER), validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new ErrorResponseParser(path, inferenceId);
    }

    public ErrorResponseParser(String messagePath, String inferenceId) {
        this.messagePath = Objects.requireNonNull(messagePath);
        this.inferenceId = Objects.requireNonNull(inferenceId);
    }

    public ErrorResponseParser(StreamInput in) throws IOException {
        this.messagePath = in.readString();
        this.inferenceId = in.readString();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(messagePath);
        out.writeString(inferenceId);
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(ERROR_PARSER);
        {
            builder.field(MESSAGE_PATH, messagePath);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ErrorResponseParser that = (ErrorResponseParser) o;
        return Objects.equals(messagePath, that.messagePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messagePath);
    }

    @Override
    public ErrorResponse apply(HttpResult httpResult) {
        try (
            XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, httpResult.body())
        ) {
            var map = jsonParser.map();
            // NOTE: This deviates from what we've done in the past. In the ErrorMessageResponseEntity logic
            // if we find the top level error field we'll return a response with an empty message but indicate
            // that we found the structure of the error object. Here if we're missing the final field we will return
            // a ErrorResponse.UNDEFINED_ERROR which will indicate that we did not find the structure even if for example
            // the outer error field does exist, but it doesn't contain the nested field we were looking for.
            // If in the future we want the previous behavior, we can add a new message_path field or something and have
            // the current path field point to the field that indicates whether we found an error object.
            var errorText = toType(MapPathExtractor.extract(map, messagePath).extractedObject(), String.class, messagePath);
            return new ErrorResponse(errorText);
        } catch (Exception e) {
            var resultAsString = new String(httpResult.body(), StandardCharsets.UTF_8);

            logger.info(
                Strings.format(
                    "Failed to parse error object for custom service inference id [%s], message path: [%s], result as string: [%s]",
                    inferenceId,
                    messagePath,
                    resultAsString
                ),
                e
            );

            return new ErrorResponse(Strings.format("Unable to parse the error, response body: [%s]", resultAsString));
        }
    }
}
