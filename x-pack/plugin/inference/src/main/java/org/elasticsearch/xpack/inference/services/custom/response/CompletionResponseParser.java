/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.response;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.common.MapPathExtractor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.JSON_PARSER;

public class CompletionResponseParser extends BaseCustomResponseParser<ChatCompletionResults> {

    public static final String NAME = "completion_response_parser";
    public static final String COMPLETION_PARSER_RESULT = "completion_result";

    private final String completionResultPath;

    public static CompletionResponseParser fromMap(
        Map<String, Object> responseParserMap,
        String scope,
        ValidationException validationException
    ) {
        var path = extractRequiredString(
            responseParserMap,
            COMPLETION_PARSER_RESULT,
            String.join(".", scope, JSON_PARSER),
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new CompletionResponseParser(path);
    }

    public CompletionResponseParser(String completionResultPath) {
        this.completionResultPath = Objects.requireNonNull(completionResultPath);
    }

    public CompletionResponseParser(StreamInput in) throws IOException {
        this.completionResultPath = in.readString();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(completionResultPath);
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(JSON_PARSER);
        {
            builder.field(COMPLETION_PARSER_RESULT, completionResultPath);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompletionResponseParser that = (CompletionResponseParser) o;
        return Objects.equals(completionResultPath, that.completionResultPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(completionResultPath);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public ChatCompletionResults transform(Map<String, Object> map) {
        var result = MapPathExtractor.extract(map, completionResultPath);
        var extractedField = result.extractedObject();

        validateNonNull(extractedField, completionResultPath);

        if (extractedField instanceof List<?> extractedList) {
            var completionList = castList(extractedList, (obj, fieldName) -> toType(obj, String.class, fieldName), completionResultPath);
            return new ChatCompletionResults(completionList.stream().map(ChatCompletionResults.Result::new).toList());
        } else if (extractedField instanceof String extractedString) {
            return new ChatCompletionResults(List.of(new ChatCompletionResults.Result(extractedString)));
        } else {
            throw new IllegalArgumentException(
                Strings.format(
                    "Extracted field [%s] from path [%s] is an invalid type, expected a list or a string but received [%s]",
                    result.getArrayFieldName(0),
                    completionResultPath,
                    extractedField.getClass().getSimpleName()
                )
            );
        }
    }
}
