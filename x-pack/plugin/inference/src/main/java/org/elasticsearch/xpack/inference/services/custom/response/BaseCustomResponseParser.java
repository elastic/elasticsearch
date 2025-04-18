/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.response;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public abstract class BaseCustomResponseParser<T extends InferenceServiceResults> implements CustomResponseParser {

    @Override
    public InferenceServiceResults parse(HttpResult response) throws IOException {
        try (
            XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, response.body())
        ) {
            var map = jsonParser.map();

            return transform(map);
        }
    }

    protected abstract T transform(Map<String, Object> extractedField);

    static List<?> validateList(Object obj) {
        validateNonNull(obj);

        if (obj instanceof List<?> == false) {
            throw new IllegalArgumentException(
                Strings.format("Extracted field was an invalid type, expected a list but received [%s]", obj.getClass().getSimpleName())
            );
        }

        return (List<?>) obj;
    }

    static void validateNonNull(Object obj) {
        Objects.requireNonNull(obj, "Failed to parse response, extracted field was null");
    }

    static List<Float> convertToListOfFloats(List<?> items) {
        return validateAndCastList(items, BaseCustomResponseParser::toFloat);
    }

    static Float toFloat(Object obj) {
        if (obj instanceof Number == false) {
            throw new IllegalArgumentException(Strings.format("Unable to convert type [%s] to Float", obj.getClass().getSimpleName()));
        }

        return ((Number) obj).floatValue();
    }

    static <ReturnType> List<ReturnType> validateAndCastList(List<?> items, Function<Object, ReturnType> converter) {
        validateNonNull(items);

        List<ReturnType> resultList = new ArrayList<>();
        for (var obj : items) {
            resultList.add(converter.apply(obj));
        }

        return resultList;
    }
}
