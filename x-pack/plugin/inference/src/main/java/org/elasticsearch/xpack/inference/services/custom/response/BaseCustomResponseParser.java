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
                Strings.format("Extracted field is an invalid type, expected a list but received [%s]", obj.getClass().getSimpleName())
            );
        }

        return (List<?>) obj;
    }

    static void validateNonNull(Object obj) {
        Objects.requireNonNull(obj, "Failed to parse response, extracted field was null");
    }

    static Map<String, Object> validateMap(Object obj) {
        validateNonNull(obj);

        if (obj instanceof Map<?, ?> == false) {
            throw new IllegalArgumentException(
                Strings.format("Extracted field is an invalid type, expected a map but received [%s]", obj.getClass().getSimpleName())
            );
        }

        var keys = ((Map<?, ?>) obj).keySet();
        for (var key : keys) {
            if (key instanceof String == false) {
                throw new IllegalStateException(
                    Strings.format(
                        "Extracted map has an invalid key type. Expected a string but received [%s]",
                        key.getClass().getSimpleName()
                    )
                );
            }
        }

        @SuppressWarnings("unchecked")
        var result = (Map<String, Object>) obj;
        return result;
    }

    static List<Float> convertToListOfFloats(Object obj) {
        return validateAndCastList(validateList(obj), BaseCustomResponseParser::toFloat);
    }

    static Float toFloat(Object obj) {
        return toNumber(obj).floatValue();
    }

    private static Number toNumber(Object obj) {
        if (obj instanceof Number == false) {
            throw new IllegalArgumentException(Strings.format("Unable to convert type [%s] to Number", obj.getClass().getSimpleName()));
        }

        return ((Number) obj);
    }

    static List<Integer> convertToListOfIntegers(Object obj) {
        return validateAndCastList(validateList(obj), BaseCustomResponseParser::toInteger);
    }

    private static Integer toInteger(Object obj) {
        return toNumber(obj).intValue();
    }

    static <T> List<T> validateAndCastList(List<?> items, Function<Object, T> converter) {
        validateNonNull(items);

        List<T> resultList = new ArrayList<>();
        for (var obj : items) {
            resultList.add(converter.apply(obj));
        }

        return resultList;
    }

    static <T> T toType(Object obj, Class<T> type) {
        validateNonNull(obj);

        if (type.isInstance(obj) == false) {
            throw new IllegalArgumentException(
                Strings.format("Unable to convert object of type [%s] to type [%s]", obj.getClass().getSimpleName(), type.getSimpleName())
            );
        }

        return type.cast(obj);
    }
}
