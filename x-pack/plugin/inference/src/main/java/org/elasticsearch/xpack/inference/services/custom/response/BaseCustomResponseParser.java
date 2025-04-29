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
import java.util.function.BiFunction;

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

    static List<?> validateList(Object obj, String fieldName) {
        validateNonNull(obj, fieldName);

        if (obj instanceof List<?> == false) {
            throw new IllegalArgumentException(
                Strings.format(
                    "Extracted field [%s] is an invalid type, expected a list but received [%s]",
                    fieldName,
                    obj.getClass().getSimpleName()
                )
            );
        }

        return (List<?>) obj;
    }

    static void validateNonNull(Object obj, String fieldName) {
        Objects.requireNonNull(obj, Strings.format("Failed to parse field [%s], extracted field was null", fieldName));
    }

    static Map<String, Object> validateMap(Object obj, String fieldName) {
        validateNonNull(obj, fieldName);

        if (obj instanceof Map<?, ?> == false) {
            throw new IllegalArgumentException(
                Strings.format(
                    "Extracted field [%s] is an invalid type, expected a map but received [%s]",
                    fieldName,
                    obj.getClass().getSimpleName()
                )
            );
        }

        var keys = ((Map<?, ?>) obj).keySet();
        for (var key : keys) {
            if (key instanceof String == false) {
                throw new IllegalStateException(
                    Strings.format(
                        "Extracted field [%s] map has an invalid key type. Expected a string but received [%s]",
                        fieldName,
                        key.getClass().getSimpleName()
                    )
                );
            }
        }

        @SuppressWarnings("unchecked")
        var result = (Map<String, Object>) obj;
        return result;
    }

    static List<Float> convertToListOfFloats(Object obj, String fieldName) {
        return castList(validateList(obj, fieldName), BaseCustomResponseParser::toFloat, fieldName);
    }

    static Float toFloat(Object obj, String fieldName) {
        return toNumber(obj, fieldName).floatValue();
    }

    private static Number toNumber(Object obj, String fieldName) {
        if (obj instanceof Number == false) {
            throw new IllegalArgumentException(
                Strings.format("Unable to convert field [%s] of type [%s] to Number", fieldName, obj.getClass().getSimpleName())
            );
        }

        return ((Number) obj);
    }

    static List<Integer> convertToListOfIntegers(Object obj, String fieldName) {
        return castList(validateList(obj, fieldName), BaseCustomResponseParser::toInteger, fieldName);
    }

    private static Integer toInteger(Object obj, String fieldName) {
        return toNumber(obj, fieldName).intValue();
    }

    static <T> List<T> castList(List<?> items, BiFunction<Object, String, T> converter, String fieldName) {
        validateNonNull(items, fieldName);

        List<T> resultList = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            try {
                resultList.add(converter.apply(items.get(i), fieldName));
            } catch (Exception e) {
                throw new IllegalStateException(Strings.format("Failed to parse list entry [%d], error: %s", i, e.getMessage()), e);
            }
        }

        return resultList;
    }

    static <T> T toType(Object obj, Class<T> type, String fieldName) {
        validateNonNull(obj, fieldName);

        if (type.isInstance(obj) == false) {
            throw new IllegalArgumentException(
                Strings.format(
                    "Unable to convert field [%s] of type [%s] to [%s]",
                    fieldName,
                    obj.getClass().getSimpleName(),
                    type.getSimpleName()
                )
            );
        }

        return type.cast(obj);
    }
}
