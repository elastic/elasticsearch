/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.MapXContentParser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class SemanticTextUtils {
    private static final String STRING_EXPECTED_TYPES = "String|Number|Boolean";
    private static final String OBJECT_EXPECTED_TYPES = STRING_EXPECTED_TYPES + "|Object";

    private SemanticTextUtils() {}

    /**
     * Normalizes a raw source value extracted from an inference field source field into a flat list of values, preserving order.
     *
     * <p>Each element in the returned list is one of:
     * <ul>
     *     <li>a {@link String} — for raw {@code String}, {@code Number}, or {@code Boolean} input values</li>
     *     <li>an {@link InferenceString} — for input values supplied as an object, when {@code parseInferenceStrings} is
     *         {@code true}</li>
     *     <li>a {@code Map<?, ?>} — for input values supplied as an object, when {@code parseInferenceStrings} is
     *         {@code false}</li>
     * </ul>
     *
     * <p>If {@code valueObj} is a {@link Collection}, every element is converted in iteration order. Any other {@code valueObj}
     * is converted as a single-element list.
     *
     * @param field                 the source field name
     * @param valueObj              the raw source field value
     * @param parseInferenceStrings if {@code true}, object values are parsed into {@link InferenceString}; if {@code false}, object
     *                              values are returned as {@code Map<?, ?>}
     * @return a flat list of values
     * @throws ElasticsearchStatusException if the raw source field value uses an invalid format
     */
    public static List<Object> nodeObjectValues(String field, Object valueObj, boolean parseInferenceStrings) {
        return nodeValues(valueObj, raw -> nodeObjectValue(field, raw, true, parseInferenceStrings));
    }

    /**
     * Normalizes a raw source value extracted from an inference field source field into a flat list of string inference inputs,
     * preserving order.
     *
     * <p>If {@code valueObj} is a {@link Collection}, every element is converted in iteration order. Any other {@code valueObj}
     * is converted as a single-element list.
     *
     * @param field    the source field name
     * @param valueObj the raw source field value
     * @return a list of string inference inputs
     * @throws ElasticsearchStatusException if the raw source field value uses an invalid format
     */
    public static List<String> nodeStringValues(String field, Object valueObj) {
        return nodeValues(valueObj, raw -> (String) nodeObjectValue(field, raw, false, false));
    }

    // TODO: Javadoc
    public static Object nodeObjectValue(String field, Object valueObj, boolean parseObjects, boolean parseInferenceStrings) {
        if (valueObj instanceof Number || valueObj instanceof Boolean) {
            return valueObj.toString();
        } else if (valueObj instanceof String value) {
            return value;
        } else if (parseObjects && valueObj instanceof Map<?, ?> map) {
            if (parseInferenceStrings) {
                return parseInferenceStringValue(field, map);
            }
            return map;
        } else {
            throw new ElasticsearchStatusException(
                "Invalid format for field [{}], expected [{}] got [{}]",
                RestStatus.BAD_REQUEST,
                field,
                parseObjects ? OBJECT_EXPECTED_TYPES : STRING_EXPECTED_TYPES,
                valueObj.getClass().getSimpleName()
            );
        }
    }

    private static <T> List<T> nodeValues(Object valueObj, Function<Object, T> parse) {
        if (valueObj instanceof Collection<?> values) {
            List<T> parsed = new ArrayList<>(values.size());
            for (var v : values) {
                parsed.add(parse.apply(v));
            }
            return parsed;
        }

        return List.of(parse.apply(valueObj));
    }

    private static InferenceString parseInferenceStringValue(String field, Map<?, ?> value) {
        @SuppressWarnings("unchecked")
        Map<String, Object> stringKeyedMap = (Map<String, Object>) value;

        InferenceString inferenceString;
        try (
            XContentParser parser = new MapXContentParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                stringKeyedMap,
                XContentType.JSON
            )
        ) {
            inferenceString = InferenceString.PARSER.parse(parser, null);
        } catch (Exception e) {
            throw new ElasticsearchStatusException("Invalid object value format for field [{}]", RestStatus.BAD_REQUEST, e, field);
        }

        if (inferenceString.isText()) {
            throw new ElasticsearchStatusException(
                "Invalid object value format for field [{}]. Objects for text values are not supported, use a string literal instead.",
                RestStatus.BAD_REQUEST,
                field
            );
        }

        return inferenceString;
    }
}
