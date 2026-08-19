/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.parser;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xpack.core.inference.InferenceUtils;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.core.inference.InferenceUtils.validateEnumValue;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.invalidValue;

public final class EnumParser {
    public static <E extends Enum<E>> E extractEnum(
        Map<String, Object> map,
        String key,
        String root,
        InferenceUtils.EnumConstructor<E> constructor,
        EnumSet<E> validValues
    ) {
        var enumString = ObjectParserUtils.removeAsType(map, key, root, String.class);
        if (enumString == null) {
            return null;
        }

        try {
            var createdEnum = constructor.apply(enumString);
            validateEnumValue(createdEnum, validValues);

            return createdEnum;
        } catch (IllegalArgumentException e) {
            var validValuesAsStrings = validValues.stream().map(value -> value.toString().toLowerCase(Locale.ROOT)).toArray(String[]::new);
            throw new IllegalArgumentException(invalidValue(key, root, enumString, validValuesAsStrings));
        }
    }

    /**
     * Parses a string into an enum returning a user-friendly error message if the value is invalid.
     * Use this instead of {@link #extractEnum(Map, String, String, InferenceUtils.EnumConstructor, EnumSet)} when parsing
     * from an object parser context.
     *
     * @param value the enum value to parse
     * @param constructor the constructor to use to create the enum
     * @param validValues the valid values for the enum
     * @param excludeValuesFromErrorMessage the values to exclude from the error message
     * @return the parsed enum or null if the value is null
     * @param <E> the enum type
     */
    @Nullable
    public static <E extends Enum<E>> E parseFromStringInObjectParserContext(
        @Nullable String value,
        InferenceUtils.EnumConstructor<E> constructor,
        EnumSet<E> validValues,
        EnumSet<E> excludeValuesFromErrorMessage
    ) {
        if (value == null) {
            return null;
        }

        try {
            var createdEnum = constructor.apply(value);
            if (validValues.contains(createdEnum) == false) {
                throw new IllegalArgumentException();
            }
            return createdEnum;
        } catch (IllegalArgumentException e) {
            var validValuesAsStrings = validValues.stream()
                .filter(enumValue -> excludeValuesFromErrorMessage.contains(enumValue) == false)
                .map(v -> v.toString().toLowerCase(Locale.ROOT))
                .toArray(String[]::new);
            String msg = Strings.format("Invalid value [%s]; expected one of %s", value, Arrays.toString(validValuesAsStrings));
            throw new IllegalArgumentException(msg);
        }
    }

    /**
     * Converts a string into a {@link SimilarityMeasure}, intended to be used as the conversion function of an object parser's
     * {@code declareString}. Delegates to {@link #parseFromStringInObjectParserContext} so that an unrecognized value yields a
     * user-facing error listing the accepted values rather than the raw enum constant names.
     *
     * @param value the similarity value to parse
     * @return the parsed {@link SimilarityMeasure}, or {@code null} if the value is null
     */
    @Nullable
    public static SimilarityMeasure parseSimilarity(@Nullable String value) {
        return parseFromStringInObjectParserContext(
            value,
            SimilarityMeasure::fromString,
            EnumSet.allOf(SimilarityMeasure.class),
            EnumSet.noneOf(SimilarityMeasure.class)
        );
    }

    private EnumParser() {}
}
