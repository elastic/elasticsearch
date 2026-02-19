/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.parser;

import org.elasticsearch.xpack.core.inference.InferenceUtils;

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

    private EnumParser() {}
}
