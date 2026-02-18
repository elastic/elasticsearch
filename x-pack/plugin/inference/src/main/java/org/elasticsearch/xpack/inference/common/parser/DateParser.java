/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.parser;

import org.elasticsearch.common.Strings;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.common.parser.ObjectParserUtils.pathToKey;

public final class DateParser {

    public static LocalDate parseLocalDate(Map<String, Object> sourceMap, String key, String root) {
        var dateString = ObjectParserUtils.removeAsType(sourceMap, key, root, String.class);
        return parseLocalDate(dateString, key, root);
    }

    public static LocalDate parseLocalDate(String dateString, String key, String root) {
        try {
            if (dateString == null) {
                return null;
            }
            return LocalDate.parse(dateString);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(
                Strings.format("Failed to parse date field [%s] with value [%s]", pathToKey(root, key), dateString),
                e
            );
        }
    }

    private DateParser() {}
}
