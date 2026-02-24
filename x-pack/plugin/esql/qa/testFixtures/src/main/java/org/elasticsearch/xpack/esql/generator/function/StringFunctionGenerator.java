/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.function;

import org.elasticsearch.xpack.esql.generator.Column;

import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.needsQuoting;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.quote;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomStringField;
import static org.elasticsearch.xpack.esql.generator.command.pipe.KeepGenerator.randomUnmappedFieldName;
import static org.elasticsearch.xpack.esql.generator.function.FunctionGeneratorUtils.fieldOrUnmapped;
import static org.elasticsearch.xpack.esql.generator.function.FunctionGeneratorUtils.shouldAddUnmappedField;

/**
 * Generates random string function expressions.
 */
public final class StringFunctionGenerator {

    private StringFunctionGenerator() {}

    /**
     * Generates a string function that returns a string.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String stringFunction(List<Column> columns, boolean allowUnmapped) {
        String stringField = fieldOrUnmapped(randomStringField(columns), allowUnmapped);
        if (stringField == null) {
            return null;
        }
        return randomFrom(
            "to_lower(" + stringField + ")",
            "to_upper(" + stringField + ")",
            "trim(" + stringField + ")",
            "ltrim(" + stringField + ")",
            "rtrim(" + stringField + ")",
            "reverse(" + stringField + ")",
            "left(" + stringField + ", " + randomIntBetween(1, 10) + ")",
            "right(" + stringField + ", " + randomIntBetween(1, 10) + ")",
            "substring(" + stringField + ", " + randomIntBetween(0, 5) + ", " + randomIntBetween(1, 10) + ")",
            "repeat(" + stringField + ", " + randomIntBetween(1, 3) + ")",
            "space(" + randomIntBetween(0, 10) + ")",
            "replace(" + stringField + ", \"a\", \"b\")",
            "md5(" + stringField + ")",
            "sha1(" + stringField + ")",
            "sha256(" + stringField + ")",
            "to_base64(" + stringField + ")",
            "from_base64(to_base64(" + stringField + "))",
            "url_encode(" + stringField + ")",
            "url_decode(" + stringField + ")"
        );
    }

    /**
     * Generates a string function that returns an integer (length-like functions).
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String stringToIntFunction(List<Column> columns, boolean allowUnmapped) {
        String stringField = fieldOrUnmapped(randomStringField(columns), allowUnmapped);
        if (stringField == null) {
            return null;
        }
        return randomFrom(
            "length(" + stringField + ")",
            "bit_length(" + stringField + ")",
            "byte_length(" + stringField + ")",
            "locate(" + stringField + ", \"a\")",
            "locate(" + stringField + ", \"a\", " + randomIntBetween(0, 5) + ")"
        );
    }

    /**
     * Generates a string function that returns a boolean.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String stringToBoolFunction(List<Column> columns, boolean allowUnmapped) {
        String stringField = fieldOrUnmapped(randomStringField(columns), allowUnmapped);
        if (stringField == null) {
            return null;
        }
        String searchStr = "\"" + randomAlphaOfLength(randomIntBetween(1, 3)) + "\"";
        return randomFrom(
            "starts_with(" + stringField + ", " + searchStr + ")",
            "ends_with(" + stringField + ", " + searchStr + ")",
            "contains(" + stringField + ", " + searchStr + ")"
        );
    }

    /**
     * Generates a concat function with multiple arguments.
     * May randomly include unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String concatFunction(List<Column> columns, boolean allowUnmapped) {
        List<String> stringFields = columns.stream()
            .filter(c -> c.type().equals("keyword") || c.type().equals("text"))
            .map(c -> needsQuoting(c.name()) ? quote(c.name()) : c.name())
            .limit(randomIntBetween(2, 4))
            .collect(Collectors.toList());
        if (stringFields.isEmpty()) {
            return null;
        }
        if (allowUnmapped && shouldAddUnmappedField()) {
            stringFields.add(randomUnmappedFieldName());
        }
        if (stringFields.size() < 2) {
            return null;
        }
        return "concat(" + String.join(", ", stringFields) + ")";
    }

    /**
     * Generates a split function.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String splitFunction(List<Column> columns, boolean allowUnmapped) {
        String stringField = fieldOrUnmapped(randomStringField(columns), allowUnmapped);
        if (stringField == null) {
            return null;
        }
        String delimiter = randomFrom(",", " ", "-", "_", ":");
        return "split(" + stringField + ", \"" + delimiter + "\")";
    }
}

