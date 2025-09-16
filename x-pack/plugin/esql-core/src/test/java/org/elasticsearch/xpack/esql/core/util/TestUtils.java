/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.util;

import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.AtomType;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.util.Locale;
import java.util.regex.Pattern;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.AtomType.INTEGER;

public final class TestUtils {
    private TestUtils() {}

    private static final Pattern WS_PATTERN = Pattern.compile("\\s");

    public static FieldAttribute fieldAttribute() {
        // NOCOMMIT should be randomDataType instead?
        return fieldAttribute(randomAlphaOfLength(10), DataType.atom(randomFrom(AtomType.types())));
    }

    public static FieldAttribute fieldAttribute(String name, DataType type) {
        return new FieldAttribute(EMPTY, name, new EsField(name, type, emptyMap(), randomBoolean(), EsField.TimeSeriesFieldType.NONE));
    }

    public static FieldAttribute getFieldAttribute(String name) {
        return getFieldAttribute(name, DataType.atom(INTEGER));
    }

    public static FieldAttribute getFieldAttribute(String name, DataType dataType) {
        return new FieldAttribute(EMPTY, name, new EsField(name + "f", dataType, emptyMap(), true, EsField.TimeSeriesFieldType.NONE));
    }

    /**
     * Similar to {@link String#strip()}, but removes the WS throughout the entire string.
     */
    public static String stripThrough(String input) {
        return WS_PATTERN.matcher(input).replaceAll(StringUtils.EMPTY);
    }

    /**
     * Returns the input string, but with parts of it having the letter casing changed.
     */
    public static String randomCasing(String input) {
        StringBuilder sb = new StringBuilder(input.length());
        for (int i = 0, inputLen = input.length(), step = (int) Math.sqrt(inputLen); i < inputLen; i += step) {
            var chunkEnd = Math.min(i + step, inputLen);
            var chunk = input.substring(i, chunkEnd);
            sb.append(randomBoolean() ? chunk.toLowerCase(Locale.ROOT) : chunk.toUpperCase(Locale.ROOT));
        }
        return sb.toString();
    }
}
