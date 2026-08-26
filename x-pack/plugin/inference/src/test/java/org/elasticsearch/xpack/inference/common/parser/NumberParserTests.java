/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.parser;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;

import static org.elasticsearch.xpack.inference.common.parser.ObjectParserUtils.pathToKey;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class NumberParserTests extends ESTestCase {

    private static final String ROOT = "root";
    private static final String VERSION = "version";
    private static final String NOT_A_NUMBER = "not_a_number";

    public void testExtractLong_HandlesInteger() {
        var map = new HashMap<String, Object>();
        map.put(VERSION, 1);

        var result = NumberParser.extractLong(map, VERSION, ROOT);

        assertThat(result, equalTo(1L));
    }

    public void testExtractLong_HandlesLong() {
        var version = 1000L;
        var map = new HashMap<String, Object>();
        map.put(VERSION, version);

        var result = NumberParser.extractLong(map, VERSION, ROOT);

        assertThat(result, equalTo(version));
    }

    public void testExtractLong_ReturnsNull_WhenKeyMissing() {
        var map = new HashMap<String, Object>();

        var result = NumberParser.extractLong(map, VERSION, ROOT);

        assertThat(result, nullValue());
    }

    public void testExtractLong_ReturnsNull_WhenValueNull() {
        var map = new HashMap<String, Object>();
        map.put(VERSION, null);

        var result = NumberParser.extractLong(map, VERSION, ROOT);

        assertThat(result, nullValue());
    }

    public void testExtractLong_Throws_WhenWrongType() {
        var map = new HashMap<String, Object>();
        map.put(VERSION, NOT_A_NUMBER);

        var e = expectThrows(IllegalArgumentException.class, () -> NumberParser.extractLong(map, VERSION, ROOT));
        assertThat(e.getMessage(), containsString(pathToKey(ROOT, VERSION)));
        assertThat(e.getMessage(), containsString(NOT_A_NUMBER));
    }

    public void testValidatePositiveInteger_NullValue_DoesNotThrow() {
        NumberParser.validatePositiveInteger(null, DIMENSIONS);
    }

    public void testValidatePositiveInteger_PositiveValue_DoesNotThrow() {
        NumberParser.validatePositiveInteger(randomIntBetween(1, 1000), DIMENSIONS);
    }

    public void testValidatePositiveInteger_ZeroValue_ThrowsException() {
        assertValidatePositiveInteger_InvalidValue_ThrowsException(0);
    }

    public void testValidatePositiveInteger_NegativeValue_ThrowsException() {
        assertValidatePositiveInteger_InvalidValue_ThrowsException(randomNegativeInt());
    }

    private static void assertValidatePositiveInteger_InvalidValue_ThrowsException(int invalidValue) {
        var e = expectThrows(IllegalArgumentException.class, () -> NumberParser.validatePositiveInteger(invalidValue, DIMENSIONS));
        assertThat(
            e.getMessage(),
            equalTo(
                Strings.format(
                    "[%s] Invalid value [%d]. [%s] must be a positive integer",
                    ModelConfigurations.SERVICE_SETTINGS,
                    invalidValue,
                    DIMENSIONS
                )
            )
        );
    }
}
