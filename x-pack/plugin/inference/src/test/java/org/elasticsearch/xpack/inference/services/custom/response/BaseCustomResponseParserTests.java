/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.response;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.xpack.inference.services.custom.response.BaseCustomResponseParser.castList;
import static org.elasticsearch.xpack.inference.services.custom.response.BaseCustomResponseParser.convertToListOfFloats;
import static org.elasticsearch.xpack.inference.services.custom.response.BaseCustomResponseParser.toFloat;
import static org.elasticsearch.xpack.inference.services.custom.response.BaseCustomResponseParser.toType;
import static org.elasticsearch.xpack.inference.services.custom.response.BaseCustomResponseParser.validateList;
import static org.elasticsearch.xpack.inference.services.custom.response.BaseCustomResponseParser.validateMap;
import static org.hamcrest.Matchers.is;

public class BaseCustomResponseParserTests extends ESTestCase {
    public void testValidateNonNull_ThrowsException_WhenPassedNull() {
        var exception = expectThrows(NullPointerException.class, () -> BaseCustomResponseParser.validateNonNull(null, "field"));
        assertThat(exception.getMessage(), is("Failed to parse field [field], extracted field was null"));
    }

    public void testValidateList_ThrowsException_WhenPassedAnObjectThatIsNotAList() {
        var exception = expectThrows(IllegalArgumentException.class, () -> validateList(new Object(), "field"));
        assertThat(exception.getMessage(), is("Extracted field [field] is an invalid type, expected a list but received [Object]"));
    }

    public void testValidateList_ReturnsList() {
        Object obj = List.of("abc", "123");
        assertThat(validateList(obj, "field"), is(List.of("abc", "123")));
    }

    public void testConvertToListOfFloats_ThrowsException_WhenAnItemInTheListIsNotANumber() {
        var list = List.of(1, "hello");

        var exception = expectThrows(IllegalStateException.class, () -> convertToListOfFloats(list, "field"));
        assertThat(
            exception.getMessage(),
            is("Failed to parse list entry [1], error: Unable to convert field [field] of type [String] to Number")
        );
    }

    public void testConvertToListOfFloats_ReturnsList() {
        var list = List.of(1, 1.1f, -2.0d, new AtomicInteger(1));

        assertThat(convertToListOfFloats(list, "field"), is(List.of(1f, 1.1f, -2f, 1f)));
    }

    public void testCastList() {
        var list = List.of("abc", "123", 1, 2.2d);

        assertThat(castList(list, (obj, fieldName) -> obj.toString(), "field"), is(List.of("abc", "123", "1", "2.2")));
    }

    public void testCastList_ThrowsException() {
        var list = List.of("abc");

        var exception = expectThrows(IllegalStateException.class, () -> castList(list, (obj, fieldName) -> {
            throw new IllegalArgumentException("failed");
        }, "field"));

        assertThat(exception.getMessage(), is("Failed to parse list entry [0], error: failed"));
    }

    public void testValidateMap() {
        assertThat(validateMap(Map.of("abc", 123), "field"), is(Map.of("abc", 123)));
    }

    public void testValidateMap_ThrowsException_WhenObjectIsNotAMap() {
        var exception = expectThrows(IllegalArgumentException.class, () -> validateMap("hello", "field"));
        assertThat(exception.getMessage(), is("Extracted field [field] is an invalid type, expected a map but received [String]"));
    }

    public void testValidateMap_ThrowsException_WhenKeysAreNotStrings() {
        var exception = expectThrows(IllegalStateException.class, () -> validateMap(Map.of("key", "value", 1, "abc"), "field"));
        assertThat(
            exception.getMessage(),
            is("Extracted field [field] map has an invalid key type. Expected a string but received [Integer]")
        );
    }

    public void testToFloat() {
        assertThat(toFloat(1, "field"), is(1f));
    }

    public void testToFloat_AtomicLong() {
        assertThat(toFloat(new AtomicLong(100), "field"), is(100f));
    }

    public void testToFloat_Double() {
        assertThat(toFloat(1.123d, "field"), is(1.123f));
    }

    public void testToType() {
        Object obj = "hello";
        assertThat(toType(obj, String.class, "field"), is("hello"));
    }

    public void testToType_List() {
        Object obj = List.of(123, 456);
        assertThat(toType(obj, List.class, "field"), is(List.of(123, 456)));
    }
}
