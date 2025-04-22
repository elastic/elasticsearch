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

import static org.elasticsearch.xpack.inference.services.custom.response.BaseCustomResponseParser.convertToListOfFloats;
import static org.elasticsearch.xpack.inference.services.custom.response.BaseCustomResponseParser.toFloat;
import static org.elasticsearch.xpack.inference.services.custom.response.BaseCustomResponseParser.toType;
import static org.elasticsearch.xpack.inference.services.custom.response.BaseCustomResponseParser.validateAndCastList;
import static org.elasticsearch.xpack.inference.services.custom.response.BaseCustomResponseParser.validateList;
import static org.elasticsearch.xpack.inference.services.custom.response.BaseCustomResponseParser.validateMap;
import static org.hamcrest.Matchers.is;

public class BaseCustomResponseParserTests extends ESTestCase {
    public void testValidateNonNull_ThrowsException_WhenPassedNull() {
        var exception = expectThrows(NullPointerException.class, () -> BaseCustomResponseParser.validateNonNull(null));
        assertThat(exception.getMessage(), is("Failed to parse response, extracted field was null"));
    }

    public void testValidateList_ThrowsException_WhenPassedAnObjectThatIsNotAList() {
        var exception = expectThrows(IllegalArgumentException.class, () -> validateList(new Object()));
        assertThat(exception.getMessage(), is("Extracted field is an invalid type, expected a list but received [Object]"));
    }

    public void testValidateList_ReturnsList() {
        Object obj = List.of("abc", "123");
        assertThat(validateList(obj), is(List.of("abc", "123")));
    }

    public void testConvertToListOfFloats_ThrowsException_WhenAnItemInTheListIsNotANumber() {
        var list = List.of(1, "hello");

        var exception = expectThrows(IllegalArgumentException.class, () -> convertToListOfFloats(list));
        assertThat(exception.getMessage(), is("Unable to convert type [String] to Number"));
    }

    public void testConvertToListOfFloats_ReturnsList() {
        var list = List.of(1, 1.1f, -2.0d, new AtomicInteger(1));

        assertThat(convertToListOfFloats(list), is(List.of(1f, 1.1f, -2f, 1f)));
    }

    public void testValidateAndCastList() {
        var list = List.of("abc", "123", 1, 2.2d);

        assertThat(validateAndCastList(list, Object::toString), is(List.of("abc", "123", "1", "2.2")));
    }

    public void testValidateAndCastList_ThrowsException() {
        var list = List.of("abc");

        var exception = expectThrows(IllegalArgumentException.class, () -> validateAndCastList(list, (obj) -> {
            throw new IllegalArgumentException("failed");
        }));

        assertThat(exception.getMessage(), is("failed"));
    }

    public void testValidateMap() {
        assertThat(validateMap(Map.of("abc", 123)), is(Map.of("abc", 123)));
    }

    public void testValidateMap_ThrowsException_WhenObjectIsNotAMap() {
        var exception = expectThrows(IllegalArgumentException.class, () -> validateMap("hello"));
        assertThat(exception.getMessage(), is("Extracted field is an invalid type, expected a map but received [String]"));
    }

    public void testValidateMap_ThrowsException_WhenKeysAreNotStrings() {
        var exception = expectThrows(IllegalStateException.class, () -> validateMap(Map.of("key", "value", 1, "abc")));
        assertThat(exception.getMessage(), is("Extracted map has an invalid key type. Expected a string but received [Integer]"));
    }

    public void testToFloat() {
        assertThat(toFloat(1), is(1f));
    }

    public void testToFloat_AtomicLong() {
        assertThat(toFloat(new AtomicLong(100)), is(100f));
    }

    public void testToFloat_Double() {
        assertThat(toFloat(1.123d), is(1.123f));
    }

    public void testToType() {
        Object obj = "hello";
        assertThat(toType(obj, String.class), is("hello"));
    }

    public void testToType_List() {
        Object obj = List.of(123, 456);
        assertThat(toType(obj, List.class), is(List.of(123, 456)));
    }
}
