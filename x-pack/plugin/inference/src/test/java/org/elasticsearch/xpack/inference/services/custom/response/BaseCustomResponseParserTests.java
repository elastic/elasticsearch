/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.response;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.inference.services.custom.response.BaseCustomResponseParser.convertToListOfFloats;
import static org.elasticsearch.xpack.inference.services.custom.response.BaseCustomResponseParser.validateList;
import static org.hamcrest.Matchers.is;

public class BaseCustomResponseParserTests extends ESTestCase {
    public void testValidateNonNull_ThrowsException_WhenPassedNull() {
        var exception = expectThrows(NullPointerException.class, () -> BaseCustomResponseParser.validateNonNull(null));
        assertThat(exception.getMessage(), is("Failed to parse response, extracted field was null"));
    }

    public void testValidateList_ThrowsException_WhenPassedAnObjectThatIsNotAList() {
        var exception = expectThrows(IllegalArgumentException.class, () -> validateList(new Object()));
        assertThat(exception.getMessage(), is("Extracted field was an invalid type, expected a list but received [Object]"));
    }

    public void testValidateList_ReturnsList() {
        Object obj = List.of("abc", "123");
        assertThat(validateList(obj), is(List.of("abc", "123")));
    }

    public void testValidateAndCastList_ThrowsException_WhenAnItemInTheListIsNotANumber() {
        var list = List.of(1, "hello");

        var exception = expectThrows(IllegalArgumentException.class, () -> convertToListOfFloats(list));
        assertThat(exception.getMessage(), is("Unable to convert type [String] to Float"));
    }

    public void testValidateAndCastList_ReturnsList() {
        var obj = List.of(1, 1.1f, -2.0d, new AtomicInteger(1));

        assertThat(convertToListOfFloats(obj), is(List.of(1f, 1.1f, -2f, 1f)));
    }
}
