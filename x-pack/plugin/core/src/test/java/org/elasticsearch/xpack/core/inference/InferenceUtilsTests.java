/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.inference.InferenceUtils.extractOptionalEnum;
import static org.elasticsearch.xpack.core.inference.InferenceUtils.extractOptionalList;
import static org.elasticsearch.xpack.core.inference.InferenceUtils.extractOptionalString;
import static org.elasticsearch.xpack.core.inference.InferenceUtils.extractRequiredPositiveInteger;
import static org.elasticsearch.xpack.core.inference.InferenceUtils.extractRequiredPositiveIntegerGreaterThanOrEqualToMin;
import static org.elasticsearch.xpack.core.inference.InferenceUtils.extractRequiredPositiveIntegerLessThanOrEqualToMax;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class InferenceUtilsTests extends ESTestCase {

    public void testRemoveAsType_Validation_WithTheCorrectType() {
        Map<String, Object> map = new HashMap<>(Map.of("a", 5, "b", "a string", "c", Boolean.TRUE, "d", 1.0));

        ValidationException validationException = new ValidationException();
        Integer i = InferenceUtils.removeAsType(map, "a", Integer.class, validationException);
        assertEquals(Integer.valueOf(5), i);
        assertNull(map.get("a")); // field has been removed
        assertThat(validationException.validationErrors(), empty());

        String str = InferenceUtils.removeAsType(map, "b", String.class, validationException);
        assertEquals("a string", str);
        assertNull(map.get("b"));
        assertThat(validationException.validationErrors(), empty());
    }

    public void testRemoveAsType_Validation_WithInCorrectType() {
        Map<String, Object> map = new HashMap<>(Map.of("a", 5, "b", "a string", "c", Boolean.TRUE, "d", 5.0, "e", 5));

        var validationException = new ValidationException();
        Object result = InferenceUtils.removeAsType(map, "a", String.class, validationException);
        assertNull(result);
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(
            validationException.validationErrors().get(0),
            containsString("field [a] is not of the expected type. The value [5] cannot be converted to a [String]")
        );
        assertNull(map.get("a"));

        validationException = new ValidationException();
        InferenceUtils.removeAsType(map, "b", Boolean.class, validationException);
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(
            validationException.validationErrors().get(0),
            containsString("field [b] is not of the expected type. The value [a string] cannot be converted to a [Boolean]")
        );
        assertNull(map.get("b"));

        validationException = new ValidationException();
        result = InferenceUtils.removeAsType(map, "c", Integer.class, validationException);
        assertNull(result);
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(
            validationException.validationErrors().get(0),
            containsString("field [c] is not of the expected type. The value [true] cannot be converted to a [Integer]")
        );
        assertNull(map.get("c"));

        // cannot convert double to integer
        validationException = new ValidationException();
        result = InferenceUtils.removeAsType(map, "d", Integer.class, validationException);
        assertNull(result);
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(
            validationException.validationErrors().get(0),
            containsString("field [d] is not of the expected type. The value [5.0] cannot be converted to a [Integer]")
        );
        assertNull(map.get("d"));

        // cannot convert integer to double
        validationException = new ValidationException();
        result = InferenceUtils.removeAsType(map, "e", Double.class, validationException);
        assertNull(result);
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(
            validationException.validationErrors().get(0),
            containsString("field [e] is not of the expected type. The value [5] cannot be converted to a [Double]")
        );
        assertNull(map.get("e"));

        assertThat(map.entrySet(), empty());
    }

    public void testExtractOptionalString_CreatesString() {
        var validation = new ValidationException();
        Map<String, Object> map = modifiableMap(Map.of("key", "value"));
        var createdString = extractOptionalString(map, "key", "scope", validation);

        assertTrue(validation.validationErrors().isEmpty());
        assertNotNull(createdString);
        assertThat(createdString, is("value"));
        assertTrue(map.isEmpty());
    }

    public void testExtractOptionalString_DoesNotAddException_WhenFieldDoesNotExist() {
        var validation = new ValidationException();
        validation.addValidationError("previous error");
        Map<String, Object> map = modifiableMap(Map.of("key", "value"));
        var createdString = extractOptionalString(map, "abc", "scope", validation);

        assertNull(createdString);
        assertThat(validation.validationErrors(), hasSize(1));
        assertThat(map.size(), is(1));
    }

    public void testExtractOptionalString_AddsException_WhenFieldIsEmpty() {
        var validation = new ValidationException();
        Map<String, Object> map = modifiableMap(Map.of("key", ""));
        var createdString = extractOptionalString(map, "key", "scope", validation);

        assertNull(createdString);
        assertFalse(validation.validationErrors().isEmpty());
        assertTrue(map.isEmpty());
        assertThat(validation.validationErrors().get(0), is("[scope] Invalid value empty string. [key] must be a non-empty string"));
    }

    public void testExtractRequiredPositiveInteger_ReturnsValue() {
        var validation = new ValidationException();
        validation.addValidationError("previous error");
        Map<String, Object> map = modifiableMap(Map.of("key", 1));
        var parsedInt = extractRequiredPositiveInteger(map, "key", "scope", validation);

        assertThat(validation.validationErrors(), hasSize(1));
        assertNotNull(parsedInt);
        assertThat(parsedInt, is(1));
        assertTrue(map.isEmpty());
    }

    public void testExtractRequiredPositiveInteger_AddsErrorForNegativeValue() {
        var validation = new ValidationException();
        validation.addValidationError("previous error");
        Map<String, Object> map = modifiableMap(Map.of("key", -1));
        var parsedInt = extractRequiredPositiveInteger(map, "key", "scope", validation);

        assertThat(validation.validationErrors(), hasSize(2));
        assertNull(parsedInt);
        assertTrue(map.isEmpty());
        assertThat(validation.validationErrors().get(1), is("[scope] Invalid value [-1]. [key] must be a positive integer"));
    }

    public void testExtractRequiredPositiveInteger_AddsErrorWhenKeyIsMissing() {
        var validation = new ValidationException();
        validation.addValidationError("previous error");
        Map<String, Object> map = modifiableMap(Map.of("key", -1));
        var parsedInt = extractRequiredPositiveInteger(map, "not_key", "scope", validation);

        assertThat(validation.validationErrors(), hasSize(2));
        assertNull(parsedInt);
        assertThat(validation.validationErrors().get(1), is("[scope] does not contain the required setting [not_key]"));
    }

    public void testExtractRequiredPositiveIntegerLessThanOrEqualToMax_ReturnsValueWhenValueIsLessThanMax() {
        var validation = new ValidationException();
        validation.addValidationError("previous error");
        Map<String, Object> map = modifiableMap(Map.of("key", 1));
        var parsedInt = extractRequiredPositiveIntegerLessThanOrEqualToMax(map, "key", 5, "scope", validation);

        assertThat(validation.validationErrors(), hasSize(1));
        assertNotNull(parsedInt);
        assertThat(parsedInt, is(1));
        assertTrue(map.isEmpty());
    }

    public void testExtractRequiredPositiveIntegerLessThanOrEqualToMax_ReturnsValueWhenValueIsEqualToMax() {
        var validation = new ValidationException();
        validation.addValidationError("previous error");
        Map<String, Object> map = modifiableMap(Map.of("key", 5));
        var parsedInt = extractRequiredPositiveIntegerLessThanOrEqualToMax(map, "key", 5, "scope", validation);

        assertThat(validation.validationErrors(), hasSize(1));
        assertNotNull(parsedInt);
        assertThat(parsedInt, is(5));
        assertTrue(map.isEmpty());
    }

    public void testExtractRequiredPositiveIntegerLessThanOrEqualToMax_AddsErrorForNegativeValue() {
        var validation = new ValidationException();
        validation.addValidationError("previous error");
        Map<String, Object> map = modifiableMap(Map.of("key", -1));
        var parsedInt = extractRequiredPositiveIntegerLessThanOrEqualToMax(map, "key", 5, "scope", validation);

        assertThat(validation.validationErrors(), hasSize(2));
        assertNull(parsedInt);
        assertTrue(map.isEmpty());
        assertThat(validation.validationErrors().get(1), is("[scope] Invalid value [-1]. [key] must be a positive integer"));
    }

    public void testExtractRequiredPositiveIntegerLessThanOrEqualToMax_AddsErrorWhenKeyIsMissing() {
        var validation = new ValidationException();
        validation.addValidationError("previous error");
        Map<String, Object> map = modifiableMap(Map.of("key", -1));
        var parsedInt = extractRequiredPositiveIntegerLessThanOrEqualToMax(map, "not_key", 5, "scope", validation);

        assertThat(validation.validationErrors(), hasSize(2));
        assertNull(parsedInt);
        assertThat(validation.validationErrors().get(1), is("[scope] does not contain the required setting [not_key]"));
    }

    public void testExtractRequiredPositiveIntegerLessThanOrEqualToMax_AddsErrorWhenValueIsGreaterThanMax() {
        var validation = new ValidationException();
        validation.addValidationError("previous error");
        Map<String, Object> map = modifiableMap(Map.of("key", 6));
        var parsedInt = extractRequiredPositiveIntegerLessThanOrEqualToMax(map, "not_key", 5, "scope", validation);

        assertThat(validation.validationErrors(), hasSize(2));
        assertNull(parsedInt);
        assertThat(validation.validationErrors().get(1), is("[scope] does not contain the required setting [not_key]"));
    }

    public void testExtractRequiredPositiveIntegerGreaterThanOrEqualToMin_ReturnsValueWhenValueIsEqualToMin() {
        testExtractRequiredPositiveIntegerGreaterThanOrEqualToMin_Successful(5, 5);
    }

    public void testExtractRequiredPositiveIntegerGreaterThanOrEqualToMin_ReturnsValueWhenValueIsGreaterThanToMin() {
        testExtractRequiredPositiveIntegerGreaterThanOrEqualToMin_Successful(5, 6);
    }

    private void testExtractRequiredPositiveIntegerGreaterThanOrEqualToMin_Successful(int minValue, int actualValue) {
        var validation = new ValidationException();
        validation.addValidationError("previous error");
        Map<String, Object> map = modifiableMap(Map.of("key", actualValue));
        var parsedInt = extractRequiredPositiveIntegerGreaterThanOrEqualToMin(map, "key", minValue, "scope", validation);

        assertThat(validation.validationErrors(), hasSize(1));
        assertNotNull(parsedInt);
        assertThat(parsedInt, is(actualValue));
        assertTrue(map.isEmpty());
    }

    public void testExtractRequiredPositiveIntegerGreaterThanOrEqualToMin_AddsErrorWhenValueIsLessThanMin() {
        testExtractRequiredPositiveIntegerGreaterThanOrEqualToMin_AddsError(
            "key",
            5,
            4,
            "[scope] Invalid value [4.0]. [key] must be a greater than or equal to [5.0]"
        );
    }

    public void testExtractRequiredPositiveIntegerGreaterThanOrEqualToMin_AddsErrorWhenKeyIsMissing() {
        testExtractRequiredPositiveIntegerGreaterThanOrEqualToMin_AddsError(
            "not_key",
            5,
            -1,
            "[scope] does not contain the required setting [not_key]"
        );
    }

    public void testExtractRequiredPositiveIntegerGreaterThanOrEqualToMin_AddsErrorOnNegativeValue() {
        testExtractRequiredPositiveIntegerGreaterThanOrEqualToMin_AddsError(
            "key",
            5,
            -1,
            "[scope] Invalid value [-1]. [key] must be a positive integer"
        );
    }

    private void testExtractRequiredPositiveIntegerGreaterThanOrEqualToMin_AddsError(
        String key,
        int minValue,
        int actualValue,
        String error
    ) {
        var validation = new ValidationException();
        validation.addValidationError("previous error");
        Map<String, Object> map = modifiableMap(Map.of("key", actualValue));
        var parsedInt = extractRequiredPositiveIntegerGreaterThanOrEqualToMin(map, key, minValue, "scope", validation);

        assertThat(validation.validationErrors(), hasSize(2));
        assertNull(parsedInt);
        assertThat(validation.validationErrors().get(1), containsString(error));
    }

    public void testExtractOptionalList_CreatesList() {
        var validation = new ValidationException();
        var list = List.of(randomAlphaOfLength(10), randomAlphaOfLength(10));

        Map<String, Object> map = modifiableMap(Map.of("key", list));
        assertEquals(list, extractOptionalList(map, "key", String.class, validation));
        assertTrue(validation.validationErrors().isEmpty());
        assertTrue(map.isEmpty());
    }

    public void testExtractOptionalList_AddsException_WhenFieldDoesNotExist() {
        var validation = new ValidationException();
        validation.addValidationError("previous error");
        Map<String, Object> map = modifiableMap(Map.of("key", List.of(randomAlphaOfLength(10), randomAlphaOfLength(10))));
        assertNull(extractOptionalList(map, "abc", String.class, validation));
        assertThat(validation.validationErrors(), hasSize(1));
        assertThat(map.size(), is(1));
    }

    public void testExtractOptionalList_AddsException_WhenFieldIsEmpty() {
        var validation = new ValidationException();
        validation.addValidationError("previous error");
        Map<String, Object> map = modifiableMap(Map.of("key", ""));
        assertNull(extractOptionalList(map, "key", String.class, validation));
        assertFalse(validation.validationErrors().isEmpty());
        assertTrue(map.isEmpty());
    }

    public void testExtractOptionalList_AddsException_WhenFieldIsNotAList() {
        var validation = new ValidationException();
        validation.addValidationError("previous error");
        Map<String, Object> map = modifiableMap(Map.of("key", 1));
        assertNull(extractOptionalList(map, "key", String.class, validation));
        assertFalse(validation.validationErrors().isEmpty());
        assertTrue(map.isEmpty());
    }

    public void testExtractOptionalList_AddsException_WhenFieldIsNotAListOfTheCorrectType() {
        var validation = new ValidationException();
        validation.addValidationError("previous error");
        Map<String, Object> map = modifiableMap(Map.of("key", List.of(1, 2)));
        assertNull(extractOptionalList(map, "key", String.class, validation));
        assertFalse(validation.validationErrors().isEmpty());
        assertTrue(map.isEmpty());
    }

    public void testExtractOptionalList_AddsException_WhenFieldContainsMixedTypeValues() {
        var validation = new ValidationException();
        validation.addValidationError("previous error");
        Map<String, Object> map = modifiableMap(Map.of("key", List.of(1, "a")));
        assertNull(extractOptionalList(map, "key", String.class, validation));
        assertFalse(validation.validationErrors().isEmpty());
        assertTrue(map.isEmpty());
    }

    public void testExtractOptionalEnum_ReturnsNull_WhenFieldDoesNotExist() {
        var validation = new ValidationException();
        Map<String, Object> map = modifiableMap(Map.of("key", "value"));
        var createdEnum = extractOptionalEnum(map, "abc", "scope", InputType::fromString, EnumSet.allOf(InputType.class), validation);

        assertNull(createdEnum);
        assertTrue(validation.validationErrors().isEmpty());
        assertThat(map.size(), is(1));
    }

    public void testExtractOptionalEnum_ReturnsNullAndAddsException_WhenAnInvalidValueExists() {
        var validation = new ValidationException();
        Map<String, Object> map = modifiableMap(Map.of("key", "invalid_value"));
        var createdEnum = extractOptionalEnum(
            map,
            "key",
            "scope",
            InputType::fromString,
            EnumSet.of(InputType.INGEST, InputType.SEARCH),
            validation
        );

        assertNull(createdEnum);
        assertFalse(validation.validationErrors().isEmpty());
        assertTrue(map.isEmpty());
        assertThat(
            validation.validationErrors().get(0),
            is("[scope] Invalid value [invalid_value] received. [key] must be one of [ingest, search]")
        );
    }

    public void testExtractOptionalEnum_ReturnsNullAndAddsException_WhenValueIsNotPartOfTheAcceptableValues() {
        var validation = new ValidationException();
        Map<String, Object> map = modifiableMap(Map.of("key", InputType.UNSPECIFIED.toString()));
        var createdEnum = extractOptionalEnum(map, "key", "scope", InputType::fromString, EnumSet.of(InputType.INGEST), validation);

        assertNull(createdEnum);
        assertFalse(validation.validationErrors().isEmpty());
        assertTrue(map.isEmpty());
        assertThat(validation.validationErrors().get(0), is("[scope] Invalid value [unspecified] received. [key] must be one of [ingest]"));
    }

    public void testExtractOptionalEnum_ReturnsIngest_WhenValueIsAcceptable() {
        var validation = new ValidationException();
        Map<String, Object> map = modifiableMap(Map.of("key", InputType.INGEST.toString()));
        var createdEnum = extractOptionalEnum(map, "key", "scope", InputType::fromString, EnumSet.of(InputType.INGEST), validation);

        assertThat(createdEnum, is(InputType.INGEST));
        assertTrue(validation.validationErrors().isEmpty());
        assertTrue(map.isEmpty());
    }

    public void testExtractOptionalEnum_ReturnsClassification_WhenValueIsAcceptable() {
        var validation = new ValidationException();
        Map<String, Object> map = modifiableMap(Map.of("key", InputType.CLASSIFICATION.toString()));
        var createdEnum = extractOptionalEnum(
            map,
            "key",
            "scope",
            InputType::fromString,
            EnumSet.of(InputType.INGEST, InputType.CLASSIFICATION),
            validation
        );

        assertThat(createdEnum, is(InputType.CLASSIFICATION));
        assertTrue(validation.validationErrors().isEmpty());
        assertTrue(map.isEmpty());
    }

    public static <K, V> Map<K, V> modifiableMap(Map<K, V> aMap) {
        return new HashMap<>(aMap);
    }

}
