/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DataSourceValidationUtilsTests extends ESTestCase {

    private enum Color {
        RED,
        GREEN,
        BLUE
    }

    // ----- rejectUnknownFields -----

    public void testRejectUnknownFieldsAllKnown() {
        ValidationException errors = new ValidationException();
        DataSourceValidationUtils.rejectUnknownFields(Map.of("a", 1, "b", "x"), Set.of("a", "b"), errors);
        assertTrue(errors.validationErrors().isEmpty());
    }

    public void testRejectUnknownFieldsOneUnknown() {
        ValidationException errors = new ValidationException();
        DataSourceValidationUtils.rejectUnknownFields(Map.of("a", 1, "bogus", "x"), Set.of("a", "b"), errors);
        assertEquals(1, errors.validationErrors().size());
        assertTrue(errors.validationErrors().get(0).contains("[bogus]"));
    }

    public void testRejectUnknownFieldsMultipleUnknown() {
        ValidationException errors = new ValidationException();
        DataSourceValidationUtils.rejectUnknownFields(Map.of("a", 1, "x", 2, "y", 3), Set.of("a"), errors);
        assertEquals(2, errors.validationErrors().size());
    }

    public void testRejectUnknownFieldsEmptySettings() {
        ValidationException errors = new ValidationException();
        DataSourceValidationUtils.rejectUnknownFields(Map.of(), Set.of("a", "b"), errors);
        assertTrue(errors.validationErrors().isEmpty());
    }

    public void testRejectUnknownFieldsEmptyKnown() {
        ValidationException errors = new ValidationException();
        DataSourceValidationUtils.rejectUnknownFields(Map.of("a", 1), Set.of(), errors);
        assertEquals(1, errors.validationErrors().size());
    }

    // ----- validateEnum -----

    public void testValidateEnumValid() {
        ValidationException errors = new ValidationException();
        Map<String, Object> result = new HashMap<>();
        DataSourceValidationUtils.validateEnum(Map.of("color", "RED"), result, "color", Color.values(), Color::valueOf, errors);
        assertTrue(errors.validationErrors().isEmpty());
        assertEquals("RED", result.get("color"));
    }

    public void testValidateEnumMissing() {
        ValidationException errors = new ValidationException();
        Map<String, Object> result = new HashMap<>();
        DataSourceValidationUtils.validateEnum(Map.of(), result, "color", Color.values(), Color::valueOf, errors);
        assertTrue(errors.validationErrors().isEmpty());
        assertFalse(result.containsKey("color"));
    }

    public void testValidateEnumInvalid() {
        ValidationException errors = new ValidationException();
        Map<String, Object> result = new HashMap<>();
        DataSourceValidationUtils.validateEnum(Map.of("color", "PURPLE"), result, "color", Color.values(), Color::valueOf, errors);
        assertEquals(1, errors.validationErrors().size());
        assertTrue(errors.validationErrors().get(0).contains("[color]"));
        assertTrue(errors.validationErrors().get(0).contains("PURPLE"));
        assertFalse(result.containsKey("color"));
    }

    public void testValidateEnumParserReturnsNull() {
        ValidationException errors = new ValidationException();
        Map<String, Object> result = new HashMap<>();
        // A parser that "succeeds" by returning null should be treated as invalid.
        DataSourceValidationUtils.validateEnum(Map.of("color", "anything"), result, "color", Color.values(), s -> null, errors);
        assertEquals(1, errors.validationErrors().size());
        assertFalse(result.containsKey("color"));
    }

    // ----- validateInt -----

    public void testValidateIntValid() {
        ValidationException errors = new ValidationException();
        Map<String, Object> result = new HashMap<>();
        DataSourceValidationUtils.validateInt(Map.of("size", 50), result, "size", 1, 100, errors);
        assertTrue(errors.validationErrors().isEmpty());
        assertEquals(50, result.get("size"));
    }

    public void testValidateIntFromString() {
        // Values come from JSON parsing — strings should still parse as ints.
        ValidationException errors = new ValidationException();
        Map<String, Object> result = new HashMap<>();
        DataSourceValidationUtils.validateInt(Map.of("size", "50"), result, "size", 1, 100, errors);
        assertTrue(errors.validationErrors().isEmpty());
        assertEquals(50, result.get("size"));
    }

    public void testValidateIntMissing() {
        ValidationException errors = new ValidationException();
        Map<String, Object> result = new HashMap<>();
        DataSourceValidationUtils.validateInt(Map.of(), result, "size", 1, 100, errors);
        assertTrue(errors.validationErrors().isEmpty());
        assertFalse(result.containsKey("size"));
    }

    public void testValidateIntBoundaries() {
        // Both min and max are inclusive — neither should be rejected.
        ValidationException minErrors = new ValidationException();
        Map<String, Object> minResult = new HashMap<>();
        DataSourceValidationUtils.validateInt(Map.of("size", 1), minResult, "size", 1, 100, minErrors);
        assertTrue(minErrors.validationErrors().isEmpty());
        assertEquals(1, minResult.get("size"));

        ValidationException maxErrors = new ValidationException();
        Map<String, Object> maxResult = new HashMap<>();
        DataSourceValidationUtils.validateInt(Map.of("size", 100), maxResult, "size", 1, 100, maxErrors);
        assertTrue(maxErrors.validationErrors().isEmpty());
        assertEquals(100, maxResult.get("size"));
    }

    public void testValidateIntBelowMin() {
        ValidationException errors = new ValidationException();
        Map<String, Object> result = new HashMap<>();
        DataSourceValidationUtils.validateInt(Map.of("size", 0), result, "size", 1, 100, errors);
        assertEquals(1, errors.validationErrors().size());
        assertFalse(result.containsKey("size"));
    }

    public void testValidateIntAboveMax() {
        ValidationException errors = new ValidationException();
        Map<String, Object> result = new HashMap<>();
        DataSourceValidationUtils.validateInt(Map.of("size", 101), result, "size", 1, 100, errors);
        assertEquals(1, errors.validationErrors().size());
        assertFalse(result.containsKey("size"));
    }

    public void testValidateIntNonNumeric() {
        ValidationException errors = new ValidationException();
        Map<String, Object> result = new HashMap<>();
        DataSourceValidationUtils.validateInt(Map.of("size", "not a number"), result, "size", 1, 100, errors);
        assertEquals(1, errors.validationErrors().size());
        assertFalse(result.containsKey("size"));
    }
}
