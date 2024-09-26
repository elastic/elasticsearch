/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.InferenceTextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.InferenceTextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.inference.results.InferenceTextEmbeddingByteResultsTests;
import org.elasticsearch.xpack.inference.results.TextEmbeddingResultsTests;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.convertToUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveLong;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalTimeValue;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredPositiveIntegerLessThanOrEqualToMax;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredSecureString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.getEmbeddingSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ServiceUtilsTests extends ESTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    public void testRemoveAsTypeWithTheCorrectType() {
        Map<String, Object> map = new HashMap<>(Map.of("a", 5, "b", "a string", "c", Boolean.TRUE, "d", 1.0));

        Integer i = ServiceUtils.removeAsType(map, "a", Integer.class);
        assertEquals(Integer.valueOf(5), i);
        assertNull(map.get("a")); // field has been removed

        String str = ServiceUtils.removeAsType(map, "b", String.class);
        assertEquals("a string", str);
        assertNull(map.get("b"));

        Boolean b = ServiceUtils.removeAsType(map, "c", Boolean.class);
        assertEquals(Boolean.TRUE, b);
        assertNull(map.get("c"));

        Double d = ServiceUtils.removeAsType(map, "d", Double.class);
        assertEquals(Double.valueOf(1.0), d);
        assertNull(map.get("d"));

        assertThat(map.entrySet(), empty());
    }

    public void testRemoveAsType_Validation_WithTheCorrectType() {
        Map<String, Object> map = new HashMap<>(Map.of("a", 5, "b", "a string", "c", Boolean.TRUE, "d", 1.0));

        ValidationException validationException = new ValidationException();
        Integer i = ServiceUtils.removeAsType(map, "a", Integer.class, validationException);
        assertEquals(Integer.valueOf(5), i);
        assertNull(map.get("a")); // field has been removed
        assertThat(validationException.validationErrors(), empty());

        String str = ServiceUtils.removeAsType(map, "b", String.class, validationException);
        assertEquals("a string", str);
        assertNull(map.get("b"));
        assertThat(validationException.validationErrors(), empty());
    }

    public void testRemoveAsTypeWithInCorrectType() {
        Map<String, Object> map = new HashMap<>(Map.of("a", 5, "b", "a string", "c", Boolean.TRUE, "d", 5.0, "e", 5));

        var e = expectThrows(ElasticsearchStatusException.class, () -> ServiceUtils.removeAsType(map, "a", String.class));
        assertThat(
            e.getMessage(),
            containsString("field [a] is not of the expected type. The value [5] cannot be converted to a [String]")
        );
        assertNull(map.get("a"));

        e = expectThrows(ElasticsearchStatusException.class, () -> ServiceUtils.removeAsType(map, "b", Boolean.class));
        assertThat(
            e.getMessage(),
            containsString("field [b] is not of the expected type. The value [a string] cannot be converted to a [Boolean]")
        );
        assertNull(map.get("b"));

        e = expectThrows(ElasticsearchStatusException.class, () -> ServiceUtils.removeAsType(map, "c", Integer.class));
        assertThat(
            e.getMessage(),
            containsString("field [c] is not of the expected type. The value [true] cannot be converted to a [Integer]")
        );
        assertNull(map.get("c"));

        // cannot convert double to integer
        e = expectThrows(ElasticsearchStatusException.class, () -> ServiceUtils.removeAsType(map, "d", Integer.class));
        assertThat(
            e.getMessage(),
            containsString("field [d] is not of the expected type. The value [5.0] cannot be converted to a [Integer]")
        );
        assertNull(map.get("d"));

        // cannot convert integer to double
        e = expectThrows(ElasticsearchStatusException.class, () -> ServiceUtils.removeAsType(map, "e", Double.class));
        assertThat(
            e.getMessage(),
            containsString("field [e] is not of the expected type. The value [5] cannot be converted to a [Double]")
        );
        assertNull(map.get("e"));

        assertThat(map.entrySet(), empty());
    }

    public void testRemoveAsType_Validation_WithInCorrectType() {
        Map<String, Object> map = new HashMap<>(Map.of("a", 5, "b", "a string", "c", Boolean.TRUE, "d", 5.0, "e", 5));

        var validationException = new ValidationException();
        Object result = ServiceUtils.removeAsType(map, "a", String.class, validationException);
        assertNull(result);
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(
            validationException.validationErrors().get(0),
            containsString("field [a] is not of the expected type. The value [5] cannot be converted to a [String]")
        );
        assertNull(map.get("a"));

        validationException = new ValidationException();
        ServiceUtils.removeAsType(map, "b", Boolean.class, validationException);
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(
            validationException.validationErrors().get(0),
            containsString("field [b] is not of the expected type. The value [a string] cannot be converted to a [Boolean]")
        );
        assertNull(map.get("b"));

        validationException = new ValidationException();
        result = ServiceUtils.removeAsType(map, "c", Integer.class, validationException);
        assertNull(result);
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(
            validationException.validationErrors().get(0),
            containsString("field [c] is not of the expected type. The value [true] cannot be converted to a [Integer]")
        );
        assertNull(map.get("c"));

        // cannot convert double to integer
        validationException = new ValidationException();
        result = ServiceUtils.removeAsType(map, "d", Integer.class, validationException);
        assertNull(result);
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(
            validationException.validationErrors().get(0),
            containsString("field [d] is not of the expected type. The value [5.0] cannot be converted to a [Integer]")
        );
        assertNull(map.get("d"));

        // cannot convert integer to double
        validationException = new ValidationException();
        result = ServiceUtils.removeAsType(map, "e", Double.class, validationException);
        assertNull(result);
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(
            validationException.validationErrors().get(0),
            containsString("field [e] is not of the expected type. The value [5] cannot be converted to a [Double]")
        );
        assertNull(map.get("e"));

        assertThat(map.entrySet(), empty());
    }

    public void testRemoveAsTypeMissingReturnsNull() {
        Map<String, Object> map = new HashMap<>(Map.of("a", 5, "b", "a string", "c", Boolean.TRUE));
        assertNull(ServiceUtils.removeAsType(map, "missing", Integer.class));
        assertThat(map.entrySet(), hasSize(3));
    }

    public void testRemoveAsOneOfTypes_Validation_WithCorrectTypes() {
        Map<String, Object> map = new HashMap<>(Map.of("a", 5, "b", "a string", "c", Boolean.TRUE, "d", 1.0));
        ValidationException validationException = new ValidationException();

        Integer i = (Integer) ServiceUtils.removeAsOneOfTypes(map, "a", List.of(String.class, Integer.class), validationException);
        assertEquals(Integer.valueOf(5), i);
        assertNull(map.get("a")); // field has been removed

        String str = (String) ServiceUtils.removeAsOneOfTypes(map, "b", List.of(Integer.class, String.class), validationException);
        assertEquals("a string", str);
        assertNull(map.get("b"));

        Boolean b = (Boolean) ServiceUtils.removeAsOneOfTypes(map, "c", List.of(String.class, Boolean.class), validationException);
        assertEquals(Boolean.TRUE, b);
        assertNull(map.get("c"));

        Double d = (Double) ServiceUtils.removeAsOneOfTypes(map, "d", List.of(Booleans.class, Double.class), validationException);
        assertEquals(Double.valueOf(1.0), d);
        assertNull(map.get("d"));

        assertThat(map.entrySet(), empty());
    }

    public void testRemoveAsOneOfTypes_Validation_WithIncorrectType() {
        Map<String, Object> map = new HashMap<>(Map.of("a", 5, "b", "a string", "c", Boolean.TRUE, "d", 5.0, "e", 5));

        var validationException = new ValidationException();
        Object result = ServiceUtils.removeAsOneOfTypes(map, "a", List.of(String.class, Boolean.class), validationException);
        assertNull(result);
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(
            validationException.validationErrors().get(0),
            containsString("field [a] is not of one of the expected types. The value [5] cannot be converted to one of [String, Boolean]")
        );
        assertNull(map.get("a"));

        validationException = new ValidationException();
        result = ServiceUtils.removeAsOneOfTypes(map, "b", List.of(Boolean.class, Integer.class), validationException);
        assertNull(result);
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(
            validationException.validationErrors().get(0),
            containsString(
                "field [b] is not of one of the expected types. The value [a string] cannot be converted to one of [Boolean, Integer]"
            )
        );
        assertNull(map.get("b"));

        validationException = new ValidationException();
        result = ServiceUtils.removeAsOneOfTypes(map, "c", List.of(String.class, Integer.class), validationException);
        assertNull(result);
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(
            validationException.validationErrors().get(0),
            containsString(
                "field [c] is not of one of the expected types. The value [true] cannot be converted to one of [String, Integer]"
            )
        );
        assertNull(map.get("c"));

        validationException = new ValidationException();
        result = ServiceUtils.removeAsOneOfTypes(map, "d", List.of(String.class, Boolean.class), validationException);
        assertNull(result);
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(
            validationException.validationErrors().get(0),
            containsString("field [d] is not of one of the expected types. The value [5.0] cannot be converted to one of [String, Boolean]")
        );
        assertNull(map.get("d"));

        validationException = new ValidationException();
        result = ServiceUtils.removeAsOneOfTypes(map, "e", List.of(String.class, Boolean.class), validationException);
        assertNull(result);
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(
            validationException.validationErrors().get(0),
            containsString("field [e] is not of one of the expected types. The value [5] cannot be converted to one of [String, Boolean]")
        );
        assertNull(map.get("e"));

        assertThat(map.entrySet(), empty());
    }

    public void testRemoveAsOneOfTypesMissingReturnsNull() {
        Map<String, Object> map = new HashMap<>(Map.of("a", 5, "b", "a string", "c", Boolean.TRUE));
        assertNull(ServiceUtils.removeAsOneOfTypes(map, "missing", List.of(Integer.class), new ValidationException()));
        assertThat(map.entrySet(), hasSize(3));
    }

    public void testRemoveAsAdaptiveAllocationsSettings() {
        Map<String, Object> map = new HashMap<>(
            Map.of("settings", new HashMap<>(Map.of("enabled", true, "min_number_of_allocations", 7, "max_number_of_allocations", 42)))
        );
        ValidationException validationException = new ValidationException();
        assertThat(
            ServiceUtils.removeAsAdaptiveAllocationsSettings(map, "settings", validationException),
            equalTo(new AdaptiveAllocationsSettings(true, 7, 42))
        );
        assertThat(validationException.validationErrors(), empty());

        assertThat(ServiceUtils.removeAsAdaptiveAllocationsSettings(map, "non-existent-key", validationException), nullValue());
        assertThat(validationException.validationErrors(), empty());

        map = new HashMap<>(Map.of("settings", new HashMap<>(Map.of("enabled", false))));
        assertThat(
            ServiceUtils.removeAsAdaptiveAllocationsSettings(map, "settings", validationException),
            equalTo(new AdaptiveAllocationsSettings(false, null, null))
        );
        assertThat(validationException.validationErrors(), empty());
    }

    public void testRemoveAsAdaptiveAllocationsSettings_exceptions() {
        Map<String, Object> map = new HashMap<>(
            Map.of("settings", new HashMap<>(Map.of("enabled", "YES!", "blah", 42, "max_number_of_allocations", -7)))
        );
        ValidationException validationException = new ValidationException();
        ServiceUtils.removeAsAdaptiveAllocationsSettings(map, "settings", validationException);
        assertThat(validationException.validationErrors(), hasSize(3));
        assertThat(
            validationException.validationErrors().get(0),
            containsString("field [enabled] is not of the expected type. The value [YES!] cannot be converted to a [Boolean]")
        );
        assertThat(validationException.validationErrors().get(1), containsString("[settings] does not allow the setting [blah]"));
        assertThat(
            validationException.validationErrors().get(2),
            containsString("[max_number_of_allocations] must be a positive integer or null")
        );
    }

    public void testConvertToUri_CreatesUri() {
        var validation = new ValidationException();
        var uri = convertToUri("www.elastic.co", "name", "scope", validation);

        assertNotNull(uri);
        assertTrue(validation.validationErrors().isEmpty());
        assertThat(uri.toString(), is("www.elastic.co"));
    }

    public void testConvertToUri_DoesNotThrowNullPointerException_WhenPassedNull() {
        var validation = new ValidationException();
        var uri = convertToUri(null, "name", "scope", validation);

        assertNull(uri);
        assertTrue(validation.validationErrors().isEmpty());
    }

    public void testConvertToUri_AddsValidationError_WhenUrlIsInvalid() {
        var validation = new ValidationException();
        var uri = convertToUri("^^", "name", "scope", validation);

        assertNull(uri);
        assertThat(validation.validationErrors().size(), is(1));
        assertThat(validation.validationErrors().get(0), containsString("[scope] Invalid url [^^] received for field [name]"));
    }

    public void testConvertToUri_AddsValidationError_WhenUrlIsInvalid_PreservesReason() {
        var validation = new ValidationException();
        var uri = convertToUri("^^", "name", "scope", validation);

        assertNull(uri);
        assertThat(validation.validationErrors().size(), is(1));
        assertThat(
            validation.validationErrors().get(0),
            is("[scope] Invalid url [^^] received for field [name]. Error: unable to parse url [^^]. Reason: Illegal character in path")
        );
    }

    public void testCreateUri_CreatesUri() {
        var uri = createUri("www.elastic.co");

        assertNotNull(uri);
        assertThat(uri.toString(), is("www.elastic.co"));
    }

    public void testCreateUri_ThrowsException_WithInvalidUrl() {
        var exception = expectThrows(IllegalArgumentException.class, () -> createUri("^^"));

        assertThat(exception.getMessage(), containsString("unable to parse url [^^]"));
    }

    public void testCreateUri_ThrowsException_WithNullUrl() {
        expectThrows(NullPointerException.class, () -> createUri(null));
    }

    public void testExtractRequiredSecureString_CreatesSecureString() {
        var validation = new ValidationException();
        Map<String, Object> map = modifiableMap(Map.of("key", "value"));
        var secureString = extractRequiredSecureString(map, "key", "scope", validation);

        assertTrue(validation.validationErrors().isEmpty());
        assertNotNull(secureString);
        assertThat(secureString.toString(), is("value"));
        assertTrue(map.isEmpty());
    }

    public void testExtractRequiredSecureString_AddsException_WhenFieldDoesNotExist() {
        var validation = new ValidationException();
        Map<String, Object> map = modifiableMap(Map.of("key", "value"));
        var secureString = extractRequiredSecureString(map, "abc", "scope", validation);

        assertNull(secureString);
        assertFalse(validation.validationErrors().isEmpty());
        assertThat(map.size(), is(1));
        assertThat(validation.validationErrors().get(0), is("[scope] does not contain the required setting [abc]"));
    }

    public void testExtractRequiredSecureString_AddsException_WhenFieldIsEmpty() {
        var validation = new ValidationException();
        Map<String, Object> map = modifiableMap(Map.of("key", ""));
        var createdString = extractOptionalString(map, "key", "scope", validation);

        assertNull(createdString);
        assertFalse(validation.validationErrors().isEmpty());
        assertTrue(map.isEmpty());
        assertThat(validation.validationErrors().get(0), is("[scope] Invalid value empty string. [key] must be a non-empty string"));
    }

    public void testExtractRequiredString_CreatesString() {
        var validation = new ValidationException();
        validation.addValidationError("previous error");
        Map<String, Object> map = modifiableMap(Map.of("key", "value"));
        var createdString = extractRequiredString(map, "key", "scope", validation);

        assertThat(validation.validationErrors(), hasSize(1));
        assertNotNull(createdString);
        assertThat(createdString, is("value"));
        assertTrue(map.isEmpty());
    }

    public void testExtractRequiredString_AddsException_WhenFieldDoesNotExist() {
        var validation = new ValidationException();
        validation.addValidationError("previous error");

        Map<String, Object> map = modifiableMap(Map.of("key", "value"));
        var createdString = extractRequiredSecureString(map, "abc", "scope", validation);

        assertNull(createdString);
        assertThat(validation.validationErrors(), hasSize(2));
        assertThat(map.size(), is(1));
        assertThat(validation.validationErrors().get(1), is("[scope] does not contain the required setting [abc]"));
    }

    public void testExtractRequiredString_AddsException_WhenFieldIsEmpty() {
        var validation = new ValidationException();
        validation.addValidationError("previous error");
        Map<String, Object> map = modifiableMap(Map.of("key", ""));
        var createdString = extractOptionalString(map, "key", "scope", validation);

        assertNull(createdString);
        assertFalse(validation.validationErrors().isEmpty());
        assertTrue(map.isEmpty());
        assertThat(validation.validationErrors().get(1), is("[scope] Invalid value empty string. [key] must be a non-empty string"));
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

    public void testExtractOptionalPositiveInt() {
        var validation = new ValidationException();
        validation.addValidationError("previous error");
        Map<String, Object> map = modifiableMap(Map.of("abc", 1));
        assertEquals(Integer.valueOf(1), extractOptionalPositiveInteger(map, "abc", "scope", validation));
        assertThat(validation.validationErrors(), hasSize(1));
    }

    public void testExtractOptionalPositiveLong_IntegerValue() {
        var validation = new ValidationException();
        validation.addValidationError("previous error");
        Map<String, Object> map = modifiableMap(Map.of("abc", 3));
        assertEquals(Long.valueOf(3), extractOptionalPositiveLong(map, "abc", "scope", validation));
        assertThat(validation.validationErrors(), hasSize(1));
    }

    public void testExtractOptionalPositiveLong() {
        var validation = new ValidationException();
        validation.addValidationError("previous error");
        Map<String, Object> map = modifiableMap(Map.of("abc", 4_000_000_000L));
        assertEquals(Long.valueOf(4_000_000_000L), extractOptionalPositiveLong(map, "abc", "scope", validation));
        assertThat(validation.validationErrors(), hasSize(1));
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

    public void testExtractOptionalTimeValue_ReturnsNull_WhenKeyDoesNotExist() {
        var validation = new ValidationException();
        Map<String, Object> map = modifiableMap(Map.of("key", 1));
        var timeValue = extractOptionalTimeValue(map, "a", "scope", validation);

        assertNull(timeValue);
        assertTrue(validation.validationErrors().isEmpty());
    }

    public void testExtractOptionalTimeValue_CreatesTimeValue_Of3Seconds() {
        var validation = new ValidationException();
        Map<String, Object> map = modifiableMap(Map.of("key", "3s"));
        var timeValue = extractOptionalTimeValue(map, "key", "scope", validation);

        assertTrue(validation.validationErrors().isEmpty());
        assertNotNull(timeValue);
        assertThat(timeValue, is(TimeValue.timeValueSeconds(3)));
        assertTrue(map.isEmpty());
    }

    public void testExtractOptionalTimeValue_ReturnsNullAndAddsException_WhenTimeValueIsInvalid_InvalidUnit() {
        var validation = new ValidationException();
        Map<String, Object> map = modifiableMap(Map.of("key", "3abc"));
        var timeValue = extractOptionalTimeValue(map, "key", "scope", validation);

        assertFalse(validation.validationErrors().isEmpty());
        assertNull(timeValue);
        assertTrue(map.isEmpty());
        assertThat(
            validation.validationErrors().get(0),
            is(
                "[scope] Invalid time value [3abc]. [key] must be a valid time value string: failed to parse setting [key] "
                    + "with value [3abc] as a time value: unit is missing or unrecognized"
            )
        );
    }

    public void testExtractOptionalTimeValue_ReturnsNullAndAddsException_WhenTimeValueIsInvalid_NegativeNumber() {
        var validation = new ValidationException();
        Map<String, Object> map = modifiableMap(Map.of("key", "-3d"));
        var timeValue = extractOptionalTimeValue(map, "key", "scope", validation);

        assertFalse(validation.validationErrors().isEmpty());
        assertNull(timeValue);
        assertTrue(map.isEmpty());
        assertThat(
            validation.validationErrors().get(0),
            is(
                "[scope] Invalid time value [-3d]. [key] must be a valid time value string: failed to parse setting [key] "
                    + "with value [-3d] as a time value: negative durations are not supported"
            )
        );
    }

    public void testExtractOptionalDouble_ExtractsAsDoubleInRange() {
        var validationException = new ValidationException();
        Map<String, Object> map = modifiableMap(Map.of("key", 1.01));
        var result = ServiceUtils.extractOptionalDoubleInRange(map, "key", 0.0, 2.0, "test_scope", validationException);
        assertEquals(Double.valueOf(1.01), result);
        assertTrue(map.isEmpty());
        assertThat(validationException.validationErrors().size(), is(0));
    }

    public void testExtractOptionalDouble_InRange_ReturnsNullWhenKeyNotPresent() {
        var validationException = new ValidationException();
        Map<String, Object> map = modifiableMap(Map.of("key", 1.01));
        var result = ServiceUtils.extractOptionalDoubleInRange(map, "other_key", 0.0, 2.0, "test_scope", validationException);
        assertNull(result);
        assertThat(map.size(), is(1));
        assertThat(map.get("key"), is(1.01));
    }

    public void testExtractOptionalDouble_InRange_HasErrorWhenBelowMinValue() {
        var validationException = new ValidationException();
        Map<String, Object> map = modifiableMap(Map.of("key", -2.0));
        var result = ServiceUtils.extractOptionalDoubleInRange(map, "key", 0.0, 2.0, "test_scope", validationException);
        assertNull(result);
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(
            validationException.validationErrors().get(0),
            is("[test_scope] Invalid value [-2.0]. [key] must be a greater than or equal to [0.0]")
        );
    }

    public void testExtractOptionalDouble_InRange_HasErrorWhenAboveMaxValue() {
        var validationException = new ValidationException();
        Map<String, Object> map = modifiableMap(Map.of("key", 12.0));
        var result = ServiceUtils.extractOptionalDoubleInRange(map, "key", 0.0, 2.0, "test_scope", validationException);
        assertNull(result);
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(
            validationException.validationErrors().get(0),
            is("[test_scope] Invalid value [12.0]. [key] must be a less than or equal to [2.0]")
        );
    }

    public void testExtractOptionalDouble_InRange_DoesNotCheckMinWhenNull() {
        var validationException = new ValidationException();
        Map<String, Object> map = modifiableMap(Map.of("key", -2.0));
        var result = ServiceUtils.extractOptionalDoubleInRange(map, "key", null, 2.0, "test_scope", validationException);
        assertEquals(Double.valueOf(-2.0), result);
        assertTrue(map.isEmpty());
        assertThat(validationException.validationErrors().size(), is(0));
    }

    public void testExtractOptionalDouble_InRange_DoesNotCheckMaxWhenNull() {
        var validationException = new ValidationException();
        Map<String, Object> map = modifiableMap(Map.of("key", 12.0));
        var result = ServiceUtils.extractOptionalDoubleInRange(map, "key", 0.0, null, "test_scope", validationException);
        assertEquals(Double.valueOf(12.0), result);
        assertTrue(map.isEmpty());
        assertThat(validationException.validationErrors().size(), is(0));
    }

    public void testExtractOptionalFloat_ExtractsAFloat() {
        Map<String, Object> map = modifiableMap(Map.of("key", 1.0f));
        var result = ServiceUtils.extractOptionalFloat(map, "key");
        assertThat(result, is(1.0f));
        assertTrue(map.isEmpty());
    }

    public void testExtractOptionalFloat_ReturnsNullWhenKeyNotPresent() {
        Map<String, Object> map = modifiableMap(Map.of("key", 1.0f));
        var result = ServiceUtils.extractOptionalFloat(map, "other_key");
        assertNull(result);
        assertThat(map.size(), is(1));
        assertThat(map.get("key"), is(1.0f));
    }

    public void testExtractRequiredEnum_ExtractsAEnum() {
        ValidationException validationException = new ValidationException();
        Map<String, Object> map = modifiableMap(Map.of("key", "ingest"));
        var result = ServiceUtils.extractRequiredEnum(
            map,
            "key",
            "testscope",
            InputType::fromString,
            EnumSet.allOf(InputType.class),
            validationException
        );
        assertThat(result, is(InputType.INGEST));
    }

    public void testExtractRequiredEnum_ReturnsNullWhenEnumValueIsNotPresent() {
        ValidationException validationException = new ValidationException();
        Map<String, Object> map = modifiableMap(Map.of("key", "invalid"));
        var result = ServiceUtils.extractRequiredEnum(
            map,
            "key",
            "testscope",
            InputType::fromString,
            EnumSet.allOf(InputType.class),
            validationException
        );
        assertNull(result);
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors().get(0), containsString("Invalid value [invalid] received. [key] must be one of"));
    }

    public void testExtractRequiredEnum_HasValidationErrorOnMissingSetting() {
        ValidationException validationException = new ValidationException();
        Map<String, Object> map = modifiableMap(Map.of("key", "ingest"));
        var result = ServiceUtils.extractRequiredEnum(
            map,
            "missing_key",
            "testscope",
            InputType::fromString,
            EnumSet.allOf(InputType.class),
            validationException
        );
        assertNull(result);
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors().get(0), is("[testscope] does not contain the required setting [missing_key]"));
    }

    public void testGetEmbeddingSize_ReturnsError_WhenTextEmbeddingResults_IsEmpty() {
        var service = mock(InferenceService.class);

        var model = mock(Model.class);
        when(model.getTaskType()).thenReturn(TaskType.TEXT_EMBEDDING);

        doAnswer(invocation -> {
            ActionListener<InferenceServiceResults> listener = invocation.getArgument(7);
            listener.onResponse(new InferenceTextEmbeddingFloatResults(List.of()));

            return Void.TYPE;
        }).when(service).infer(any(), any(), any(), anyBoolean(), any(), any(), any(), any());

        PlainActionFuture<Integer> listener = new PlainActionFuture<>();
        getEmbeddingSize(model, service, listener);

        var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is("Could not determine embedding size"));
        assertThat(thrownException.getCause().getMessage(), is("Embeddings list is empty"));
    }

    public void testGetEmbeddingSize_ReturnsError_WhenTextEmbeddingByteResults_IsEmpty() {
        var service = mock(InferenceService.class);

        var model = mock(Model.class);
        when(model.getTaskType()).thenReturn(TaskType.TEXT_EMBEDDING);

        doAnswer(invocation -> {
            ActionListener<InferenceServiceResults> listener = invocation.getArgument(7);
            listener.onResponse(new InferenceTextEmbeddingByteResults(List.of()));

            return Void.TYPE;
        }).when(service).infer(any(), any(), any(), anyBoolean(), any(), any(), any(), any());

        PlainActionFuture<Integer> listener = new PlainActionFuture<>();
        getEmbeddingSize(model, service, listener);

        var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is("Could not determine embedding size"));
        assertThat(thrownException.getCause().getMessage(), is("Embeddings list is empty"));
    }

    public void testGetEmbeddingSize_ReturnsSize_ForTextEmbeddingResults() {
        var service = mock(InferenceService.class);

        var model = mock(Model.class);
        when(model.getTaskType()).thenReturn(TaskType.TEXT_EMBEDDING);

        var textEmbedding = TextEmbeddingResultsTests.createRandomResults();

        doAnswer(invocation -> {
            ActionListener<InferenceServiceResults> listener = invocation.getArgument(7);
            listener.onResponse(textEmbedding);

            return Void.TYPE;
        }).when(service).infer(any(), any(), any(), anyBoolean(), any(), any(), any(), any());

        PlainActionFuture<Integer> listener = new PlainActionFuture<>();
        getEmbeddingSize(model, service, listener);

        var size = listener.actionGet(TIMEOUT);

        assertThat(size, is(textEmbedding.embeddings().get(0).getSize()));
    }

    public void testGetEmbeddingSize_ReturnsSize_ForTextEmbeddingByteResults() {
        var service = mock(InferenceService.class);

        var model = mock(Model.class);
        when(model.getTaskType()).thenReturn(TaskType.TEXT_EMBEDDING);

        var textEmbedding = InferenceTextEmbeddingByteResultsTests.createRandomResults();

        doAnswer(invocation -> {
            ActionListener<InferenceServiceResults> listener = invocation.getArgument(7);
            listener.onResponse(textEmbedding);

            return Void.TYPE;
        }).when(service).infer(any(), any(), any(), anyBoolean(), any(), any(), any(), any());

        PlainActionFuture<Integer> listener = new PlainActionFuture<>();
        getEmbeddingSize(model, service, listener);

        var size = listener.actionGet(TIMEOUT);

        assertThat(size, is(textEmbedding.embeddings().get(0).getSize()));
    }

    private static <K, V> Map<K, V> modifiableMap(Map<K, V> aMap) {
        return new HashMap<>(aMap);
    }
}
