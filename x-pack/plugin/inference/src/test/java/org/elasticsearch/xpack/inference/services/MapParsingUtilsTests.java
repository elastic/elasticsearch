/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.MapParsingUtils.convertToUri;
import static org.elasticsearch.xpack.inference.services.MapParsingUtils.createUri;
import static org.elasticsearch.xpack.inference.services.MapParsingUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.MapParsingUtils.extractRequiredSecureString;
import static org.elasticsearch.xpack.inference.services.MapParsingUtils.extractRequiredString;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class MapParsingUtilsTests extends ESTestCase {

    public void testRemoveAsTypeWithTheCorrectType() {
        Map<String, Object> map = new HashMap<>(Map.of("a", 5, "b", "a string", "c", Boolean.TRUE, "d", 1.0));

        Integer i = MapParsingUtils.removeAsType(map, "a", Integer.class);
        assertEquals(Integer.valueOf(5), i);
        assertNull(map.get("a")); // field has been removed

        String str = MapParsingUtils.removeAsType(map, "b", String.class);
        assertEquals("a string", str);
        assertNull(map.get("b"));

        Boolean b = MapParsingUtils.removeAsType(map, "c", Boolean.class);
        assertEquals(Boolean.TRUE, b);
        assertNull(map.get("c"));

        Double d = MapParsingUtils.removeAsType(map, "d", Double.class);
        assertEquals(Double.valueOf(1.0), d);
        assertNull(map.get("d"));

        assertThat(map.entrySet(), empty());
    }

    public void testRemoveAsTypeWithInCorrectType() {
        Map<String, Object> map = new HashMap<>(Map.of("a", 5, "b", "a string", "c", Boolean.TRUE, "d", 5.0, "e", 5));

        var e = expectThrows(ElasticsearchStatusException.class, () -> MapParsingUtils.removeAsType(map, "a", String.class));
        assertThat(
            e.getMessage(),
            containsString("field [a] is not of the expected type. The value [5] cannot be converted to a [String]")
        );

        e = expectThrows(ElasticsearchStatusException.class, () -> MapParsingUtils.removeAsType(map, "b", Boolean.class));
        assertThat(
            e.getMessage(),
            containsString("field [b] is not of the expected type. The value [a string] cannot be converted to a [Boolean]")
        );
        assertNull(map.get("b"));

        e = expectThrows(ElasticsearchStatusException.class, () -> MapParsingUtils.removeAsType(map, "c", Integer.class));
        assertThat(
            e.getMessage(),
            containsString("field [c] is not of the expected type. The value [true] cannot be converted to a [Integer]")
        );
        assertNull(map.get("c"));

        // cannot convert double to integer
        e = expectThrows(ElasticsearchStatusException.class, () -> MapParsingUtils.removeAsType(map, "d", Integer.class));
        assertThat(
            e.getMessage(),
            containsString("field [d] is not of the expected type. The value [5.0] cannot be converted to a [Integer]")
        );
        assertNull(map.get("d"));

        // cannot convert integer to double
        e = expectThrows(ElasticsearchStatusException.class, () -> MapParsingUtils.removeAsType(map, "e", Double.class));
        assertThat(
            e.getMessage(),
            containsString("field [e] is not of the expected type. The value [5] cannot be converted to a [Double]")
        );
        assertNull(map.get("d"));

        assertThat(map.entrySet(), empty());
    }

    public void testRemoveAsTypeMissingReturnsNull() {
        Map<String, Object> map = new HashMap<>(Map.of("a", 5, "b", "a string", "c", Boolean.TRUE));
        assertNull(MapParsingUtils.removeAsType(new HashMap<>(), "missing", Integer.class));
        assertThat(map.entrySet(), hasSize(3));
    }

    public void testConvertToUri_CreatesUri() {
        var validation = new ValidationException();
        var uri = convertToUri("www.elastic.co", "name", "scope", validation);

        assertNotNull(uri);
        assertTrue(validation.validationErrors().isEmpty());
        assertThat(uri.toString(), is("www.elastic.co"));
    }

    public void testConvertToUri_ThrowsNullPointerException_WhenPassedNull() {
        var validation = new ValidationException();
        expectThrows(NullPointerException.class, () -> convertToUri(null, "name", "scope", validation));

        assertTrue(validation.validationErrors().isEmpty());
    }

    public void testConvertToUri_AddsValidationError_WhenUrlIsInvalid() {
        var validation = new ValidationException();
        var uri = convertToUri("^^", "name", "scope", validation);

        assertNull(uri);
        assertThat(validation.validationErrors().size(), is(1));
        assertThat(validation.validationErrors().get(0), is("[scope] Invalid url [^^] received for field [name]"));
    }

    public void testCreateUri_CreatesUri() {
        var uri = createUri("www.elastic.co");

        assertNotNull(uri);
        assertThat(uri.toString(), is("www.elastic.co"));
    }

    public void testCreateUri_ThrowsException_WithInvalidUrl() {
        var exception = expectThrows(IllegalArgumentException.class, () -> createUri("^^"));

        assertThat(exception.getMessage(), is("unable to parse url [^^]"));
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
        Map<String, Object> map = modifiableMap(Map.of("key", "value"));
        var createdString = extractRequiredString(map, "key", "scope", validation);

        assertTrue(validation.validationErrors().isEmpty());
        assertNotNull(createdString);
        assertThat(createdString, is("value"));
        assertTrue(map.isEmpty());
    }

    public void testExtractRequiredString_AddsException_WhenFieldDoesNotExist() {
        var validation = new ValidationException();
        Map<String, Object> map = modifiableMap(Map.of("key", "value"));
        var createdString = extractRequiredSecureString(map, "abc", "scope", validation);

        assertNull(createdString);
        assertFalse(validation.validationErrors().isEmpty());
        assertThat(map.size(), is(1));
        assertThat(validation.validationErrors().get(0), is("[scope] does not contain the required setting [abc]"));
    }

    public void testExtractRequiredString_AddsException_WhenFieldIsEmpty() {
        var validation = new ValidationException();
        Map<String, Object> map = modifiableMap(Map.of("key", ""));
        var createdString = extractOptionalString(map, "key", "scope", validation);

        assertNull(createdString);
        assertFalse(validation.validationErrors().isEmpty());
        assertTrue(map.isEmpty());
        assertThat(validation.validationErrors().get(0), is("[scope] Invalid value empty string. [key] must be a non-empty string"));
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
        Map<String, Object> map = modifiableMap(Map.of("key", "value"));
        var createdString = extractOptionalString(map, "abc", "scope", validation);

        assertNull(createdString);
        assertTrue(validation.validationErrors().isEmpty());
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

    private static <K, V> Map<K, V> modifiableMap(Map<K, V> aMap) {
        return new HashMap<>(aMap);
    }
}
