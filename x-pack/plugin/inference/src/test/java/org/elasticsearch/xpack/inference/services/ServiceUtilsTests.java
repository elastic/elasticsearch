/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.inference.InferencePlugin;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.inference.util.InferenceUtils.extractRequiredEnum;
import static org.elasticsearch.xpack.core.inference.util.InferenceUtils.modifiableMap;
import static org.elasticsearch.xpack.inference.Utils.mockClusterService;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.convertMapStringsToSecureString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.convertToUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.validateInputTypeIsUnspecifiedOrInternal;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ServiceUtilsTests extends ESTestCase {

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

    public void testExtractRequiredEnum_HasValidationErrorOnMissingSetting() {
        ValidationException validationException = new ValidationException();
        Map<String, Object> map = modifiableMap(Map.of("key", "ingest"));
        var result = extractRequiredEnum(
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

    public void testValidateInputType_NoValidationErrorsWhenInternalType() {
        ValidationException validationException = new ValidationException();

        validateInputTypeIsUnspecifiedOrInternal(InputType.INTERNAL_SEARCH, validationException);
        assertThat(validationException.validationErrors().size(), is(0));

        validateInputTypeIsUnspecifiedOrInternal(InputType.INTERNAL_INGEST, validationException);
        assertThat(validationException.validationErrors().size(), is(0));
    }

    public void testValidateInputType_NoValidationErrorsWhenInputTypeIsNullOrUnspecified() {
        ValidationException validationException = new ValidationException();

        validateInputTypeIsUnspecifiedOrInternal(InputType.UNSPECIFIED, validationException);
        assertThat(validationException.validationErrors().size(), is(0));

        validateInputTypeIsUnspecifiedOrInternal(null, validationException);
        assertThat(validationException.validationErrors().size(), is(0));
    }

    public void testValidateInputType_ValidationErrorsWhenInputTypeIsSpecified() {
        ValidationException validationException = new ValidationException();

        validateInputTypeIsUnspecifiedOrInternal(InputType.SEARCH, validationException);
        assertThat(validationException.validationErrors().size(), is(1));

        validateInputTypeIsUnspecifiedOrInternal(InputType.INGEST, validationException);
        assertThat(validationException.validationErrors().size(), is(2));

        validateInputTypeIsUnspecifiedOrInternal(InputType.CLASSIFICATION, validationException);
        assertThat(validationException.validationErrors().size(), is(3));

        validateInputTypeIsUnspecifiedOrInternal(InputType.CLUSTERING, validationException);
        assertThat(validationException.validationErrors().size(), is(4));
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

    public void testConvertMapStringsToSecureString() {
        var validation = new ValidationException();
        assertThat(
            convertMapStringsToSecureString(Map.of("key", "value", "key2", "abc"), "setting", validation),
            is(Map.of("key", new SecureString("value".toCharArray()), "key2", new SecureString("abc".toCharArray())))
        );
    }

    public void testConvertMapStringsToSecureString_ReturnsAnEmptyMap_WhenMapIsNull() {
        var validation = new ValidationException();
        assertThat(convertMapStringsToSecureString(null, "setting", validation), is(Map.of()));
    }

    public void testConvertMapStringsToSecureString_ThrowsException_WhenMapContainsInvalidTypes() {
        var validation = new ValidationException();
        var exception = expectThrows(
            ValidationException.class,
            () -> convertMapStringsToSecureString(Map.of("key", "value", "key2", 123), "setting", validation)
        );

        assertThat(
            exception.getMessage(),
            is("Validation Failed: 1: Map field [setting] has an entry that is not valid. Value type is not one of [String].;")
        );
    }

    public void testResolveInferenceTimeout_WithProvidedTimeout_ReturnsProvidedTimeout() {
        var clusterService = mockClusterService(Settings.builder().put(InferencePlugin.INFERENCE_QUERY_TIMEOUT.getKey(), "10s").build());
        var providedTimeout = TimeValue.timeValueSeconds(45);

        for (InputType inputType : InputType.values()) {
            var result = ServiceUtils.resolveInferenceTimeout(providedTimeout, inputType, clusterService);
            assertEquals("Input type " + inputType + " should return provided timeout", providedTimeout, result);
        }
    }

    public void testResolveInferenceTimeout_WithNullTimeout_ReturnsExpectedTimeoutByInputType() {
        var configuredTimeout = TimeValue.timeValueSeconds(10);
        var clusterService = mockClusterService(
            Settings.builder().put(InferencePlugin.INFERENCE_QUERY_TIMEOUT.getKey(), configuredTimeout).build()
        );

        Map<InputType, TimeValue> expectedTimeouts = Map.of(
            InputType.SEARCH,
            configuredTimeout,
            InputType.INTERNAL_SEARCH,
            configuredTimeout,
            InputType.INGEST,
            InferenceAction.Request.DEFAULT_TIMEOUT,
            InputType.INTERNAL_INGEST,
            InferenceAction.Request.DEFAULT_TIMEOUT,
            InputType.CLASSIFICATION,
            InferenceAction.Request.DEFAULT_TIMEOUT,
            InputType.CLUSTERING,
            InferenceAction.Request.DEFAULT_TIMEOUT,
            InputType.UNSPECIFIED,
            InferenceAction.Request.DEFAULT_TIMEOUT
        );

        for (Map.Entry<InputType, TimeValue> entry : expectedTimeouts.entrySet()) {
            InputType inputType = entry.getKey();
            TimeValue expectedTimeout = entry.getValue();

            var result = ServiceUtils.resolveInferenceTimeout(null, inputType, clusterService);
            assertEquals("Input type " + inputType + " should return expected timeout", expectedTimeout, result);
        }
    }
}
