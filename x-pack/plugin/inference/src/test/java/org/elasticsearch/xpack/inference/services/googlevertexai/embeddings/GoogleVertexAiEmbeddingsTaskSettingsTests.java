/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.inference.InputTypeTests.randomWithoutUnspecified;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiService.VALID_INPUT_TYPE_VALUES;
import static org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsTaskSettings.AUTO_TRUNCATE;
import static org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsTaskSettings.INPUT_TYPE;
import static org.hamcrest.Matchers.is;

public class GoogleVertexAiEmbeddingsTaskSettingsTests extends AbstractBWCWireSerializationTestCase<GoogleVertexAiEmbeddingsTaskSettings> {
    public void testIsEmpty() {
        var randomSettings = createRandom();
        var stringRep = Strings.toString(randomSettings);
        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    public void testUpdatedTaskSettings() {
        var initialSettings = createRandom();
        var newSettings = createRandom();
        Map<String, Object> newSettingsMap = new HashMap<>();
        if (newSettings.autoTruncate() != null) {
            newSettingsMap.put(GoogleVertexAiEmbeddingsTaskSettings.AUTO_TRUNCATE, newSettings.autoTruncate());
        }
        if (newSettings.getInputType() != null) {
            newSettingsMap.put(GoogleVertexAiEmbeddingsTaskSettings.INPUT_TYPE, newSettings.getInputType().toString());
        }
        GoogleVertexAiEmbeddingsTaskSettings updatedSettings = (GoogleVertexAiEmbeddingsTaskSettings) initialSettings.updatedTaskSettings(
            Collections.unmodifiableMap(newSettingsMap)
        );
        if (newSettings.autoTruncate() == null) {
            assertEquals(initialSettings.autoTruncate(), updatedSettings.autoTruncate());
        } else {
            assertEquals(newSettings.autoTruncate(), updatedSettings.autoTruncate());
        }
        if (newSettings.getInputType() == null) {
            assertEquals(initialSettings.getInputType(), updatedSettings.getInputType());
        } else {
            assertEquals(newSettings.getInputType(), updatedSettings.getInputType());
        }
    }

    public void testFromMap_CreatesEmptySettings_WhenAllFieldsAreNull() {
        MatcherAssert.assertThat(
            GoogleVertexAiEmbeddingsTaskSettings.fromMap(new HashMap<>()),
            is(new GoogleVertexAiEmbeddingsTaskSettings(null, null))
        );
        assertNull(GoogleVertexAiEmbeddingsTaskSettings.fromMap(new HashMap<>()).autoTruncate());
        assertNull(GoogleVertexAiEmbeddingsTaskSettings.fromMap(new HashMap<>()).getInputType());
    }

    public void testFromMap_CreatesEmptySettings_WhenMapIsNull() {
        MatcherAssert.assertThat(
            GoogleVertexAiEmbeddingsTaskSettings.fromMap(null),
            is(new GoogleVertexAiEmbeddingsTaskSettings(null, null))
        );
        assertNull(GoogleVertexAiEmbeddingsTaskSettings.fromMap(null).autoTruncate());
        assertNull(GoogleVertexAiEmbeddingsTaskSettings.fromMap(null).getInputType());
    }

    public void testFromMap_AutoTruncateIsSet() {
        var autoTruncate = true;
        var taskSettingsMap = getTaskSettingsMap(autoTruncate, null);
        var taskSettings = GoogleVertexAiEmbeddingsTaskSettings.fromMap(taskSettingsMap);

        assertThat(taskSettings, is(new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate, null)));
    }

    public void testFromMap_ThrowsValidationException_IfAutoTruncateIsInvalidValue() {
        var taskSettings = getTaskSettingsMap("invalid", null);

        expectThrows(ValidationException.class, () -> GoogleVertexAiEmbeddingsTaskSettings.fromMap(taskSettings));
    }

    public void testFromMap_AutoTruncateIsNull() {
        var taskSettingsMap = getTaskSettingsMap(null, null);
        var taskSettings = GoogleVertexAiEmbeddingsTaskSettings.fromMap(taskSettingsMap);
        // needed, because of constructors being ambiguous otherwise
        Boolean nullBoolean = null;

        assertThat(taskSettings, is(new GoogleVertexAiEmbeddingsTaskSettings(nullBoolean, null)));
    }

    public void testFromMap_ReturnsFailure_WhenInputTypeIsInvalid() {
        var exception = expectThrows(
            ValidationException.class,
            () -> GoogleVertexAiEmbeddingsTaskSettings.fromMap(
                new HashMap<>(Map.of(GoogleVertexAiEmbeddingsTaskSettings.INPUT_TYPE, "abc"))
            )
        );

        assertThat(
            exception.getMessage(),
            is(
                Strings.format(
                    "Validation Failed: 1: [task_settings] Invalid value [abc] received. [input_type] must be one of [%s];",
                    getValidValuesSortedAndCombined(VALID_INPUT_TYPE_VALUES)
                )
            )
        );
    }

    public void testFromMap_ReturnsFailure_WhenInputTypeIsUnspecified() {
        var exception = expectThrows(
            ValidationException.class,
            () -> GoogleVertexAiEmbeddingsTaskSettings.fromMap(
                new HashMap<>(Map.of(GoogleVertexAiEmbeddingsTaskSettings.INPUT_TYPE, InputType.UNSPECIFIED.toString()))
            )
        );

        assertThat(
            exception.getMessage(),
            is(
                Strings.format(
                    "Validation Failed: 1: [task_settings] Invalid value [unspecified] received. [input_type] must be one of [%s];",
                    getValidValuesSortedAndCombined(VALID_INPUT_TYPE_VALUES)
                )
            )
        );
    }

    public void testOf_UseRequestSettings() {
        var originalAutoTruncate = true;
        var originalSettings = new GoogleVertexAiEmbeddingsTaskSettings(originalAutoTruncate, null);

        var requestAutoTruncate = originalAutoTruncate == false;
        var requestTaskSettings = new GoogleVertexAiEmbeddingsRequestTaskSettings(requestAutoTruncate, null);

        assertThat(GoogleVertexAiEmbeddingsTaskSettings.of(originalSettings, requestTaskSettings).autoTruncate(), is(requestAutoTruncate));
    }

    public void testOf_UseOriginalSettings() {
        var originalAutoTruncate = true;
        var originalSettings = new GoogleVertexAiEmbeddingsTaskSettings(originalAutoTruncate, null);

        var requestTaskSettings = new GoogleVertexAiEmbeddingsRequestTaskSettings(null, null);

        assertThat(GoogleVertexAiEmbeddingsTaskSettings.of(originalSettings, requestTaskSettings).autoTruncate(), is(originalAutoTruncate));
    }

    public void testOf_UseOriginalSettings_WithInputType() {
        var originalAutoTruncate = true;
        var originalSettings = new GoogleVertexAiEmbeddingsTaskSettings(originalAutoTruncate, InputType.INGEST);

        var requestTaskSettings = new GoogleVertexAiEmbeddingsRequestTaskSettings(null, null);

        assertThat(GoogleVertexAiEmbeddingsTaskSettings.of(originalSettings, requestTaskSettings).autoTruncate(), is(originalAutoTruncate));
    }

    public void testToXContent_WritesAutoTruncateIfNotNull() throws IOException {
        var settings = GoogleVertexAiEmbeddingsTaskSettings.fromMap(getTaskSettingsMap(true, null));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"auto_truncate":true}"""));
    }

    public void testToXContent_DoesNotWriteAutoTruncateIfNull() throws IOException {
        var settings = GoogleVertexAiEmbeddingsTaskSettings.fromMap(getTaskSettingsMap(null, null));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {}"""));
    }

    public void testToXContent_WritesInputTypeIfNotNull() throws IOException {
        var settings = GoogleVertexAiEmbeddingsTaskSettings.fromMap(getTaskSettingsMap(true, InputType.INGEST));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"input_type":"ingest","auto_truncate":true}"""));
    }

    public void testToXContent_ThrowsAssertionFailure_WhenInputTypeIsUnspecified() {
        var thrownException = expectThrows(
            AssertionError.class,
            () -> new GoogleVertexAiEmbeddingsTaskSettings(false, InputType.UNSPECIFIED)
        );
        assertThat(thrownException.getMessage(), is("received invalid input type value [unspecified]"));
    }

    @Override
    protected Writeable.Reader<GoogleVertexAiEmbeddingsTaskSettings> instanceReader() {
        return GoogleVertexAiEmbeddingsTaskSettings::new;
    }

    @Override
    protected GoogleVertexAiEmbeddingsTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected GoogleVertexAiEmbeddingsTaskSettings mutateInstance(GoogleVertexAiEmbeddingsTaskSettings instance) throws IOException {
        return randomValueOtherThan(instance, GoogleVertexAiEmbeddingsTaskSettingsTests::createRandom);
    }

    @Override
    protected GoogleVertexAiEmbeddingsTaskSettings mutateInstanceForVersion(
        GoogleVertexAiEmbeddingsTaskSettings instance,
        TransportVersion version
    ) {
        if (version.before(TransportVersions.V_8_17_0)) {
            // default to null input type if node is on a version before input type was introduced
            return new GoogleVertexAiEmbeddingsTaskSettings(instance.autoTruncate(), null);
        }
        return instance;
    }

    private static GoogleVertexAiEmbeddingsTaskSettings createRandom() {
        var inputType = randomBoolean() ? randomWithoutUnspecified() : null;
        var autoTruncate = randomFrom(new Boolean[] { null, randomBoolean() });
        return new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate, inputType);
    }

    private static <E extends Enum<E>> String getValidValuesSortedAndCombined(EnumSet<E> validValues) {
        var validValuesAsStrings = validValues.stream().map(value -> value.toString().toLowerCase(Locale.ROOT)).toArray(String[]::new);
        Arrays.sort(validValuesAsStrings);

        return String.join(", ", validValuesAsStrings);
    }

    public static Map<String, Object> getTaskSettingsMap(@Nullable Object autoTruncate, @Nullable InputType inputType) {
        var map = new HashMap<String, Object>();

        if (autoTruncate != null) {
            map.put(AUTO_TRUNCATE, autoTruncate);
        }

        if (inputType != null) {
            map.put(INPUT_TYPE, inputType.toString());
        }

        return map;
    }
}
