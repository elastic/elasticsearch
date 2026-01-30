/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.RETURN_DOCUMENTS_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.TOP_N_FIELD;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AzureAiStudioRerankTaskSettingsTests extends AbstractBWCWireSerializationTestCase<AzureAiStudioRerankTaskSettings> {
    private static final String INVALID_FIELD_TYPE_STRING = "invalid";

    public void testIsEmpty() {
        final var randomSettings = createRandom();
        final var stringRep = Strings.toString(randomSettings);
        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    public void testUpdatedTaskSettings_WithAllValues() {
        final AzureAiStudioRerankTaskSettings initialSettings = createRandom();
        AzureAiStudioRerankTaskSettings newSettings = new AzureAiStudioRerankTaskSettings(
            randomValueOtherThan(initialSettings.returnDocuments(), ESTestCase::randomBoolean),
            randomValueOtherThan(initialSettings.topN(), ESTestCase::randomNonNegativeInt)
        );
        assertUpdateSettings(newSettings, initialSettings);
    }

    public void testUpdatedTaskSettings_WithReturnDocumentsValue() {
        final AzureAiStudioRerankTaskSettings initialSettings = createRandom();
        AzureAiStudioRerankTaskSettings newSettings = new AzureAiStudioRerankTaskSettings(
            randomValueOtherThan(initialSettings.returnDocuments(), ESTestCase::randomBoolean),
            null
        );
        assertUpdateSettings(newSettings, initialSettings);
    }

    public void testUpdatedTaskSettings_WithTopNValue() {
        final AzureAiStudioRerankTaskSettings initialSettings = createRandom();
        AzureAiStudioRerankTaskSettings newSettings = new AzureAiStudioRerankTaskSettings(
            null,
            randomValueOtherThan(initialSettings.topN(), ESTestCase::randomNonNegativeInt)
        );
        assertUpdateSettings(newSettings, initialSettings);
    }

    public void testUpdatedTaskSettings_WithNoValues() {
        AzureAiStudioRerankTaskSettings initialSettings = createRandom();
        final AzureAiStudioRerankTaskSettings newSettings = new AzureAiStudioRerankTaskSettings(null, null);
        assertUpdateSettings(newSettings, initialSettings);
    }

    private void assertUpdateSettings(AzureAiStudioRerankTaskSettings newSettings, AzureAiStudioRerankTaskSettings initialSettings) {
        final var settingsMap = new HashMap<String, Object>();
        if (newSettings.returnDocuments() != null) settingsMap.put(RETURN_DOCUMENTS_FIELD, newSettings.returnDocuments());
        if (newSettings.topN() != null) settingsMap.put(TOP_N_FIELD, newSettings.topN());

        final AzureAiStudioRerankTaskSettings updatedSettings = (AzureAiStudioRerankTaskSettings) initialSettings.updatedTaskSettings(
            Collections.unmodifiableMap(settingsMap)
        );
        assertEquals(
            newSettings.returnDocuments() == null ? initialSettings.returnDocuments() : newSettings.returnDocuments(),
            updatedSettings.returnDocuments()
        );
        assertEquals(newSettings.topN() == null ? initialSettings.topN() : newSettings.topN(), updatedSettings.topN());
    }

    public void testFromMap_AllValues() {
        assertEquals(new AzureAiStudioRerankTaskSettings(true, 2), AzureAiStudioRerankTaskSettings.fromMap(getTaskSettingsMap(true, 2)));
    }

    public void testFromMap_ReturnDocuments() {
        assertEquals(
            new AzureAiStudioRerankTaskSettings(true, null),
            AzureAiStudioRerankTaskSettings.fromMap(getTaskSettingsMap(true, null))
        );
    }

    public void testFromMap_TopN() {
        assertEquals(new AzureAiStudioRerankTaskSettings(null, 2), AzureAiStudioRerankTaskSettings.fromMap(getTaskSettingsMap(null, 2)));
    }

    public void testFromMap_ReturnDocumentsIsInvalidValue_ThrowsValidationException() {
        getTaskSettingsMap(true, 2).put(RETURN_DOCUMENTS_FIELD, INVALID_FIELD_TYPE_STRING);
        assertThrowsValidationExceptionIfStringValueProvidedFor(RETURN_DOCUMENTS_FIELD);
    }

    public void testFromMap_TopNIsInvalidValue_ThrowsValidationException() {
        getTaskSettingsMap(true, 2).put(TOP_N_FIELD, INVALID_FIELD_TYPE_STRING);
        assertThrowsValidationExceptionIfStringValueProvidedFor(TOP_N_FIELD);
    }

    public void testFromMap_WithNoValues_DoesNotThrowException() {
        final var taskMap = AzureAiStudioRerankTaskSettings.fromMap(new HashMap<>(Map.of()));
        assertNull(taskMap.returnDocuments());
        assertNull(taskMap.topN());
    }

    public void testOverrideWith_KeepsOriginalValuesWithOverridesAreNull() {
        final var settings = AzureAiStudioRerankTaskSettings.fromMap(getTaskSettingsMap(true, 2));
        final var overrideSettings = AzureAiStudioRerankTaskSettings.of(settings, AzureAiStudioRerankRequestTaskSettings.EMPTY_SETTINGS);
        MatcherAssert.assertThat(overrideSettings, is(settings));
    }

    public void testOverrideWith_UsesReturnDocumentsOverride() {
        final var settings = AzureAiStudioRerankTaskSettings.fromMap(getTaskSettingsMap(true, null));
        final var overrideSettings = AzureAiStudioRerankRequestTaskSettings.fromMap(getTaskSettingsMap(false, null));
        final var overriddenTaskSettings = AzureAiStudioRerankTaskSettings.of(settings, overrideSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new AzureAiStudioRerankTaskSettings(false, null)));
    }

    public void testOverrideWith_UsesTopNOverride() {
        final var settings = AzureAiStudioRerankTaskSettings.fromMap(getTaskSettingsMap(null, 2));
        final var overrideSettings = AzureAiStudioRerankRequestTaskSettings.fromMap(getTaskSettingsMap(null, 1));
        final var overriddenTaskSettings = AzureAiStudioRerankTaskSettings.of(settings, overrideSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new AzureAiStudioRerankTaskSettings(null, 1)));
    }

    public void testOverrideWith_UsesAllParametersOverride() {
        final var settings = AzureAiStudioRerankTaskSettings.fromMap(getTaskSettingsMap(false, 2));
        final var overrideSettings = AzureAiStudioRerankRequestTaskSettings.fromMap(getTaskSettingsMap(true, 1));
        final var overriddenTaskSettings = AzureAiStudioRerankTaskSettings.of(settings, overrideSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new AzureAiStudioRerankTaskSettings(true, 1)));
    }

    public void testToXContent_WithoutParameters() throws IOException {
        assertThat(getXContentResult(null, null), is("{}"));
    }

    public void testToXContent_WithReturnDocumentsParameter() throws IOException {
        assertThat(getXContentResult(true, null), is("""
            {"return_documents":true}"""));
    }

    public void testToXContent_WithTopNParameter() throws IOException {
        assertThat(getXContentResult(null, 2), is("""
            {"top_n":2}"""));
    }

    public void testToXContent_WithParameters() throws IOException {
        assertThat(getXContentResult(true, 2), is("""
            {"return_documents":true,"top_n":2}"""));
    }

    private String getXContentResult(Boolean returnDocuments, Integer topN) throws IOException {
        final var settings = AzureAiStudioRerankTaskSettings.fromMap(getTaskSettingsMap(returnDocuments, topN));
        final XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        return Strings.toString(builder);
    }

    public static Map<String, Object> getTaskSettingsMap(@Nullable Boolean returnDocuments, @Nullable Integer topN) {
        final var map = new HashMap<String, Object>();

        if (returnDocuments != null) {
            map.put(RETURN_DOCUMENTS_FIELD, returnDocuments);
        }

        if (topN != null) {
            map.put(TOP_N_FIELD, topN);
        }

        return map;
    }

    @Override
    protected Writeable.Reader<AzureAiStudioRerankTaskSettings> instanceReader() {
        return AzureAiStudioRerankTaskSettings::new;
    }

    @Override
    protected AzureAiStudioRerankTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AzureAiStudioRerankTaskSettings mutateInstance(AzureAiStudioRerankTaskSettings instance) throws IOException {
        if (randomBoolean()) {
            Boolean newReturnDocuments = instance.returnDocuments() == null ? randomBoolean() : instance.returnDocuments() == false;
            return new AzureAiStudioRerankTaskSettings(newReturnDocuments, instance.topN());
        } else {
            Integer topN = randomValueOtherThan(instance.topN(), ESTestCase::randomNonNegativeIntOrNull);
            return new AzureAiStudioRerankTaskSettings(instance.returnDocuments(), topN);
        }
    }

    @Override
    protected AzureAiStudioRerankTaskSettings mutateInstanceForVersion(AzureAiStudioRerankTaskSettings instance, TransportVersion version) {
        return instance;
    }

    private static AzureAiStudioRerankTaskSettings createRandom() {
        return new AzureAiStudioRerankTaskSettings(randomOptionalBoolean(), randomNonNegativeIntOrNull());
    }

    private void assertThrowsValidationExceptionIfStringValueProvidedFor(String field) {
        final var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureAiStudioRerankRequestTaskSettings.fromMap(new HashMap<>(Map.of(field, INVALID_FIELD_TYPE_STRING)))
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "field ["
                        + field
                        + "] is not of the expected type. The value ["
                        + INVALID_FIELD_TYPE_STRING
                        + "] cannot be converted to a "
                )
            )
        );
    }
}
