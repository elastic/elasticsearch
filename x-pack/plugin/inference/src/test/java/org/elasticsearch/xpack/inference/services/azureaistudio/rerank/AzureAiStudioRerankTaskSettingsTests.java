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
    public void testIsEmpty() {
        var randomSettings = createRandom();
        var stringRep = Strings.toString(randomSettings);
        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    public void testUpdatedTaskSettings() {
        var initialSettings = createRandom();
        var newSettings = createRandom();
        var settingsMap = new HashMap<String, Object>();
        if (newSettings.returnDocuments() != null) settingsMap.put(RETURN_DOCUMENTS_FIELD, newSettings.returnDocuments());
        if (newSettings.topN() != null) settingsMap.put(TOP_N_FIELD, newSettings.topN());

        AzureAiStudioRerankTaskSettings updatedSettings = (AzureAiStudioRerankTaskSettings) initialSettings.updatedTaskSettings(
            Collections.unmodifiableMap(settingsMap)
        );

        assertEquals(
            newSettings.returnDocuments() == null ? initialSettings.returnDocuments() : newSettings.returnDocuments(),
            updatedSettings.returnDocuments()
        );
        assertEquals(newSettings.topN() == null ? initialSettings.topN() : newSettings.topN(), updatedSettings.topN());
    }

    public void testFromMap_AllValues() {
        var taskMap = getTaskSettingsMap(true, 2);
        assertEquals(new AzureAiStudioRerankTaskSettings(true, 2), AzureAiStudioRerankTaskSettings.fromMap(taskMap));
    }

    public void testFromMap_ReturnDocumentsIsInvalidValue_ThrowsValidationException() {
        var taskMap = getTaskSettingsMap(true, 2);
        taskMap.put(RETURN_DOCUMENTS_FIELD, "invalid");

        var thrownException = expectThrows(ValidationException.class, () -> AzureAiStudioRerankTaskSettings.fromMap(taskMap));

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString("field [return_documents] is not of the expected type. The value [invalid] cannot be converted to a [Boolean]")
        );
    }

    public void testFromMap_TopNIsInvalidValue_ThrowsValidationException() {
        var taskMap = getTaskSettingsMap(true, 2);
        taskMap.put(TOP_N_FIELD, "invalid");

        var thrownException = expectThrows(ValidationException.class, () -> AzureAiStudioRerankTaskSettings.fromMap(taskMap));

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format("field [top_n] is not of the expected type. The value [invalid] cannot be converted to a [Integer]")
            )
        );
    }

    public void testFromMap_WithNoValues_DoesNotThrowException() {
        var taskMap = AzureAiStudioRerankTaskSettings.fromMap(new HashMap<String, Object>(Map.of()));
        assertNull(taskMap.returnDocuments());
        assertNull(taskMap.topN());
    }

    public void testOverrideWith_KeepsOriginalValuesWithOverridesAreNull() {
        var settings = AzureAiStudioRerankTaskSettings.fromMap(getTaskSettingsMap(true, 2));
        var overrideSettings = AzureAiStudioRerankTaskSettings.of(settings, AzureAiStudioRerankRequestTaskSettings.EMPTY_SETTINGS);
        MatcherAssert.assertThat(overrideSettings, is(settings));
    }

    public void testOverrideWith_UsesReturnDocumentsOverride() {
        var settings = AzureAiStudioRerankTaskSettings.fromMap(getTaskSettingsMap(true, null));
        var overrideSettings = AzureAiStudioRerankRequestTaskSettings.fromMap(getTaskSettingsMap(false, null));
        var overriddenTaskSettings = AzureAiStudioRerankTaskSettings.of(settings, overrideSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new AzureAiStudioRerankTaskSettings(false, null)));
    }

    public void testOverrideWith_UsesTopNOverride() {
        var settings = AzureAiStudioRerankTaskSettings.fromMap(getTaskSettingsMap(null, 2));
        var overrideSettings = AzureAiStudioRerankRequestTaskSettings.fromMap(getTaskSettingsMap(null, 1));
        var overriddenTaskSettings = AzureAiStudioRerankTaskSettings.of(settings, overrideSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new AzureAiStudioRerankTaskSettings(null, 1)));
    }

    public void testToXContent_WithoutParameters() throws IOException {
        var settings = AzureAiStudioRerankTaskSettings.fromMap(getTaskSettingsMap(null, null));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("{}"));
    }

    public void testToXContent_WithParameters() throws IOException {
        var settings = AzureAiStudioRerankTaskSettings.fromMap(getTaskSettingsMap(true, 2));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"return_documents":true,"top_n":2}"""));
    }

    public static Map<String, Object> getTaskSettingsMap(@Nullable Boolean returnDocuments, @Nullable Integer topN) {
        var map = new HashMap<String, Object>();

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
        return randomValueOtherThan(instance, AzureAiStudioRerankTaskSettingsTests::createRandom);
    }

    @Override
    protected AzureAiStudioRerankTaskSettings mutateInstanceForVersion(AzureAiStudioRerankTaskSettings instance, TransportVersion version) {
        return instance;
    }

    private static AzureAiStudioRerankTaskSettings createRandom() {
        return new AzureAiStudioRerankTaskSettings(
            randomFrom(randomFrom(new Boolean[] { null, randomBoolean() })),
            randomFrom(new Integer[] { null, randomNonNegativeInt() })
        );
    }
}
