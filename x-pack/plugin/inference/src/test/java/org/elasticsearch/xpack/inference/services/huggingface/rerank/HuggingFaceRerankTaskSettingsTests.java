/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class HuggingFaceRerankTaskSettingsTests extends AbstractBWCWireSerializationTestCase<HuggingFaceRerankTaskSettings> {

    public static HuggingFaceRerankTaskSettings createRandom() {
        var returnDocuments = randomBoolean() ? randomBoolean() : null;
        var topNDocsOnly = randomBoolean() ? randomIntBetween(1, 10) : null;

        return new HuggingFaceRerankTaskSettings(topNDocsOnly, returnDocuments);
    }

    public void testFromMap_WithValidValues_ReturnsSettings() {
        Map<String, Object> taskMap = Map.of(
            HuggingFaceRerankTaskSettings.RETURN_DOCUMENTS,
            true,
            HuggingFaceRerankTaskSettings.TOP_N_DOCS_ONLY,
            5
        );
        var settings = HuggingFaceRerankTaskSettings.fromMap(new HashMap<>(taskMap));
        assertTrue(settings.getReturnDocuments());
        assertEquals(5, settings.getTopNDocumentsOnly().intValue());
    }

    public void testFromMap_WithNullValues_ReturnsSettingsWithNulls() {
        var settings = HuggingFaceRerankTaskSettings.fromMap(Map.of());
        assertNull(settings.getReturnDocuments());
        assertNull(settings.getTopNDocumentsOnly());
    }

    public void testFromMap_WithInvalidReturnDocuments_ThrowsValidationException() {
        Map<String, Object> taskMap = Map.of(
            HuggingFaceRerankTaskSettings.RETURN_DOCUMENTS,
            "invalid",
            HuggingFaceRerankTaskSettings.TOP_N_DOCS_ONLY,
            5
        );
        var thrownException = expectThrows(ValidationException.class, () -> HuggingFaceRerankTaskSettings.fromMap(new HashMap<>(taskMap)));
        assertThat(thrownException.getMessage(), containsString("field [return_documents] is not of the expected type"));
    }

    public void testFromMap_WithInvalidTopNDocsOnly_ThrowsValidationException() {
        Map<String, Object> taskMap = Map.of(
            HuggingFaceRerankTaskSettings.RETURN_DOCUMENTS,
            true,
            HuggingFaceRerankTaskSettings.TOP_N_DOCS_ONLY,
            "invalid"
        );
        var thrownException = expectThrows(ValidationException.class, () -> HuggingFaceRerankTaskSettings.fromMap(new HashMap<>(taskMap)));
        assertThat(thrownException.getMessage(), containsString("field [top_n] is not of the expected type"));
    }

    public void UpdatedTaskSettings_WithEmptyMap_ReturnsSameSettings() {
        var initialSettings = new HuggingFaceRerankTaskSettings(5, true);
        HuggingFaceRerankTaskSettings updatedSettings = (HuggingFaceRerankTaskSettings) initialSettings.updatedTaskSettings(Map.of());
        assertEquals(initialSettings, updatedSettings);
    }

    public void testUpdatedTaskSettings_WithNewReturnDocuments_ReturnsUpdatedSettings() {
        var initialSettings = new HuggingFaceRerankTaskSettings(5, true);
        Map<String, Object> newSettings = Map.of(HuggingFaceRerankTaskSettings.RETURN_DOCUMENTS, false);
        HuggingFaceRerankTaskSettings updatedSettings = (HuggingFaceRerankTaskSettings) initialSettings.updatedTaskSettings(newSettings);
        assertFalse(updatedSettings.getReturnDocuments());
        assertEquals(initialSettings.getTopNDocumentsOnly(), updatedSettings.getTopNDocumentsOnly());
    }

    public void testUpdatedTaskSettings_WithNewTopNDocsOnly_ReturnsUpdatedSettings() {
        var initialSettings = new HuggingFaceRerankTaskSettings(5, true);
        Map<String, Object> newSettings = Map.of(HuggingFaceRerankTaskSettings.TOP_N_DOCS_ONLY, 7);
        HuggingFaceRerankTaskSettings updatedSettings = (HuggingFaceRerankTaskSettings) initialSettings.updatedTaskSettings(newSettings);
        assertEquals(7, updatedSettings.getTopNDocumentsOnly().intValue());
        assertEquals(initialSettings.getReturnDocuments(), updatedSettings.getReturnDocuments());
    }

    public void testUpdatedTaskSettings_WithMultipleNewValues_ReturnsUpdatedSettings() {
        var initialSettings = new HuggingFaceRerankTaskSettings(5, true);
        Map<String, Object> newSettings = Map.of(
            HuggingFaceRerankTaskSettings.RETURN_DOCUMENTS,
            false,
            HuggingFaceRerankTaskSettings.TOP_N_DOCS_ONLY,
            7
        );
        HuggingFaceRerankTaskSettings updatedSettings = (HuggingFaceRerankTaskSettings) initialSettings.updatedTaskSettings(newSettings);
        assertFalse(updatedSettings.getReturnDocuments());
        assertEquals(7, updatedSettings.getTopNDocumentsOnly().intValue());
    }

    @Override
    protected Writeable.Reader<HuggingFaceRerankTaskSettings> instanceReader() {
        return HuggingFaceRerankTaskSettings::new;
    }

    @Override
    protected HuggingFaceRerankTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected HuggingFaceRerankTaskSettings mutateInstance(HuggingFaceRerankTaskSettings instance) throws IOException {
        return randomValueOtherThan(instance, HuggingFaceRerankTaskSettingsTests::createRandom);
    }

    @Override
    protected HuggingFaceRerankTaskSettings mutateInstanceForVersion(HuggingFaceRerankTaskSettings instance, TransportVersion version) {
        return instance;
    }
}
