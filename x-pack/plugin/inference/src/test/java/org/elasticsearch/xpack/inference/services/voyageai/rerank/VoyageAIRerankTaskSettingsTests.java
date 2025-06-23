/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.rerank;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIServiceFields;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class VoyageAIRerankTaskSettingsTests extends AbstractWireSerializingTestCase<VoyageAIRerankTaskSettings> {

    public static VoyageAIRerankTaskSettings createRandom() {
        var returnDocuments = randomBoolean() ? randomBoolean() : null;
        var topNDocsOnly = randomBoolean() ? randomIntBetween(1, 10) : null;
        var truncation = randomBoolean() ? randomBoolean() : null;

        return new VoyageAIRerankTaskSettings(topNDocsOnly, returnDocuments, truncation);
    }

    public void testFromMap_WithInvalidTruncation_ThrowsValidationException() {
        Map<String, Object> taskMap = Map.of(
            VoyageAIRerankTaskSettings.RETURN_DOCUMENTS,
            true,
            VoyageAIRerankTaskSettings.TOP_K_DOCS_ONLY,
            5,
            VoyageAIServiceFields.TRUNCATION,
            "invalid"
        );
        var thrownException = expectThrows(ValidationException.class, () -> VoyageAIRerankTaskSettings.fromMap(new HashMap<>(taskMap)));
        assertThat(thrownException.getMessage(), containsString("field [truncation] is not of the expected type"));
    }

    public void testFromMap_WithValidValues_ReturnsSettings() {
        Map<String, Object> taskMap = Map.of(
            VoyageAIRerankTaskSettings.RETURN_DOCUMENTS,
            true,
            VoyageAIRerankTaskSettings.TOP_K_DOCS_ONLY,
            5,
            VoyageAIServiceFields.TRUNCATION,
            true
        );
        var settings = VoyageAIRerankTaskSettings.fromMap(new HashMap<>(taskMap));
        assertTrue(settings.getReturnDocuments());
        assertEquals(5, settings.getTopKDocumentsOnly().intValue());
        assertTrue(settings.getTruncation());
    }

    public void testFromMap_WithNullValues_ReturnsSettingsWithNulls() {
        var settings = VoyageAIRerankTaskSettings.fromMap(Map.of());
        assertNull(settings.getReturnDocuments());
        assertNull(settings.getTopKDocumentsOnly());
        assertNull(settings.getTruncation());
    }

    public void testFromMap_WithInvalidReturnDocuments_ThrowsValidationException() {
        Map<String, Object> taskMap = Map.of(
            VoyageAIRerankTaskSettings.RETURN_DOCUMENTS,
            "invalid",
            VoyageAIRerankTaskSettings.TOP_K_DOCS_ONLY,
            5
        );
        var thrownException = expectThrows(ValidationException.class, () -> VoyageAIRerankTaskSettings.fromMap(new HashMap<>(taskMap)));
        assertThat(thrownException.getMessage(), containsString("field [return_documents] is not of the expected type"));
    }

    public void testFromMap_WithInvalidTopNDocsOnly_ThrowsValidationException() {
        Map<String, Object> taskMap = Map.of(
            VoyageAIRerankTaskSettings.RETURN_DOCUMENTS,
            true,
            VoyageAIRerankTaskSettings.TOP_K_DOCS_ONLY,
            "invalid"
        );
        var thrownException = expectThrows(ValidationException.class, () -> VoyageAIRerankTaskSettings.fromMap(new HashMap<>(taskMap)));
        assertThat(
            thrownException.getMessage(),
            containsString("field [top_k] is not of the expected type. The value [invalid] cannot be converted to a [Integer];")
        );
    }

    public void testUpdatedTaskSettings_WithEmptyMap_ReturnsSameSettings() {
        var initialSettings = new VoyageAIRerankTaskSettings(5, true, true);
        VoyageAIRerankTaskSettings updatedSettings = (VoyageAIRerankTaskSettings) initialSettings.updatedTaskSettings(Map.of());
        assertEquals(initialSettings, updatedSettings);
    }

    public void testUpdatedTaskSettings_WithNewReturnDocuments_ReturnsUpdatedSettings() {
        var initialSettings = new VoyageAIRerankTaskSettings(5, true, true);
        Map<String, Object> newSettings = Map.of(VoyageAIRerankTaskSettings.RETURN_DOCUMENTS, false);
        VoyageAIRerankTaskSettings updatedSettings = (VoyageAIRerankTaskSettings) initialSettings.updatedTaskSettings(newSettings);
        assertFalse(updatedSettings.getReturnDocuments());
        assertTrue(updatedSettings.getTruncation());
        assertEquals(initialSettings.getTopKDocumentsOnly(), updatedSettings.getTopKDocumentsOnly());
    }

    public void testUpdatedTaskSettings_WithNewTopNDocsOnly_ReturnsUpdatedSettings() {
        var initialSettings = new VoyageAIRerankTaskSettings(5, true, true);
        Map<String, Object> newSettings = Map.of(VoyageAIRerankTaskSettings.TOP_K_DOCS_ONLY, 7);
        VoyageAIRerankTaskSettings updatedSettings = (VoyageAIRerankTaskSettings) initialSettings.updatedTaskSettings(newSettings);
        assertTrue(updatedSettings.getTruncation());
        assertEquals(7, updatedSettings.getTopKDocumentsOnly().intValue());
        assertEquals(initialSettings.getReturnDocuments(), updatedSettings.getReturnDocuments());
    }

    public void testUpdatedTaskSettings_WithMultipleNewValues_ReturnsUpdatedSettings() {
        var initialSettings = new VoyageAIRerankTaskSettings(5, true, true);
        Map<String, Object> newSettings = Map.of(
            VoyageAIRerankTaskSettings.RETURN_DOCUMENTS,
            false,
            VoyageAIRerankTaskSettings.TOP_K_DOCS_ONLY,
            7
        );
        VoyageAIRerankTaskSettings updatedSettings = (VoyageAIRerankTaskSettings) initialSettings.updatedTaskSettings(newSettings);
        assertTrue(updatedSettings.getTruncation());
        assertFalse(updatedSettings.getReturnDocuments());
        assertEquals(7, updatedSettings.getTopKDocumentsOnly().intValue());
    }

    @Override
    protected Writeable.Reader<VoyageAIRerankTaskSettings> instanceReader() {
        return VoyageAIRerankTaskSettings::new;
    }

    @Override
    protected VoyageAIRerankTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected VoyageAIRerankTaskSettings mutateInstance(VoyageAIRerankTaskSettings instance) throws IOException {
        return randomValueOtherThan(instance, VoyageAIRerankTaskSettingsTests::createRandom);
    }

    public static Map<String, Object> getTaskSettingsMapEmpty() {
        return new HashMap<>();
    }

    public static Map<String, Object> getTaskSettingsMap(@Nullable Integer topNDocumentsOnly, Boolean returnDocuments) {
        var map = new HashMap<String, Object>();

        if (topNDocumentsOnly != null) {
            map.put(VoyageAIRerankTaskSettings.TOP_K_DOCS_ONLY, topNDocumentsOnly.toString());
        }

        if (returnDocuments != null) {
            map.put(VoyageAIRerankTaskSettings.RETURN_DOCUMENTS, returnDocuments.toString());
        }

        return map;
    }
}
