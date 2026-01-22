/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.rerank;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class MixedbreadRerankTaskSettingsTests extends AbstractWireSerializingTestCase<MixedbreadRerankTaskSettings> {

    public static MixedbreadRerankTaskSettings createRandom() {
        var returnDocuments = randomOptionalBoolean();
        var topNDocsOnly = randomBoolean() ? randomIntBetween(1, 10) : null;

        return new MixedbreadRerankTaskSettings(topNDocsOnly, returnDocuments);
    }

    public void testFromMap_WithValidValues_ReturnsSettings() {
        Map<String, Object> taskMap = Map.of(
            MixedbreadRerankTaskSettings.RETURN_DOCUMENTS,
            true,
            MixedbreadRerankTaskSettings.TOP_N_DOCS_ONLY,
            5
        );
        var settings = MixedbreadRerankTaskSettings.fromMap(new HashMap<>(taskMap));
        assertTrue(settings.getReturnDocuments());
        assertEquals(5, settings.getTopNDocumentsOnly().intValue());
    }

    public void testFromMap_WithNullValues_ReturnsSettingsWithNulls() {
        var settings = MixedbreadRerankTaskSettings.fromMap(Map.of());
        assertNull(settings.getReturnDocuments());
        assertNull(settings.getTopNDocumentsOnly());
    }

    public void testFromMap_WithInvalidReturnDocuments_ThrowsValidationException() {
        Map<String, Object> taskMap = Map.of(
            MixedbreadRerankTaskSettings.RETURN_DOCUMENTS,
            "invalid",
            MixedbreadRerankTaskSettings.TOP_N_DOCS_ONLY,
            5
        );
        var thrownException = expectThrows(ValidationException.class, () -> MixedbreadRerankTaskSettings.fromMap(new HashMap<>(taskMap)));
        assertThat(thrownException.getMessage(), containsString("field [return_documents] is not of the expected type"));
    }

    public void testFromMap_WithInvalidTopNDocsOnly_ThrowsValidationException() {
        Map<String, Object> taskMap = Map.of(
            MixedbreadRerankTaskSettings.RETURN_DOCUMENTS,
            true,
            MixedbreadRerankTaskSettings.TOP_N_DOCS_ONLY,
            "invalid"
        );
        var thrownException = expectThrows(ValidationException.class, () -> MixedbreadRerankTaskSettings.fromMap(new HashMap<>(taskMap)));
        assertThat(thrownException.getMessage(), containsString("field [top_n] is not of the expected type"));
    }

    public void testUpdatedTaskSettings_WithEmptyMap_ReturnsSameSettings() {
        var initialSettings = new MixedbreadRerankTaskSettings(5, true);
        MixedbreadRerankTaskSettings updatedSettings = (MixedbreadRerankTaskSettings) initialSettings.updatedTaskSettings(Map.of());
        assertEquals(initialSettings, updatedSettings);
    }

    public void testUpdatedTaskSettings_WithNewReturnDocuments_ReturnsUpdatedSettings() {
        var initialSettings = new MixedbreadRerankTaskSettings(5, true);
        Map<String, Object> newSettings = Map.of(MixedbreadRerankTaskSettings.RETURN_DOCUMENTS, false);
        MixedbreadRerankTaskSettings updatedSettings = (MixedbreadRerankTaskSettings) initialSettings.updatedTaskSettings(newSettings);
        assertFalse(updatedSettings.getReturnDocuments());
        assertEquals(initialSettings.getTopNDocumentsOnly(), updatedSettings.getTopNDocumentsOnly());
    }

    public void testUpdatedTaskSettings_WithNewTopNDocsOnly_ReturnsUpdatedSettings() {
        var initialSettings = new MixedbreadRerankTaskSettings(5, true);
        Map<String, Object> newSettings = Map.of(MixedbreadRerankTaskSettings.TOP_N_DOCS_ONLY, 7);
        MixedbreadRerankTaskSettings updatedSettings = (MixedbreadRerankTaskSettings) initialSettings.updatedTaskSettings(newSettings);
        assertEquals(7, updatedSettings.getTopNDocumentsOnly().intValue());
        assertEquals(initialSettings.getReturnDocuments(), updatedSettings.getReturnDocuments());
    }

    public void testUpdatedTaskSettings_WithMultipleNewValues_ReturnsUpdatedSettings() {
        var initialSettings = new MixedbreadRerankTaskSettings(5, true);
        Map<String, Object> newSettings = Map.of(
            MixedbreadRerankTaskSettings.RETURN_DOCUMENTS,
            false,
            MixedbreadRerankTaskSettings.TOP_N_DOCS_ONLY,
            7
        );
        MixedbreadRerankTaskSettings updatedSettings = (MixedbreadRerankTaskSettings) initialSettings.updatedTaskSettings(newSettings);
        assertFalse(updatedSettings.getReturnDocuments());
        assertEquals(7, updatedSettings.getTopNDocumentsOnly().intValue());
    }

    @Override
    protected Writeable.Reader<MixedbreadRerankTaskSettings> instanceReader() {
        return MixedbreadRerankTaskSettings::new;
    }

    @Override
    protected MixedbreadRerankTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected MixedbreadRerankTaskSettings mutateInstance(MixedbreadRerankTaskSettings instance) throws IOException {
        var topNDocsOnly = instance.getTopNDocumentsOnly();
        var returnDocuments = instance.getReturnDocuments();
        switch (randomInt(1)) {
            case 0 -> topNDocsOnly = randomValueOtherThan(topNDocsOnly, () -> randomFrom(randomIntBetween(1, 10), null));
            case 1 -> returnDocuments = returnDocuments == null ? randomBoolean() : returnDocuments == false;
        }
        return new MixedbreadRerankTaskSettings(topNDocsOnly, returnDocuments);
    }

    public static Map<String, Object> getTaskSettingsMap(@Nullable Integer topNDocumentsOnly, Boolean returnDocuments) {
        var map = new HashMap<String, Object>();

        if (topNDocumentsOnly != null) {
            map.put(MixedbreadRerankTaskSettings.TOP_N_DOCS_ONLY, topNDocumentsOnly);
        }

        if (returnDocuments != null) {
            map.put(MixedbreadRerankTaskSettings.RETURN_DOCUMENTS, returnDocuments);
        }

        return map;
    }
}
