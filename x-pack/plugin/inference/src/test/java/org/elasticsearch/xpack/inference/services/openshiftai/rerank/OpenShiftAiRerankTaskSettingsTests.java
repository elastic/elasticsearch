/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class OpenShiftAiRerankTaskSettingsTests extends AbstractBWCWireSerializationTestCase<OpenShiftAiRerankTaskSettings> {
    public static OpenShiftAiRerankTaskSettings createRandom() {
        var returnDocuments = randomOptionalBoolean();
        var topNDocsOnly = randomBoolean() ? randomIntBetween(1, 10) : null;

        return new OpenShiftAiRerankTaskSettings(topNDocsOnly, returnDocuments);
    }

    public void testFromMap_WithValidValues_ReturnsSettings() {
        Map<String, Object> taskMap = Map.of(OpenShiftAiRerankTaskSettings.RETURN_DOCUMENTS, true, OpenShiftAiRerankTaskSettings.TOP_N, 5);
        var settings = OpenShiftAiRerankTaskSettings.fromMap(new HashMap<>(taskMap));
        assertTrue(settings.getReturnDocuments());
        assertEquals(5, settings.getTopN().intValue());
    }

    public void testFromMap_WithNullValues_ReturnsSettingsWithNulls() {
        var settings = OpenShiftAiRerankTaskSettings.fromMap(Map.of());
        assertNull(settings.getReturnDocuments());
        assertNull(settings.getTopN());
    }

    public void testFromMap_WithInvalidReturnDocuments_ThrowsValidationException() {
        Map<String, Object> taskMap = Map.of(
            OpenShiftAiRerankTaskSettings.RETURN_DOCUMENTS,
            "invalid",
            OpenShiftAiRerankTaskSettings.TOP_N,
            5
        );
        var thrownException = expectThrows(ValidationException.class, () -> OpenShiftAiRerankTaskSettings.fromMap(new HashMap<>(taskMap)));
        assertThat(thrownException.getMessage(), containsString("field [return_documents] is not of the expected type"));
    }

    public void testFromMap_WithInvalidTopNDocsOnly_ThrowsValidationException() {
        Map<String, Object> taskMap = Map.of(
            OpenShiftAiRerankTaskSettings.RETURN_DOCUMENTS,
            true,
            OpenShiftAiRerankTaskSettings.TOP_N,
            "invalid"
        );
        var thrownException = expectThrows(ValidationException.class, () -> OpenShiftAiRerankTaskSettings.fromMap(new HashMap<>(taskMap)));
        assertThat(thrownException.getMessage(), containsString("field [top_n] is not of the expected type"));
    }

    public void UpdatedTaskSettings_WithEmptyMap_ReturnsSameSettings() {
        var initialSettings = new OpenShiftAiRerankTaskSettings(5, true);
        OpenShiftAiRerankTaskSettings updatedSettings = (OpenShiftAiRerankTaskSettings) initialSettings.updatedTaskSettings(Map.of());
        assertEquals(initialSettings, updatedSettings);
    }

    public void testUpdatedTaskSettings_WithNewReturnDocuments_ReturnsUpdatedSettings() {
        var initialSettings = new OpenShiftAiRerankTaskSettings(5, true);
        Map<String, Object> newSettings = Map.of(OpenShiftAiRerankTaskSettings.RETURN_DOCUMENTS, false);
        OpenShiftAiRerankTaskSettings updatedSettings = (OpenShiftAiRerankTaskSettings) initialSettings.updatedTaskSettings(newSettings);
        assertFalse(updatedSettings.getReturnDocuments());
        assertEquals(initialSettings.getTopN(), updatedSettings.getTopN());
    }

    public void testUpdatedTaskSettings_WithNewTopNDocsOnly_ReturnsUpdatedSettings() {
        var initialSettings = new OpenShiftAiRerankTaskSettings(5, true);
        Map<String, Object> newSettings = Map.of(OpenShiftAiRerankTaskSettings.TOP_N, 7);
        OpenShiftAiRerankTaskSettings updatedSettings = (OpenShiftAiRerankTaskSettings) initialSettings.updatedTaskSettings(newSettings);
        assertEquals(7, updatedSettings.getTopN().intValue());
        assertEquals(initialSettings.getReturnDocuments(), updatedSettings.getReturnDocuments());
    }

    public void testUpdatedTaskSettings_WithMultipleNewValues_ReturnsUpdatedSettings() {
        var initialSettings = new OpenShiftAiRerankTaskSettings(5, true);
        Map<String, Object> newSettings = Map.of(
            OpenShiftAiRerankTaskSettings.RETURN_DOCUMENTS,
            false,
            OpenShiftAiRerankTaskSettings.TOP_N,
            7
        );
        OpenShiftAiRerankTaskSettings updatedSettings = (OpenShiftAiRerankTaskSettings) initialSettings.updatedTaskSettings(newSettings);
        assertFalse(updatedSettings.getReturnDocuments());
        assertEquals(7, updatedSettings.getTopN().intValue());
    }

    @Override
    protected Writeable.Reader<OpenShiftAiRerankTaskSettings> instanceReader() {
        return OpenShiftAiRerankTaskSettings::new;
    }

    @Override
    protected OpenShiftAiRerankTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected OpenShiftAiRerankTaskSettings mutateInstance(OpenShiftAiRerankTaskSettings instance) throws IOException {
        return randomValueOtherThan(instance, OpenShiftAiRerankTaskSettingsTests::createRandom);
    }

    @Override
    protected OpenShiftAiRerankTaskSettings mutateInstanceForVersion(OpenShiftAiRerankTaskSettings instance, TransportVersion version) {
        return instance;
    }
}
