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

import static org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadServiceTests.RETURN_DOCUMENTS_FALSE;
import static org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadServiceTests.RETURN_DOCUMENTS_TRUE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class MixedbreadRerankTaskSettingsTests extends AbstractWireSerializingTestCase<MixedbreadRerankTaskSettings> {
    private static final int TOP_N = 7;
    private static final int TOP_N_UPDATE_VALUE = 8;

    public static MixedbreadRerankTaskSettings createRandom() {
        var returnDocuments = randomOptionalBoolean();
        var topNDocsOnly = randomBoolean() ? randomIntBetween(1, 10) : null;

        return new MixedbreadRerankTaskSettings(topNDocsOnly, returnDocuments);
    }

    public void testFromMap_WithValidValues_ReturnsSettings() {
        Map<String, Object> taskMap = Map.of(
            MixedbreadRerankTaskSettings.RETURN_DOCUMENTS,
            RETURN_DOCUMENTS_TRUE,
            MixedbreadRerankTaskSettings.TOP_N,
            TOP_N
        );
        var settings = MixedbreadRerankTaskSettings.fromMap(new HashMap<>(taskMap));
        assertTrue(settings.getReturnDocuments());
        assertEquals(TOP_N, settings.getTopN().intValue());
    }

    public void testFromMap_WithNullValues_ReturnsSettingsWithNulls() {
        var settings = MixedbreadRerankTaskSettings.fromMap(Map.of());
        assertNull(settings.getReturnDocuments());
        assertNull(settings.getTopN());
    }

    public void testFromMap_WithInvalidReturnDocuments_ThrowsValidationException() {
        Map<String, Object> taskMap = Map.of(
            MixedbreadRerankTaskSettings.RETURN_DOCUMENTS,
            "invalid",
            MixedbreadRerankTaskSettings.TOP_N,
            TOP_N
        );
        var thrownException = expectThrows(ValidationException.class, () -> MixedbreadRerankTaskSettings.fromMap(new HashMap<>(taskMap)));
        assertThat(thrownException.getMessage(), containsString("field [return_documents] is not of the expected type"));
    }

    public void testFromMap_WithInvalidTopNDocsOnly_ThrowsValidationException() {
        Map<String, Object> taskMap = Map.of(
            MixedbreadRerankTaskSettings.RETURN_DOCUMENTS,
            RETURN_DOCUMENTS_TRUE,
            MixedbreadRerankTaskSettings.TOP_N,
            "invalid"
        );
        var thrownException = expectThrows(ValidationException.class, () -> MixedbreadRerankTaskSettings.fromMap(new HashMap<>(taskMap)));
        assertThat(thrownException.getMessage(), containsString("field [top_n] is not of the expected type"));
    }

    public void testUpdatedTaskSettings_WithEmptyMap_ReturnsSameSettings() {
        var initialSettings = new MixedbreadRerankTaskSettings(TOP_N, RETURN_DOCUMENTS_TRUE);
        MixedbreadRerankTaskSettings updatedSettings = initialSettings.updatedTaskSettings(Map.of());
        assertThat(initialSettings, is(sameInstance(updatedSettings)));
    }

    public void testUpdatedTaskSettings_WithNewReturnDocuments_ReturnsUpdatedSettings() {
        var initialSettings = new MixedbreadRerankTaskSettings(TOP_N, RETURN_DOCUMENTS_TRUE);
        Map<String, Object> newSettings = Map.of(MixedbreadRerankTaskSettings.RETURN_DOCUMENTS, RETURN_DOCUMENTS_FALSE);
        MixedbreadRerankTaskSettings updatedSettings = initialSettings.updatedTaskSettings(newSettings);
        assertFalse(updatedSettings.getReturnDocuments());
        assertEquals(initialSettings.getTopN(), updatedSettings.getTopN());
    }

    public void testUpdatedTaskSettings_WithNewTopNDocsOnly_ReturnsUpdatedSettings() {
        var initialSettings = new MixedbreadRerankTaskSettings(TOP_N, RETURN_DOCUMENTS_TRUE);
        Map<String, Object> newSettings = Map.of(MixedbreadRerankTaskSettings.TOP_N, TOP_N_UPDATE_VALUE);
        MixedbreadRerankTaskSettings updatedSettings = initialSettings.updatedTaskSettings(newSettings);
        assertEquals(TOP_N_UPDATE_VALUE, updatedSettings.getTopN().intValue());
        assertEquals(initialSettings.getReturnDocuments(), updatedSettings.getReturnDocuments());
    }

    public void testUpdatedTaskSettings_WithMultipleNewValues_ReturnsUpdatedSettings() {
        var initialSettings = new MixedbreadRerankTaskSettings(TOP_N, RETURN_DOCUMENTS_TRUE);
        Map<String, Object> newSettings = Map.of(
            MixedbreadRerankTaskSettings.RETURN_DOCUMENTS,
            RETURN_DOCUMENTS_FALSE,
            MixedbreadRerankTaskSettings.TOP_N,
            TOP_N_UPDATE_VALUE
        );
        MixedbreadRerankTaskSettings updatedSettings = initialSettings.updatedTaskSettings(newSettings);
        assertFalse(updatedSettings.getReturnDocuments());
        assertEquals(TOP_N_UPDATE_VALUE, updatedSettings.getTopN().intValue());
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
        var topNDocsOnly = instance.getTopN();
        var returnDocuments = instance.getReturnDocuments();
        switch (randomInt(1)) {
            case 0 -> topNDocsOnly = randomValueOtherThan(topNDocsOnly, () -> randomFrom(randomIntBetween(1, 10), null));
            case 1 -> returnDocuments = returnDocuments == null ? randomBoolean() : returnDocuments == false;
        }
        return new MixedbreadRerankTaskSettings(topNDocsOnly, returnDocuments);
    }

    public static Map<String, Object> getTaskSettingsMap(@Nullable Integer topN, Boolean returnDocuments) {
        var map = new HashMap<String, Object>();

        if (topN != null) {
            map.put(MixedbreadRerankTaskSettings.TOP_N, topN);
        }

        if (returnDocuments != null) {
            map.put(MixedbreadRerankTaskSettings.RETURN_DOCUMENTS, returnDocuments);
        }

        return map;
    }
}
