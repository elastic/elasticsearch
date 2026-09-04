/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class CohereRerankTaskSettingsTests extends AbstractBWCSerializationTestCase<CohereRerankTaskSettings> {

    private final boolean supportsUnknownFields = randomBoolean();

    public static CohereRerankTaskSettings createRandom() {
        var returnDocuments = randomOptionalBoolean();
        var topNDocsOnly = randomBoolean() ? randomIntBetween(1, 10) : null;
        var maxChunksPerDoc = randomBoolean() ? randomIntBetween(1, 20) : null;

        return new CohereRerankTaskSettings(topNDocsOnly, returnDocuments, maxChunksPerDoc);
    }

    public void testFromMap_WithValidValues_ReturnsSettings() {
        Map<String, Object> taskMap = Map.of(
            CohereRerankTaskSettings.RETURN_DOCUMENTS,
            true,
            CohereRerankTaskSettings.TOP_N_DOCS_ONLY,
            5,
            CohereRerankTaskSettings.MAX_CHUNKS_PER_DOC,
            10
        );
        var settings = CohereRerankTaskSettings.fromMap(new HashMap<>(taskMap), ConfigurationParseContext.REQUEST);
        assertTrue(settings.getReturnDocuments());
        assertEquals(5, settings.getTopNDocumentsOnly().intValue());
        assertEquals(10, settings.getMaxChunksPerDoc().intValue());
    }

    public void testFromMap_WithNullValues_ReturnsSettingsWithNulls() {
        var settings = CohereRerankTaskSettings.fromMap(Map.of(), ConfigurationParseContext.REQUEST);
        assertNull(settings.getReturnDocuments());
        assertNull(settings.getTopNDocumentsOnly());
        assertNull(settings.getMaxChunksPerDoc());
    }

    public void testFromMap_WithInvalidReturnDocuments_ThrowsException() {
        Map<String, Object> taskMap = Map.of(
            CohereRerankTaskSettings.RETURN_DOCUMENTS,
            "invalid",
            CohereRerankTaskSettings.TOP_N_DOCS_ONLY,
            5,
            CohereRerankTaskSettings.MAX_CHUNKS_PER_DOC,
            10
        );
        var thrownException = expectThrows(
            XContentParseException.class,
            () -> CohereRerankTaskSettings.fromMap(new HashMap<>(taskMap), ConfigurationParseContext.REQUEST)
        );
        assertThat(thrownException.getMessage(), containsString("return_documents"));
    }

    public void testFromMap_WithInvalidTopNDocsOnly_ThrowsException() {
        Map<String, Object> taskMap = Map.of(
            CohereRerankTaskSettings.RETURN_DOCUMENTS,
            true,
            CohereRerankTaskSettings.TOP_N_DOCS_ONLY,
            "invalid",
            CohereRerankTaskSettings.MAX_CHUNKS_PER_DOC,
            10
        );
        var thrownException = expectThrows(
            XContentParseException.class,
            () -> CohereRerankTaskSettings.fromMap(new HashMap<>(taskMap), ConfigurationParseContext.REQUEST)
        );
        assertThat(thrownException.getMessage(), containsString("top_n"));
    }

    public void testFromMap_WithInvalidMaxChunksPerDoc_ThrowsException() {
        Map<String, Object> taskMap = Map.of(
            CohereRerankTaskSettings.RETURN_DOCUMENTS,
            true,
            CohereRerankTaskSettings.TOP_N_DOCS_ONLY,
            5,
            CohereRerankTaskSettings.MAX_CHUNKS_PER_DOC,
            "invalid"
        );
        var thrownException = expectThrows(
            XContentParseException.class,
            () -> CohereRerankTaskSettings.fromMap(new HashMap<>(taskMap), ConfigurationParseContext.REQUEST)
        );
        assertThat(thrownException.getMessage(), containsString("max_chunks_per_doc"));
    }

    public void testFromMap_WithNonPositiveTopNDocsOnly_ThrowsException() {
        for (int value : new int[] { 0, randomNegativeInt() }) {
            Map<String, Object> taskMap = Map.of(CohereRerankTaskSettings.TOP_N_DOCS_ONLY, value);
            var thrownException = expectThrows(
                XContentParseException.class,
                () -> CohereRerankTaskSettings.fromMap(new HashMap<>(taskMap), ConfigurationParseContext.REQUEST)
            );
            assertThat(thrownException.getMessage(), containsString(CohereRerankTaskSettings.TOP_N_DOCS_ONLY));
        }
    }

    public void testFromMap_WithNonPositiveMaxChunksPerDoc_ThrowsException() {
        for (int value : new int[] { 0, randomNegativeInt() }) {
            Map<String, Object> taskMap = Map.of(CohereRerankTaskSettings.MAX_CHUNKS_PER_DOC, value);
            var thrownException = expectThrows(
                XContentParseException.class,
                () -> CohereRerankTaskSettings.fromMap(new HashMap<>(taskMap), ConfigurationParseContext.REQUEST)
            );
            assertThat(thrownException.getMessage(), containsString(CohereRerankTaskSettings.MAX_CHUNKS_PER_DOC));
        }
    }

    public void UpdatedTaskSettings_WithEmptyMap_ReturnsSameSettings() {
        var initialSettings = new CohereRerankTaskSettings(5, true, 10);
        CohereRerankTaskSettings updatedSettings = (CohereRerankTaskSettings) initialSettings.updatedTaskSettings(new HashMap<>());
        assertEquals(initialSettings, updatedSettings);
    }

    public void testUpdatedTaskSettings_WithNewReturnDocuments_ReturnsUpdatedSettings() {
        var initialSettings = new CohereRerankTaskSettings(5, true, 10);
        var newSettings = new HashMap<String, Object>(Map.of(CohereRerankTaskSettings.RETURN_DOCUMENTS, false));
        CohereRerankTaskSettings updatedSettings = (CohereRerankTaskSettings) initialSettings.updatedTaskSettings(newSettings);
        assertFalse(updatedSettings.getReturnDocuments());
        assertEquals(initialSettings.getTopNDocumentsOnly(), updatedSettings.getTopNDocumentsOnly());
        assertEquals(initialSettings.getMaxChunksPerDoc(), updatedSettings.getMaxChunksPerDoc());
    }

    public void testUpdatedTaskSettings_WithNewTopNDocsOnly_ReturnsUpdatedSettings() {
        var initialSettings = new CohereRerankTaskSettings(5, true, 10);
        var newSettings = new HashMap<String, Object>(Map.of(CohereRerankTaskSettings.TOP_N_DOCS_ONLY, 7));
        CohereRerankTaskSettings updatedSettings = (CohereRerankTaskSettings) initialSettings.updatedTaskSettings(newSettings);
        assertEquals(7, updatedSettings.getTopNDocumentsOnly().intValue());
        assertEquals(initialSettings.getReturnDocuments(), updatedSettings.getReturnDocuments());
        assertEquals(initialSettings.getMaxChunksPerDoc(), updatedSettings.getMaxChunksPerDoc());
    }

    public void testUpdatedTaskSettings_WithNewMaxChunksPerDoc_ReturnsUpdatedSettings() {
        var initialSettings = new CohereRerankTaskSettings(5, true, 10);
        var newSettings = new HashMap<String, Object>(Map.of(CohereRerankTaskSettings.MAX_CHUNKS_PER_DOC, 15));
        CohereRerankTaskSettings updatedSettings = (CohereRerankTaskSettings) initialSettings.updatedTaskSettings(newSettings);
        assertEquals(15, updatedSettings.getMaxChunksPerDoc().intValue());
        assertEquals(initialSettings.getReturnDocuments(), updatedSettings.getReturnDocuments());
        assertEquals(initialSettings.getTopNDocumentsOnly(), updatedSettings.getTopNDocumentsOnly());
    }

    public void testUpdatedTaskSettings_WithMultipleNewValues_ReturnsUpdatedSettings() {
        var initialSettings = new CohereRerankTaskSettings(5, true, 10);
        var newSettings = new HashMap<String, Object>(
            Map.of(
                CohereRerankTaskSettings.RETURN_DOCUMENTS,
                false,
                CohereRerankTaskSettings.TOP_N_DOCS_ONLY,
                7,
                CohereRerankTaskSettings.MAX_CHUNKS_PER_DOC,
                15
            )
        );
        CohereRerankTaskSettings updatedSettings = (CohereRerankTaskSettings) initialSettings.updatedTaskSettings(newSettings);
        assertFalse(updatedSettings.getReturnDocuments());
        assertEquals(7, updatedSettings.getTopNDocumentsOnly().intValue());
        assertEquals(15, updatedSettings.getMaxChunksPerDoc().intValue());
    }

    @Override
    protected Writeable.Reader<CohereRerankTaskSettings> instanceReader() {
        return CohereRerankTaskSettings::new;
    }

    @Override
    protected CohereRerankTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected CohereRerankTaskSettings mutateInstance(CohereRerankTaskSettings instance) throws IOException {
        var topNDocsOnly = instance.getTopNDocumentsOnly();
        var returnDocuments = instance.getReturnDocuments();
        var maxChunksPerDoc = instance.getMaxChunksPerDoc();
        switch (randomInt(2)) {
            case 0 -> topNDocsOnly = randomValueOtherThan(topNDocsOnly, () -> randomFrom(randomIntBetween(1, 10), null));
            case 1 -> returnDocuments = returnDocuments == null ? randomBoolean() : returnDocuments == false;
            case 2 -> maxChunksPerDoc = randomValueOtherThan(maxChunksPerDoc, () -> randomFrom(randomIntBetween(1, 20), null));
        }
        return new CohereRerankTaskSettings(topNDocsOnly, returnDocuments, maxChunksPerDoc);
    }

    @Override
    protected CohereRerankTaskSettings doParseInstance(XContentParser parser) throws IOException {
        return CohereRerankTaskSettings.createParser(supportsUnknownFields).apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return supportsUnknownFields;
    }

    @Override
    protected CohereRerankTaskSettings mutateInstanceForVersion(CohereRerankTaskSettings instance, TransportVersion version) {
        return instance;
    }
}
