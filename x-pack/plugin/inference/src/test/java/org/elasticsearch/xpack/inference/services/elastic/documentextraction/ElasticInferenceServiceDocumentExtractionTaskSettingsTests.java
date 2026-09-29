/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.documentextraction;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.elastic.documentextraction.ElasticInferenceServiceDocumentExtractionTaskSettings.EMPTY_SETTINGS;
import static org.elasticsearch.xpack.inference.services.elastic.documentextraction.ElasticInferenceServiceDocumentExtractionTaskSettings.OUTPUT_FORMAT;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ElasticInferenceServiceDocumentExtractionTaskSettingsTests extends AbstractBWCSerializationTestCase<
    ElasticInferenceServiceDocumentExtractionTaskSettings> {

    private static final TransportVersion INFERENCE_API_EIS_DOCUMENT_EXTRACTION_ADDED = TransportVersion.fromName(
        "inference_api_eis_document_extraction_added"
    );

    public void testFromMap_WithNullMap_ReturnsEmptySettings() {
        assertThat(ElasticInferenceServiceDocumentExtractionTaskSettings.fromMap(null), sameInstance(EMPTY_SETTINGS));
    }

    public void testFromMap_WithEmptyMap_ReturnsEmptySettings() {
        assertThat(ElasticInferenceServiceDocumentExtractionTaskSettings.fromMap(new HashMap<>()), sameInstance(EMPTY_SETTINGS));
    }

    public void testFromMap_ParsesOutputFormatAndRemovesItFromTheMap() {
        var map = new HashMap<String, Object>(Map.of(OUTPUT_FORMAT, "markdown"));

        var settings = ElasticInferenceServiceDocumentExtractionTaskSettings.fromMap(map);

        assertThat(settings.outputFormat(), is("markdown"));
        assertThat(map, anEmptyMap());
    }

    public void testFromMap_WithNonStringOutputFormat_Throws() {
        var map = new HashMap<String, Object>(Map.of(OUTPUT_FORMAT, 1));

        var exception = expectThrows(ValidationException.class, () -> ElasticInferenceServiceDocumentExtractionTaskSettings.fromMap(map));

        assertThat(
            exception.getMessage(),
            containsString("field [output_format] is not of the expected type. The value [1] cannot be converted to a [String]")
        );
    }

    public void testOf_RequestSettingsOverrideStoredSettings() {
        var stored = new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown");
        var request = new ElasticInferenceServiceDocumentExtractionTaskSettings("text");

        assertThat(ElasticInferenceServiceDocumentExtractionTaskSettings.of(stored, request).outputFormat(), is("text"));
    }

    public void testOf_FallsBackToStoredSettingsWhenRequestSettingsAreEmpty() {
        var stored = new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown");

        assertThat(ElasticInferenceServiceDocumentExtractionTaskSettings.of(stored, EMPTY_SETTINGS).outputFormat(), is("markdown"));
    }

    public void testUpdatedTaskSettings_ReplacesOutputFormat() {
        var settings = new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown");

        var updated = settings.updatedTaskSettings(new HashMap<>(Map.of(OUTPUT_FORMAT, "text")));

        assertThat(updated, is(new ElasticInferenceServiceDocumentExtractionTaskSettings("text")));
    }

    public void testUpdatedTaskSettings_WithEmptyMap_KeepsOutputFormat() {
        var settings = new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown");

        assertThat(settings.updatedTaskSettings(Map.of()), is(settings));
    }

    public void testIsEmpty() {
        assertTrue(EMPTY_SETTINGS.isEmpty());
        assertTrue(new ElasticInferenceServiceDocumentExtractionTaskSettings("").isEmpty());
        assertFalse(new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown").isEmpty());
    }

    public void testToXContent_WithEmptySettings_WritesEmptyObject() throws IOException {
        assertThat(Strings.toString(EMPTY_SETTINGS), is("{}"));
    }

    public void testToXContent_WritesOutputFormat() throws IOException {
        assertThat(Strings.toString(new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown")), is("""
            {"output_format":"markdown"}"""));
    }

    public void testEmptySettings_HaveNoOutputFormat() {
        assertThat(EMPTY_SETTINGS.outputFormat(), nullValue());
    }

    @Override
    protected Writeable.Reader<ElasticInferenceServiceDocumentExtractionTaskSettings> instanceReader() {
        return ElasticInferenceServiceDocumentExtractionTaskSettings::new;
    }

    @Override
    protected ElasticInferenceServiceDocumentExtractionTaskSettings createTestInstance() {
        return createRandom();
    }

    public static ElasticInferenceServiceDocumentExtractionTaskSettings createRandom() {
        return new ElasticInferenceServiceDocumentExtractionTaskSettings(randomBoolean() ? null : randomAlphaOfLengthBetween(1, 10));
    }

    @Override
    protected ElasticInferenceServiceDocumentExtractionTaskSettings mutateInstance(
        ElasticInferenceServiceDocumentExtractionTaskSettings instance
    ) {
        return new ElasticInferenceServiceDocumentExtractionTaskSettings(
            randomValueOtherThan(instance.outputFormat(), () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 10))
        );
    }

    @Override
    protected ElasticInferenceServiceDocumentExtractionTaskSettings mutateInstanceForVersion(
        ElasticInferenceServiceDocumentExtractionTaskSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    /**
     * The task settings were introduced together with the document extraction task type, so older versions cannot read them at all.
     */
    @Override
    protected Collection<TransportVersion> bwcVersions() {
        return super.bwcVersions().stream().filter(version -> version.supports(INFERENCE_API_EIS_DOCUMENT_EXTRACTION_ADDED)).toList();
    }

    @Override
    protected ElasticInferenceServiceDocumentExtractionTaskSettings doParseInstance(XContentParser parser) throws IOException {
        return ElasticInferenceServiceDocumentExtractionTaskSettings.fromMap(parser.map());
    }
}
