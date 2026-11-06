/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.documentextraction;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.elastic.documentextraction.ElasticInferenceServiceDocumentExtractionTaskSettings.EMPTY_SETTINGS;
import static org.elasticsearch.xpack.inference.services.elastic.documentextraction.ElasticInferenceServiceDocumentExtractionTaskSettings.OUTPUT_FORMAT;
import static org.elasticsearch.xpack.inference.services.elastic.documentextraction.ElasticInferenceServiceDocumentExtractionTaskSettings.PARAMETERS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ElasticInferenceServiceDocumentExtractionTaskSettingsTests extends AbstractBWCSerializationTestCase<
    ElasticInferenceServiceDocumentExtractionTaskSettings> {

    private static final TransportVersion INFERENCE_API_EIS_DOCUMENT_EXTRACTION_ADDED = TransportVersion.fromName(
        "inference_api_eis_document_extraction_added"
    );
    private static final TransportVersion INFERENCE_API_EIS_DOCUMENT_EXTRACTION_PARAMETERS = TransportVersion.fromName(
        "inference_api_eis_document_extraction_parameters"
    );

    public void testFromMap_WithNullMap_ReturnsEmptySettings() {
        assertThat(fromMap(null), sameInstance(EMPTY_SETTINGS));
    }

    public void testFromMap_WithEmptyMap_ReturnsEmptySettings() {
        assertThat(fromMap(new HashMap<>()), sameInstance(EMPTY_SETTINGS));
    }

    public void testFromMap_ParsesOutputFormat() {
        var settings = fromMap(Map.of(OUTPUT_FORMAT, "markdown"));

        assertThat(settings.outputFormat(), is("markdown"));
        assertThat(settings.parameters(), is(Map.of()));
    }

    public void testFromMap_WithNonStringOutputFormat_Throws() {
        var exception = expectThrows(XContentParseException.class, () -> fromMap(Map.of(OUTPUT_FORMAT, 1)));

        assertThat(exception.getMessage(), containsString("output_format doesn't support values of type: VALUE_NUMBER"));
    }

    public void testFromMap_WithEmptyOutputFormat_Throws() {
        var exception = expectThrows(ValidationException.class, () -> fromMap(Map.of(OUTPUT_FORMAT, "")));

        assertThat(
            exception.getMessage(),
            containsString("[task_settings] Invalid value empty string. [output_format] must be a non-empty string")
        );
    }

    public void testFromMap_ParsesParametersWithoutValidatingThem() {
        // Parameters are defined by the Elastic Inference Service, so arbitrary keys and nested values pass in both parse contexts.
        // The names and values below are made-up values only demonstrating the pass-through; they are not valid parameters.
        var parameters = Map.<String, Object>of(
            "preset",
            "ocr",
            "page",
            2,
            "with_alt",
            true,
            "selectors",
            List.of("nav", "footer"),
            "nested",
            Map.of("key", "value")
        );

        var settings = fromMap(Map.of(PARAMETERS, parameters));

        assertThat(settings.parameters(), is(parameters));
        assertThat(settings.outputFormat(), nullValue());
    }

    /**
     * Made-up parameters only demonstrating the pass-through; they are not valid parameters.
     * <p>
     * Parameters are free-form: embedded objects, arrays and arrays of objects are kept as they are, at any depth, so that they reach
     * the Elastic Inference Service unchanged.
     */
    private static Map<String, Object> embeddedParameters() {
        return Map.of(
            "preset",
            "ocr",
            "ocr",
            Map.of("preferEndToEnd", true, "layout", Map.of("columns", 2, "regions", List.of("header", "body"))),
            "targetSelector",
            List.of("main", Map.of("css", "#post", "weight", 1.5))
        );
    }

    public void testFromMap_KeepsEmbeddedObjectsAndArrays() {
        var settings = fromMap(Map.of(PARAMETERS, embeddedParameters()));

        assertThat(settings.parameters(), is(embeddedParameters()));
    }

    public void testSerialization_KeepsEmbeddedObjectsAndArrays() throws IOException {
        var settings = new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown", embeddedParameters());

        var copy = copyWriteable(settings, getNamedWriteableRegistry(), instanceReader(), TransportVersion.current());

        assertThat(copy.parameters(), is(embeddedParameters()));
    }

    public void testPersistedSettings_KeepEmbeddedObjectsAndArrays() throws IOException {
        // As stored on an inference endpoint: written as XContent, then read back in the persistent context
        var settings = new ElasticInferenceServiceDocumentExtractionTaskSettings(null, embeddedParameters());
        var stored = XContentHelper.convertToMap(
            BytesReference.bytes(settings.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)),
            false,
            XContentType.JSON
        ).v2();

        var restored = ElasticInferenceServiceDocumentExtractionTaskSettings.fromMap(stored, ConfigurationParseContext.PERSISTENT);

        assertThat(restored, is(settings));
    }

    public void testOf_ReplacesEmbeddedObjectsAsAWhole() {
        // The merge is per top-level parameter: an object set in the request replaces the stored one rather than being merged into it.
        // The parameters are made-up values only demonstrating the pass-through; they are not valid parameters.
        var stored = new ElasticInferenceServiceDocumentExtractionTaskSettings(
            null,
            Map.of("preset", "pdf", "ocr", Map.of("preferEndToEnd", true, "inlineFormula", true))
        );
        var request = new ElasticInferenceServiceDocumentExtractionTaskSettings(null, Map.of("ocr", Map.of("preferEndToEnd", false)));

        var merged = ElasticInferenceServiceDocumentExtractionTaskSettings.of(stored, request);

        assertThat(merged.parameters(), is(Map.of("preset", "pdf", "ocr", Map.of("preferEndToEnd", false))));
    }

    public void testFromMap_WithNonObjectParameters_Throws() {
        var exception = expectThrows(XContentParseException.class, () -> fromMap(Map.of(PARAMETERS, "page=2")));

        assertThat(exception.getMessage(), containsString("parameters doesn't support values of type: VALUE_STRING"));
    }

    public void testFromMap_WithUnknownField_ThrowsForRequestContext() {
        var exception = expectThrows(
            XContentParseException.class,
            () -> ElasticInferenceServiceDocumentExtractionTaskSettings.fromMap(
                Map.of(OUTPUT_FORMAT, "markdown", "unknown_field", "value"),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(exception.getMessage(), containsString("[task_settings] unknown field [unknown_field]"));
    }

    public void testFromMap_WithUnknownField_IgnoresItForPersistentContext() {
        var settings = ElasticInferenceServiceDocumentExtractionTaskSettings.fromMap(
            Map.of(OUTPUT_FORMAT, "markdown", "unknown_field", "value"),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(settings, is(new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown")));
    }

    public void testFromMap_WithTopLevelPreset_ThrowsForRequestContext() {
        // The preset is one of the parameters, not a task setting of its own
        var exception = expectThrows(
            XContentParseException.class,
            () -> ElasticInferenceServiceDocumentExtractionTaskSettings.fromMap(Map.of("preset", "ocr"), ConfigurationParseContext.REQUEST)
        );

        assertThat(exception.getMessage(), containsString("[task_settings] unknown field [preset]"));
    }

    public void testOf_RequestSettingsOverrideStoredSettings() {
        var stored = new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown", Map.of("preset", "pdf"));
        var request = new ElasticInferenceServiceDocumentExtractionTaskSettings("text", Map.of("preset", "ocr"));

        assertThat(ElasticInferenceServiceDocumentExtractionTaskSettings.of(stored, request), is(request));
    }

    public void testOf_FallsBackToStoredSettingsWhenRequestSettingsAreEmpty() {
        var stored = new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown", Map.of("preset", "pdf"));

        assertThat(ElasticInferenceServiceDocumentExtractionTaskSettings.of(stored, EMPTY_SETTINGS), is(stored));
    }

    public void testOf_MergesParametersPerKey() {
        var stored = new ElasticInferenceServiceDocumentExtractionTaskSettings(
            "markdown",
            Map.of("preset", "pdf", "retain_images", "none")
        );
        var request = new ElasticInferenceServiceDocumentExtractionTaskSettings(null, Map.of("preset", "ocr", "page", 3));

        var merged = ElasticInferenceServiceDocumentExtractionTaskSettings.of(stored, request);

        assertThat(merged.outputFormat(), is("markdown"));
        assertThat(merged.parameters(), is(Map.of("preset", "ocr", "page", 3, "retain_images", "none")));
    }

    public void testUpdatedTaskSettings_ReplacesOutputFormat() {
        var settings = new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown");

        var updated = settings.updatedTaskSettings(new HashMap<>(Map.of(OUTPUT_FORMAT, "text")));

        assertThat(updated, is(new ElasticInferenceServiceDocumentExtractionTaskSettings("text")));
    }

    public void testUpdatedTaskSettings_MergesParameters() {
        var settings = new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown", Map.of("preset", "pdf", "page", 1));

        var updated = settings.updatedTaskSettings(
            new HashMap<>(Map.of(PARAMETERS, new HashMap<>(Map.of("preset", "pdf+", "retain_images", "none"))))
        );

        assertThat(
            updated,
            is(
                new ElasticInferenceServiceDocumentExtractionTaskSettings(
                    "markdown",
                    Map.of("preset", "pdf+", "page", 1, "retain_images", "none")
                )
            )
        );
    }

    public void testUpdatedTaskSettings_WithEmptyMap_KeepsSettings() {
        var settings = new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown", Map.of("preset", "ocr"));

        assertThat(settings.updatedTaskSettings(new HashMap<>()), is(settings));
    }

    public void testUpdatedTaskSettings_WithUnknownField_Throws() {
        var settings = new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown");

        var exception = expectThrows(XContentParseException.class, () -> settings.updatedTaskSettings(Map.of("unknown_field", "value")));

        assertThat(exception.getMessage(), containsString("[task_settings] unknown field [unknown_field]"));
    }

    public void testIsEmpty() {
        assertTrue(EMPTY_SETTINGS.isEmpty());
        assertTrue(new ElasticInferenceServiceDocumentExtractionTaskSettings("").isEmpty());
        assertFalse(new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown").isEmpty());
        assertFalse(new ElasticInferenceServiceDocumentExtractionTaskSettings(null, Map.of("preset", "ocr")).isEmpty());
    }

    public void testEmptySettings_HaveNoValues() {
        assertThat(EMPTY_SETTINGS.outputFormat(), nullValue());
        assertThat(EMPTY_SETTINGS.parameters(), is(Map.of()));
    }

    private static ElasticInferenceServiceDocumentExtractionTaskSettings fromMap(@Nullable Map<String, Object> map) {
        return ElasticInferenceServiceDocumentExtractionTaskSettings.fromMap(map, randomFrom(ConfigurationParseContext.values()));
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
        return new ElasticInferenceServiceDocumentExtractionTaskSettings(randomOutputFormat(), randomParameters());
    }

    private static String randomOutputFormat() {
        return randomBoolean() ? null : randomAlphaOfLengthBetween(1, 10);
    }

    private static Map<String, Object> randomParameters() {
        return randomBoolean()
            ? Map.of()
            : randomMap(
                1,
                3,
                () -> Tuple.<String, Object>tuple(
                    randomAlphaOfLengthBetween(1, 10),
                    randomFrom(randomAlphaOfLength(5), randomInt(), randomBoolean())
                )
            );
    }

    @Override
    protected ElasticInferenceServiceDocumentExtractionTaskSettings mutateInstance(
        ElasticInferenceServiceDocumentExtractionTaskSettings instance
    ) {
        var outputFormat = instance.outputFormat();
        var parameters = instance.parameters();
        switch (randomInt(1)) {
            case 0 -> outputFormat = randomValueOtherThan(
                outputFormat,
                ElasticInferenceServiceDocumentExtractionTaskSettingsTests::randomOutputFormat
            );
            case 1 -> parameters = randomValueOtherThan(
                parameters,
                ElasticInferenceServiceDocumentExtractionTaskSettingsTests::randomParameters
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new ElasticInferenceServiceDocumentExtractionTaskSettings(outputFormat, parameters);
    }

    @Override
    protected ElasticInferenceServiceDocumentExtractionTaskSettings mutateInstanceForVersion(
        ElasticInferenceServiceDocumentExtractionTaskSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    /**
     * Versions before the parameters were added cannot read the task settings at all.
     */
    @Override
    protected Collection<TransportVersion> bwcVersions() {
        return super.bwcVersions().stream().filter(version -> version.supports(INFERENCE_API_EIS_DOCUMENT_EXTRACTION_PARAMETERS)).toList();
    }

    @Override
    protected ElasticInferenceServiceDocumentExtractionTaskSettings doParseInstance(XContentParser parser) throws IOException {
        return ElasticInferenceServiceDocumentExtractionTaskSettings.fromMap(parser.map(), ConfigurationParseContext.PERSISTENT);
    }
}
