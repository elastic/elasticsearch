/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.documentextraction;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.elastic.documentextraction.ElasticInferenceServiceDocumentExtractionTaskSettings.EMPTY_SETTINGS;
import static org.elasticsearch.xpack.inference.services.elastic.documentextraction.ElasticInferenceServiceDocumentExtractionTaskSettings.OUTPUT_FORMAT;
import static org.elasticsearch.xpack.inference.services.elastic.documentextraction.ElasticInferenceServiceDocumentExtractionTaskSettings.PARAMETERS;
import static org.elasticsearch.xpack.inference.services.elastic.documentextraction.ElasticInferenceServiceDocumentExtractionTaskSettings.PRESET;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ElasticInferenceServiceDocumentExtractionTaskSettingsTests extends AbstractBWCSerializationTestCase<
    ElasticInferenceServiceDocumentExtractionTaskSettings> {

    private static final TransportVersion INFERENCE_API_EIS_DOCUMENT_EXTRACTION_ADDED = TransportVersion.fromName(
        "inference_api_eis_document_extraction_added"
    );
    private static final TransportVersion INFERENCE_API_EIS_DOCUMENT_EXTRACTION_PRESET_PARAMETERS = TransportVersion.fromName(
        "inference_api_eis_document_extraction_preset_parameters"
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
        assertThat(settings.preset(), nullValue());
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

    public void testFromMap_ParsesPreset() {
        var settings = fromMap(Map.of(PRESET, "ocr"));

        assertThat(settings.preset(), is("ocr"));
        assertThat(settings.outputFormat(), nullValue());
        assertThat(settings.parameters(), is(Map.of()));
    }

    public void testFromMap_WithEmptyPreset_Throws() {
        var exception = expectThrows(ValidationException.class, () -> fromMap(Map.of(PRESET, "")));

        assertThat(
            exception.getMessage(),
            containsString("[task_settings] Invalid value empty string. [preset] must be a non-empty string")
        );
    }

    public void testFromMap_WithEmptyOutputFormatAndPreset_ReportsBoth() {
        var exception = expectThrows(ValidationException.class, () -> fromMap(Map.of(OUTPUT_FORMAT, "", PRESET, "")));

        assertThat(exception.validationErrors().size(), is(2));
    }

    public void testFromMap_WithNonStringPreset_Throws() {
        var exception = expectThrows(XContentParseException.class, () -> fromMap(Map.of(PRESET, 1)));

        assertThat(exception.getMessage(), containsString("preset doesn't support values of type: VALUE_NUMBER"));
    }

    public void testFromMap_ParsesParametersWithoutValidatingThem() {
        // Parameters are defined by the Elastic Inference Service, so arbitrary keys and nested values pass in both parse contexts
        var parameters = Map.<String, Object>of(
            "page",
            2,
            "retain_images",
            "none",
            "with_alt",
            true,
            "selectors",
            List.of("nav", "footer"),
            "nested",
            Map.of("key", "value")
        );

        var settings = fromMap(Map.of(PARAMETERS, parameters));

        assertThat(settings.parameters(), is(parameters));
        assertThat(settings.preset(), nullValue());
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

    public void testFromMap_WithRemovedCssSettings_ThrowsForRequestContext() {
        var exception = expectThrows(
            XContentParseException.class,
            () -> ElasticInferenceServiceDocumentExtractionTaskSettings.fromMap(
                Map.of("css", Map.of("extract_only", List.of(".main-content"))),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(exception.getMessage(), containsString("[task_settings] unknown field [css]"));
    }

    public void testFromMap_WithRemovedCssSettings_IgnoresThemForPersistentContext() {
        // Endpoints stored by an earlier version may still carry css settings
        var settings = ElasticInferenceServiceDocumentExtractionTaskSettings.fromMap(
            Map.of(OUTPUT_FORMAT, "markdown", "css", Map.of("extract_only", List.of(".main-content"), "remove", List.of("nav"))),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(settings, is(new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown")));
    }

    public void testOf_RequestSettingsOverrideStoredSettings() {
        var stored = new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown", "pdf", Map.of("page", 1));
        var request = new ElasticInferenceServiceDocumentExtractionTaskSettings("text", "ocr", Map.of("page", 2));

        assertThat(ElasticInferenceServiceDocumentExtractionTaskSettings.of(stored, request), is(request));
    }

    public void testOf_FallsBackToStoredSettingsWhenRequestSettingsAreEmpty() {
        var stored = new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown", "pdf", Map.of("page", 1));

        assertThat(ElasticInferenceServiceDocumentExtractionTaskSettings.of(stored, EMPTY_SETTINGS), is(stored));
    }

    public void testOf_MergesPerField() {
        var stored = new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown", "pdf", Map.of());
        var request = new ElasticInferenceServiceDocumentExtractionTaskSettings(null, "ocr", Map.of());

        var merged = ElasticInferenceServiceDocumentExtractionTaskSettings.of(stored, request);

        assertThat(merged.outputFormat(), is("markdown"));
        assertThat(merged.preset(), is("ocr"));
    }

    public void testOf_MergesParametersPerKey() {
        var stored = new ElasticInferenceServiceDocumentExtractionTaskSettings(null, null, Map.of("page", 1, "retain_images", "none"));
        var request = new ElasticInferenceServiceDocumentExtractionTaskSettings(null, null, Map.of("page", 3, "with_alt", true));

        var merged = ElasticInferenceServiceDocumentExtractionTaskSettings.of(stored, request);

        assertThat(merged.parameters(), is(Map.of("page", 3, "retain_images", "none", "with_alt", true)));
    }

    public void testUpdatedTaskSettings_ReplacesOutputFormat() {
        var settings = new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown");

        var updated = settings.updatedTaskSettings(new HashMap<>(Map.of(OUTPUT_FORMAT, "text")));

        assertThat(updated, is(new ElasticInferenceServiceDocumentExtractionTaskSettings("text")));
    }

    public void testUpdatedTaskSettings_MergesPresetAndParameters() {
        var settings = new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown", "pdf", Map.of("page", 1));

        var updated = settings.updatedTaskSettings(
            new HashMap<>(Map.of(PRESET, "pdf+", PARAMETERS, new HashMap<>(Map.of("retain_images", "none"))))
        );

        assertThat(
            updated,
            is(new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown", "pdf+", Map.of("page", 1, "retain_images", "none")))
        );
    }

    public void testUpdatedTaskSettings_WithEmptyMap_KeepsSettings() {
        var settings = new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown", "ocr", Map.of("page", 1));

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
        assertTrue(new ElasticInferenceServiceDocumentExtractionTaskSettings(null, "", Map.of()).isEmpty());
        assertFalse(new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown").isEmpty());
        assertFalse(new ElasticInferenceServiceDocumentExtractionTaskSettings(null, "ocr", Map.of()).isEmpty());
        assertFalse(new ElasticInferenceServiceDocumentExtractionTaskSettings(null, null, Map.of("page", 2)).isEmpty());
    }

    public void testEmptySettings_HaveNoValues() {
        assertThat(EMPTY_SETTINGS.outputFormat(), nullValue());
        assertThat(EMPTY_SETTINGS.preset(), nullValue());
        assertThat(EMPTY_SETTINGS.parameters(), is(Map.of()));
    }

    /**
     * Nodes on a version before the preset and parameters were added serialize css settings, which must be read and discarded.
     */
    public void testReadFromEarlierVersion_DiscardsCssSettings() throws IOException {
        var earlierVersion = TransportVersionUtils.getPreviousVersion(INFERENCE_API_EIS_DOCUMENT_EXTRACTION_PRESET_PARAMETERS);
        assertTrue(earlierVersion.supports(INFERENCE_API_EIS_DOCUMENT_EXTRACTION_ADDED));

        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(earlierVersion);
            out.writeOptionalString("markdown");
            out.writeOptionalStringCollection(List.of(".main-content"));
            out.writeOptionalStringCollection(List.of("nav"));

            try (var in = out.bytes().streamInput()) {
                in.setTransportVersion(earlierVersion);
                var settings = new ElasticInferenceServiceDocumentExtractionTaskSettings(in);

                assertThat(settings, is(new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown")));
                assertThat(in.available(), is(0));
            }
        }
    }

    /**
     * Nodes on a version before the preset and parameters were added expect css settings, which are written as unset.
     */
    public void testWriteToEarlierVersion_WritesUnsetCssSettings() throws IOException {
        var earlierVersion = TransportVersionUtils.getPreviousVersion(INFERENCE_API_EIS_DOCUMENT_EXTRACTION_PRESET_PARAMETERS);
        var settings = new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown", "ocr", Map.of("page", 2));

        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(earlierVersion);
            settings.writeTo(out);

            try (var in = out.bytes().streamInput()) {
                in.setTransportVersion(earlierVersion);
                assertThat(in.readOptionalString(), is("markdown"));
                assertThat(in.readOptionalStringCollectionAsList(), nullValue());
                assertThat(in.readOptionalStringCollectionAsList(), nullValue());
                assertThat(in.available(), is(0));
            }
        }
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
        return new ElasticInferenceServiceDocumentExtractionTaskSettings(randomOutputFormat(), randomPreset(), randomParameters());
    }

    private static String randomOutputFormat() {
        return randomBoolean() ? null : randomAlphaOfLengthBetween(1, 10);
    }

    private static String randomPreset() {
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
        var preset = instance.preset();
        var parameters = instance.parameters();
        switch (randomInt(2)) {
            case 0 -> outputFormat = randomValueOtherThan(
                outputFormat,
                ElasticInferenceServiceDocumentExtractionTaskSettingsTests::randomOutputFormat
            );
            case 1 -> preset = randomValueOtherThan(preset, ElasticInferenceServiceDocumentExtractionTaskSettingsTests::randomPreset);
            case 2 -> parameters = randomValueOtherThan(
                parameters,
                ElasticInferenceServiceDocumentExtractionTaskSettingsTests::randomParameters
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new ElasticInferenceServiceDocumentExtractionTaskSettings(outputFormat, preset, parameters);
    }

    @Override
    protected ElasticInferenceServiceDocumentExtractionTaskSettings mutateInstanceForVersion(
        ElasticInferenceServiceDocumentExtractionTaskSettings instance,
        TransportVersion version
    ) {
        if (version.supports(INFERENCE_API_EIS_DOCUMENT_EXTRACTION_PRESET_PARAMETERS)) {
            return instance;
        }
        // Earlier versions drop the preset and the parameters
        return new ElasticInferenceServiceDocumentExtractionTaskSettings(instance.outputFormat());
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
        return ElasticInferenceServiceDocumentExtractionTaskSettings.fromMap(parser.map(), ConfigurationParseContext.PERSISTENT);
    }
}
