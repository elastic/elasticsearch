/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.documentextraction;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.elastic.documentextraction.ElasticInferenceServiceDocumentExtractionTaskSettings.CssSettings;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.elastic.documentextraction.ElasticInferenceServiceDocumentExtractionTaskSettings.CSS;
import static org.elasticsearch.xpack.inference.services.elastic.documentextraction.ElasticInferenceServiceDocumentExtractionTaskSettings.CssSettings.EXTRACT_ONLY;
import static org.elasticsearch.xpack.inference.services.elastic.documentextraction.ElasticInferenceServiceDocumentExtractionTaskSettings.CssSettings.REMOVE;
import static org.elasticsearch.xpack.inference.services.elastic.documentextraction.ElasticInferenceServiceDocumentExtractionTaskSettings.EMPTY_SETTINGS;
import static org.elasticsearch.xpack.inference.services.elastic.documentextraction.ElasticInferenceServiceDocumentExtractionTaskSettings.OUTPUT_FORMAT;
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
        assertThat(fromMap(null), sameInstance(EMPTY_SETTINGS));
    }

    public void testFromMap_WithEmptyMap_ReturnsEmptySettings() {
        assertThat(fromMap(new HashMap<>()), sameInstance(EMPTY_SETTINGS));
    }

    public void testFromMap_ParsesOutputFormat() {
        var settings = fromMap(Map.of(OUTPUT_FORMAT, "markdown"));

        assertThat(settings.outputFormat(), is("markdown"));
        assertThat(settings.css(), is(CssSettings.EMPTY));
    }

    public void testFromMap_ParsesCssSettings() {
        var settings = fromMap(Map.of(CSS, Map.of(EXTRACT_ONLY, List.of(".main-content", "#post-body"), REMOVE, List.of("nav"))));

        assertThat(settings.outputFormat(), nullValue());
        assertThat(settings.css(), is(new CssSettings(List.of(".main-content", "#post-body"), List.of("nav"))));
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

    public void testFromMap_WithNonObjectCss_Throws() {
        var exception = expectThrows(XContentParseException.class, () -> fromMap(Map.of(CSS, "nav")));

        assertThat(exception.getMessage(), containsString("css doesn't support values of type: VALUE_STRING"));
    }

    public void testFromMap_WithNonArraySelectors_Throws() {
        var exception = expectThrows(
            XContentParseException.class,
            () -> fromMap(Map.of(CSS, Map.of(EXTRACT_ONLY, Map.of("selector", ".main-content"))))
        );

        assertThat(exception.getMessage(), containsString("[task_settings] failed to parse field [css]"));
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

    public void testFromMap_WithUnknownCssField_ThrowsForRequestContext() {
        var exception = expectThrows(
            XContentParseException.class,
            () -> ElasticInferenceServiceDocumentExtractionTaskSettings.fromMap(
                Map.of(CSS, Map.of(EXTRACT_ONLY, List.of(".main-content"), "wait_for", List.of("#x"))),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(exception.getMessage(), containsString("[task_settings] failed to parse field [css]"));
        assertThat(exception.getCause().getMessage(), containsString("[css] unknown field [wait_for]"));
    }

    public void testFromMap_WithUnknownCssField_IgnoresItForPersistentContext() {
        var settings = ElasticInferenceServiceDocumentExtractionTaskSettings.fromMap(
            Map.of(CSS, Map.of(EXTRACT_ONLY, List.of(".main-content"), "wait_for", List.of("#x"))),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(settings.css(), is(new CssSettings(List.of(".main-content"), null)));
    }

    public void testFromMap_ForwardsSelectorsWithoutValidatingThem() {
        // Selector validation is left to the Elastic Inference Service, so empty lists and blank selectors are passed through
        var settings = fromMap(Map.of(CSS, Map.of(EXTRACT_ONLY, List.of(), REMOVE, List.of(".main-content", " "))));

        assertThat(settings.css(), is(new CssSettings(List.of(), List.of(".main-content", " "))));
    }

    public void testOf_RequestSettingsOverrideStoredSettings() {
        var stored = new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown", new CssSettings(List.of(".a"), List.of("nav")));
        var request = new ElasticInferenceServiceDocumentExtractionTaskSettings("text", new CssSettings(List.of(".b"), List.of("footer")));

        assertThat(ElasticInferenceServiceDocumentExtractionTaskSettings.of(stored, request), is(request));
    }

    public void testOf_FallsBackToStoredSettingsWhenRequestSettingsAreEmpty() {
        var stored = new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown", new CssSettings(List.of(".a"), List.of("nav")));

        assertThat(ElasticInferenceServiceDocumentExtractionTaskSettings.of(stored, EMPTY_SETTINGS), is(stored));
    }

    public void testOf_MergesCssSettingsPerField() {
        var stored = new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown", new CssSettings(List.of(".a"), List.of("nav")));
        var request = new ElasticInferenceServiceDocumentExtractionTaskSettings(null, new CssSettings(List.of(".b"), null));

        var merged = ElasticInferenceServiceDocumentExtractionTaskSettings.of(stored, request);

        assertThat(merged.outputFormat(), is("markdown"));
        assertThat(merged.css(), is(new CssSettings(List.of(".b"), List.of("nav"))));
    }

    public void testUpdatedTaskSettings_ReplacesOutputFormat() {
        var settings = new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown");

        var updated = settings.updatedTaskSettings(new HashMap<>(Map.of(OUTPUT_FORMAT, "text")));

        assertThat(updated, is(new ElasticInferenceServiceDocumentExtractionTaskSettings("text")));
    }

    public void testUpdatedTaskSettings_ReplacesCssSettingsAndKeepsOutputFormat() {
        var settings = new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown", new CssSettings(List.of(".a"), null));

        var updated = settings.updatedTaskSettings(new HashMap<>(Map.of(CSS, new HashMap<>(Map.of(REMOVE, List.of("nav"))))));

        assertThat(
            updated,
            is(new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown", new CssSettings(List.of(".a"), List.of("nav"))))
        );
    }

    public void testUpdatedTaskSettings_WithEmptyMap_KeepsSettings() {
        var settings = new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown", new CssSettings(List.of(".a"), null));

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
        assertFalse(new ElasticInferenceServiceDocumentExtractionTaskSettings(null, new CssSettings(List.of(".a"), null)).isEmpty());
        assertFalse(new ElasticInferenceServiceDocumentExtractionTaskSettings(null, new CssSettings(null, List.of("nav"))).isEmpty());
    }

    public void testEmptySettings_HaveNoOutputFormatAndNoCssSettings() {
        assertThat(EMPTY_SETTINGS.outputFormat(), nullValue());
        assertThat(EMPTY_SETTINGS.css(), is(CssSettings.EMPTY));
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
        return new ElasticInferenceServiceDocumentExtractionTaskSettings(randomOutputFormat(), randomCssSettings());
    }

    private static String randomOutputFormat() {
        return randomBoolean() ? null : randomAlphaOfLengthBetween(1, 10);
    }

    private static CssSettings randomCssSettings() {
        return new CssSettings(randomSelectors(), randomSelectors());
    }

    private static List<String> randomSelectors() {
        return randomBoolean() ? null : randomList(1, 3, () -> randomAlphaOfLengthBetween(1, 10));
    }

    @Override
    protected ElasticInferenceServiceDocumentExtractionTaskSettings mutateInstance(
        ElasticInferenceServiceDocumentExtractionTaskSettings instance
    ) {
        var outputFormat = instance.outputFormat();
        var css = instance.css();
        switch (randomInt(2)) {
            case 0 -> outputFormat = randomValueOtherThan(
                outputFormat,
                ElasticInferenceServiceDocumentExtractionTaskSettingsTests::randomOutputFormat
            );
            case 1 -> css = new CssSettings(
                randomValueOtherThan(css.extractOnly(), ElasticInferenceServiceDocumentExtractionTaskSettingsTests::randomSelectors),
                css.remove()
            );
            case 2 -> css = new CssSettings(
                css.extractOnly(),
                randomValueOtherThan(css.remove(), ElasticInferenceServiceDocumentExtractionTaskSettingsTests::randomSelectors)
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new ElasticInferenceServiceDocumentExtractionTaskSettings(outputFormat, css);
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
        return ElasticInferenceServiceDocumentExtractionTaskSettings.fromMap(parser.map(), ConfigurationParseContext.PERSISTENT);
    }
}
