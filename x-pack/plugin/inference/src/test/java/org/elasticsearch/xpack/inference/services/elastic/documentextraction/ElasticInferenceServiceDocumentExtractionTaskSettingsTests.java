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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;
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
        assertThat(fromMap(null), sameInstance(EMPTY_SETTINGS));
    }

    public void testFromMap_WithEmptyMap_ReturnsEmptySettings() {
        assertThat(fromMap(new HashMap<>()), sameInstance(EMPTY_SETTINGS));
    }

    public void testFromMap_ParsesOutputFormatAndRemovesItFromTheMap() {
        var map = new HashMap<String, Object>(Map.of(OUTPUT_FORMAT, "markdown"));

        var settings = fromMap(map);

        assertThat(settings.outputFormat(), is("markdown"));
        assertThat(settings.css(), is(CssSettings.EMPTY));
        assertThat(map, anEmptyMap());
    }

    public void testFromMap_ParsesCssSettingsAndRemovesThemFromTheMap() {
        var map = new HashMap<String, Object>(
            Map.of(CSS, Map.of(EXTRACT_ONLY, List.of(".main-content", "#post-body"), REMOVE, List.of("nav")))
        );

        var settings = fromMap(map);

        assertThat(settings.outputFormat(), nullValue());
        assertThat(settings.css(), is(new CssSettings(List.of(".main-content", "#post-body"), List.of("nav"))));
        assertThat(map, anEmptyMap());
    }

    public void testFromMap_WithImmutableNestedCssMap_DoesNotThrow() {
        // The request task settings arrive as parsed, immutable maps; the nested css object must not be mutated in place
        var map = new HashMap<String, Object>(Map.of(CSS, Map.of(EXTRACT_ONLY, List.of(".main-content"))));

        var settings = fromMap(map);

        assertThat(settings.css().extractOnly(), is(List.of(".main-content")));
    }

    public void testFromMap_WithNonStringOutputFormat_Throws() {
        var map = new HashMap<String, Object>(Map.of(OUTPUT_FORMAT, 1));

        var exception = expectThrows(ValidationException.class, () -> fromMap(map));

        assertThat(
            exception.getMessage(),
            containsString("field [output_format] is not of the expected type. The value [1] cannot be converted to a [String]")
        );
    }

    public void testFromMap_WithEmptyOutputFormat_Throws() {
        var map = new HashMap<String, Object>(Map.of(OUTPUT_FORMAT, ""));

        var exception = expectThrows(ValidationException.class, () -> fromMap(map));

        assertThat(
            exception.getMessage(),
            containsString("[task_settings] Invalid value empty string. [output_format] must be a non-empty string")
        );
    }

    public void testFromMap_WithNonObjectCss_Throws() {
        var map = new HashMap<String, Object>(Map.of(CSS, "nav"));

        var exception = expectThrows(ValidationException.class, () -> fromMap(map));

        assertThat(exception.getMessage(), containsString("field [css] is not of the expected type"));
    }

    public void testFromMap_WithNonStringSelector_Throws() {
        var map = new HashMap<String, Object>(Map.of(CSS, Map.of(EXTRACT_ONLY, List.of(1))));

        var exception = expectThrows(ValidationException.class, () -> fromMap(map));

        assertThat(exception.getMessage(), containsString("field [extract_only] is not of the expected type"));
    }

    public void testFromMap_ForwardsSelectorsWithoutValidatingThem() {
        // Selector validation is left to the Elastic Inference Service, so empty lists and blank selectors are passed through
        var map = new HashMap<String, Object>(Map.of(CSS, Map.of(EXTRACT_ONLY, List.of(), REMOVE, List.of(".main-content", " "))));

        var settings = fromMap(map);

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

    public void testUpdatedTaskSettings_WithImmutableMap_ReplacesCssSettingsAndKeepsOutputFormat() {
        var settings = new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown", new CssSettings(List.of(".a"), null));

        var updated = settings.updatedTaskSettings(Map.of(CSS, Map.of(REMOVE, List.of("nav"))));

        assertThat(
            updated,
            is(new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown", new CssSettings(List.of(".a"), List.of("nav"))))
        );
    }

    public void testUpdatedTaskSettings_WithEmptyMap_KeepsSettings() {
        var settings = new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown", new CssSettings(List.of(".a"), null));

        assertThat(settings.updatedTaskSettings(Map.of()), is(settings));
    }

    public void testIsEmpty() {
        assertTrue(EMPTY_SETTINGS.isEmpty());
        assertTrue(new ElasticInferenceServiceDocumentExtractionTaskSettings("").isEmpty());
        assertFalse(new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown").isEmpty());
        assertFalse(new ElasticInferenceServiceDocumentExtractionTaskSettings(null, new CssSettings(List.of(".a"), null)).isEmpty());
        assertFalse(new ElasticInferenceServiceDocumentExtractionTaskSettings(null, new CssSettings(null, List.of("nav"))).isEmpty());
    }

    public void testToXContent_WithEmptySettings_WritesEmptyObject() throws IOException {
        assertThat(Strings.toString(EMPTY_SETTINGS), is("{}"));
    }

    public void testToXContent_WritesOutputFormat() throws IOException {
        assertThat(Strings.toString(new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown")), is("""
            {"output_format":"markdown"}"""));
    }

    public void testToXContent_WritesCssSettings() throws IOException {
        var settings = new ElasticInferenceServiceDocumentExtractionTaskSettings(
            "markdown",
            new CssSettings(List.of(".main-content", "#post-body"), List.of("nav"))
        );

        assertThat(Strings.toString(settings), is("""
            {"output_format":"markdown","css":{"extract_only":[".main-content","#post-body"],"remove":["nav"]}}"""));
    }

    public void testToXContent_OmitsUnsetCssSelectors() throws IOException {
        var settings = new ElasticInferenceServiceDocumentExtractionTaskSettings(null, new CssSettings(null, List.of("nav")));

        assertThat(Strings.toString(settings), is("""
            {"css":{"remove":["nav"]}}"""));
    }

    public void testEmptySettings_HaveNoOutputFormatAndNoCssSettings() {
        assertThat(EMPTY_SETTINGS.outputFormat(), nullValue());
        assertThat(EMPTY_SETTINGS.css(), is(CssSettings.EMPTY));
    }

    private static ElasticInferenceServiceDocumentExtractionTaskSettings fromMap(@Nullable Map<String, Object> map) {
        return ElasticInferenceServiceDocumentExtractionTaskSettings.fromMap(map);
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
        return ElasticInferenceServiceDocumentExtractionTaskSettings.fromMap(parser.map());
    }
}
