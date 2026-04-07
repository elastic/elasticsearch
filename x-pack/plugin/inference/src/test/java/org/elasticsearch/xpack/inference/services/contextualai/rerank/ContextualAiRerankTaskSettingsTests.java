/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankTaskSettings.EMPTY_SETTINGS;
import static org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankTaskSettings.INSTRUCTION_FIELD;
import static org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankTaskSettings.TOP_N_FIELD;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class ContextualAiRerankTaskSettingsTests extends AbstractBWCWireSerializationTestCase<ContextualAiRerankTaskSettings> {

    private static final String INVALID_FIELD_TYPE_STRING = "invalid";

    private static final int ORIGINAL_TOP_N = 15;
    private static final int NEW_TOP_N = 10;
    private static final int TEST_TOP_N = 5;

    private static final String ORIGINAL_INSTRUCTION = "original instruction";
    private static final String NEW_INSTRUCTION = "new instruction";
    private static final String TEST_INSTRUCTION = "some instruction";

    public void testIsEmpty() {
        final var randomSettings = createRandom();
        final var stringRep = Strings.toString(randomSettings);
        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    public void testUpdatedTaskSettings_WithAllValues_ReplacesSettings() {
        var originalSettings = new ContextualAiRerankTaskSettings(ORIGINAL_TOP_N, ORIGINAL_INSTRUCTION);
        var updatedSettings = (ContextualAiRerankTaskSettings) originalSettings.updatedTaskSettings(
            getTaskSettingsMap(NEW_TOP_N, NEW_INSTRUCTION)
        );
        assertEquals(new ContextualAiRerankTaskSettings(NEW_TOP_N, NEW_INSTRUCTION), updatedSettings);
    }

    public void testUpdatedTaskSettings_MapWithOnlyTopN_DoesNotRetainInstruction() {
        var originalSettings = new ContextualAiRerankTaskSettings(ORIGINAL_TOP_N, ORIGINAL_INSTRUCTION);
        var updatedSettings = (ContextualAiRerankTaskSettings) originalSettings.updatedTaskSettings(getTaskSettingsMap(NEW_TOP_N, null));
        assertEquals(new ContextualAiRerankTaskSettings(NEW_TOP_N, null), updatedSettings);
    }

    public void testUpdatedTaskSettings_MapWithOnlyInstruction_DoesNotRetainTopN() {
        var originalSettings = new ContextualAiRerankTaskSettings(ORIGINAL_TOP_N, ORIGINAL_INSTRUCTION);
        var updatedSettings = (ContextualAiRerankTaskSettings) originalSettings.updatedTaskSettings(
            getTaskSettingsMap(null, ORIGINAL_INSTRUCTION)
        );
        assertEquals(new ContextualAiRerankTaskSettings(null, ORIGINAL_INSTRUCTION), updatedSettings);
    }

    public void testUpdatedTaskSettings_EmptyMap_ReturnsEmptySettings() {
        var originalSettings = new ContextualAiRerankTaskSettings(ORIGINAL_TOP_N, ORIGINAL_INSTRUCTION);
        var updatedSettings = (ContextualAiRerankTaskSettings) originalSettings.updatedTaskSettings(new HashMap<>());
        assertThat(updatedSettings, sameInstance(EMPTY_SETTINGS));
    }

    public void testFromMap_AllValues() {
        assertEquals(
            new ContextualAiRerankTaskSettings(TEST_TOP_N, TEST_INSTRUCTION),
            ContextualAiRerankTaskSettings.fromMap(getTaskSettingsMap(TEST_TOP_N, TEST_INSTRUCTION))
        );
    }

    public void testFromMap_TopNOnly() {
        assertEquals(
            new ContextualAiRerankTaskSettings(TEST_TOP_N, null),
            ContextualAiRerankTaskSettings.fromMap(getTaskSettingsMap(TEST_TOP_N, null))
        );
    }

    public void testFromMap_InstructionOnly() {
        assertEquals(
            new ContextualAiRerankTaskSettings(null, TEST_INSTRUCTION),
            ContextualAiRerankTaskSettings.fromMap(getTaskSettingsMap(null, TEST_INSTRUCTION))
        );
    }

    public void testFromMap_NullMap_ReturnsEmptySettings() {
        assertThat(ContextualAiRerankTaskSettings.fromMap(null), sameInstance(EMPTY_SETTINGS));
    }

    public void testFromMap_EmptyMap_ReturnsEmptySettings() {
        assertThat(ContextualAiRerankTaskSettings.fromMap(new HashMap<>()), sameInstance(EMPTY_SETTINGS));
    }

    public void testFromMap_TopNIsInvalidValue_ThrowsValidationException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> ContextualAiRerankTaskSettings.fromMap(new HashMap<>(Map.of(TOP_N_FIELD, INVALID_FIELD_TYPE_STRING)))
        );
        assertThat(
            thrownException.validationErrors().getFirst(),
            is("field [top_n] is not of the expected type. The value [invalid] cannot be converted to a [Integer]")
        );
        assertThat(thrownException.validationErrors().size(), is(1));
    }

    public void testFromMap_InstructionIsInvalidValue_ThrowsValidationException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> ContextualAiRerankTaskSettings.fromMap(new HashMap<>(Map.of(INSTRUCTION_FIELD, 123)))
        );
        assertThat(
            thrownException.validationErrors().getFirst(),
            is("field [instruction] is not of the expected type. The value [123] cannot be converted to a [String]")
        );
        assertThat(thrownException.validationErrors().size(), is(1));
    }

    public void testFromMap_TopNIsNotPositive_ThrowsValidationException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> ContextualAiRerankTaskSettings.fromMap(getTaskSettingsMap(0, null))
        );
        assertThat(thrownException.getMessage(), containsString(TOP_N_FIELD));
        assertThat(thrownException.getMessage(), containsString("[task_settings]"));
    }

    public void testFromMap_TopNIsNegative_ThrowsValidationException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> ContextualAiRerankTaskSettings.fromMap(getTaskSettingsMap(-1, null))
        );
        assertThat(thrownException.getMessage(), containsString(TOP_N_FIELD));
        assertThat(thrownException.getMessage(), containsString("[task_settings]"));
    }

    public void testGetWriteableName_ReturnsContextualAiRerankTaskSettingsName() {
        assertThat(EMPTY_SETTINGS.getWriteableName(), is(ContextualAiRerankTaskSettings.NAME));
    }

    public void testOf_KeepsOriginalWhenRequestIsEmpty() {
        var originalSettings = new ContextualAiRerankTaskSettings(ORIGINAL_TOP_N, ORIGINAL_INSTRUCTION);
        var overrideSettings = ContextualAiRerankTaskSettings.fromMap(new HashMap<>());
        assertThat(ContextualAiRerankTaskSettings.of(originalSettings, overrideSettings), sameInstance(originalSettings));
    }

    public void testOf_ReturnsOriginalInstanceWhenRequestMatchesExistingValues() {
        var originalSettings = new ContextualAiRerankTaskSettings(ORIGINAL_TOP_N, ORIGINAL_INSTRUCTION);
        var requestWithSameValues = new ContextualAiRerankTaskSettings(ORIGINAL_TOP_N, ORIGINAL_INSTRUCTION);
        assertThat(ContextualAiRerankTaskSettings.of(originalSettings, requestWithSameValues), sameInstance(originalSettings));
    }

    public void testOf_PartialTopNOverride_RetainsOriginalInstruction() {
        var originalSettings = new ContextualAiRerankTaskSettings(ORIGINAL_TOP_N, ORIGINAL_INSTRUCTION);
        var requestTopNOnly = new ContextualAiRerankTaskSettings(NEW_TOP_N, null);
        assertThat(
            ContextualAiRerankTaskSettings.of(originalSettings, requestTopNOnly),
            is(new ContextualAiRerankTaskSettings(NEW_TOP_N, ORIGINAL_INSTRUCTION))
        );
    }

    public void testOf_PartialInstructionOverride_RetainsOriginalTopN() {
        var originalSettings = new ContextualAiRerankTaskSettings(ORIGINAL_TOP_N, ORIGINAL_INSTRUCTION);
        var requestInstructionOnly = new ContextualAiRerankTaskSettings(null, NEW_INSTRUCTION);
        assertThat(
            ContextualAiRerankTaskSettings.of(originalSettings, requestInstructionOnly),
            is(new ContextualAiRerankTaskSettings(ORIGINAL_TOP_N, NEW_INSTRUCTION))
        );
    }

    public void testOf_UsesAllParametersOverride() {
        var originalSettings = new ContextualAiRerankTaskSettings(ORIGINAL_TOP_N, ORIGINAL_INSTRUCTION);
        var overrideSettings = new ContextualAiRerankTaskSettings(NEW_TOP_N, NEW_INSTRUCTION);
        assertThat(
            ContextualAiRerankTaskSettings.of(originalSettings, overrideSettings),
            is(new ContextualAiRerankTaskSettings(NEW_TOP_N, NEW_INSTRUCTION))
        );
    }

    public void testToXContent_WithoutParameters() throws IOException {
        assertThat(getXContentResult(null, null), equalToIgnoringWhitespaceInJsonString("{}"));
    }

    public void testToXContent_WithTopNParameter() throws IOException {
        assertThat(getXContentResult(TEST_TOP_N, null), equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "top_n": %d
            }
            """, TEST_TOP_N)));
    }

    public void testToXContent_WithInstructionParameter() throws IOException {
        assertThat(getXContentResult(null, TEST_INSTRUCTION), equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "instruction": "%s"
            }
            """, TEST_INSTRUCTION)));
    }

    public void testToXContent_WithParameters() throws IOException {
        assertThat(getXContentResult(TEST_TOP_N, TEST_INSTRUCTION), equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "top_n": %d,
                "instruction": "%s"
            }
            """, TEST_TOP_N, TEST_INSTRUCTION)));
    }

    private static String getXContentResult(@Nullable Integer topN, @Nullable String instruction) throws IOException {
        var settings = ContextualAiRerankTaskSettings.fromMap(getTaskSettingsMap(topN, instruction));
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        return Strings.toString(builder);
    }

    public static HashMap<String, Object> getTaskSettingsMap(@Nullable Integer topN, @Nullable String instruction) {
        var map = new HashMap<String, Object>();
        if (topN != null) {
            map.put(TOP_N_FIELD, topN);
        }
        if (instruction != null) {
            map.put(INSTRUCTION_FIELD, instruction);
        }
        return map;
    }

    @Override
    protected Writeable.Reader<ContextualAiRerankTaskSettings> instanceReader() {
        return ContextualAiRerankTaskSettings::new;
    }

    @Override
    protected ContextualAiRerankTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected ContextualAiRerankTaskSettings mutateInstance(ContextualAiRerankTaskSettings instance) throws IOException {
        if (randomBoolean()) {
            Integer newTopN = randomValueOtherThan(instance.getTopN(), () -> randomIntBetween(1, 500));
            return new ContextualAiRerankTaskSettings(newTopN, instance.getInstruction());
        } else {
            String newInstruction = randomValueOtherThan(instance.getInstruction(), () -> randomAlphaOfLength(10));
            return new ContextualAiRerankTaskSettings(instance.getTopN(), newInstruction);
        }
    }

    @Override
    protected ContextualAiRerankTaskSettings mutateInstanceForVersion(ContextualAiRerankTaskSettings instance, TransportVersion version) {
        return instance;
    }

    private static ContextualAiRerankTaskSettings createRandom() {
        return new ContextualAiRerankTaskSettings(
            randomBoolean() ? randomIntBetween(1, 1000) : null,
            randomBoolean() ? randomAlphaOfLength(12) : null
        );
    }
}
