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
import org.elasticsearch.test.ESTestCase;
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
import static org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankTaskSettings.RETURN_DOCUMENTS_FIELD;
import static org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankTaskSettings.TOP_N_FIELD;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class ContextualAiRerankTaskSettingsTests extends AbstractBWCWireSerializationTestCase<ContextualAiRerankTaskSettings> {

    private static final String INVALID_FIELD_TYPE_STRING = "invalid";

    private static final boolean TEST_RETURN_DOCUMENTS = true;
    private static final boolean ORIGINAL_RETURN_DOCUMENTS = false;
    private static final boolean NEW_RETURN_DOCUMENTS = true;

    private static final int TEST_TOP_N = 5;
    private static final int ORIGINAL_TOP_N = 10;
    private static final int NEW_TOP_N = 15;

    private static final String TEST_INSTRUCTION = "some instruction";
    private static final String ORIGINAL_INSTRUCTION = "original instruction";
    private static final String NEW_INSTRUCTION = "new instruction";

    public void testIsEmpty_True() {
        var emptySettings = new ContextualAiRerankTaskSettings(null, null, null);
        assertThat(emptySettings.isEmpty(), is(true));
        assertThat(emptySettings, is(EMPTY_SETTINGS));
    }

    public void testIsEmpty_False() {
        var nonEmptySettings = new ContextualAiRerankTaskSettings(TEST_RETURN_DOCUMENTS, TEST_TOP_N, TEST_INSTRUCTION);
        assertThat(nonEmptySettings.isEmpty(), is(false));
        assertThat(nonEmptySettings, is(not(EMPTY_SETTINGS)));
    }

    public void testUpdatedTaskSettings_WithAllValues_ReplacesSettings() {
        var originalSettings = new ContextualAiRerankTaskSettings(ORIGINAL_RETURN_DOCUMENTS, ORIGINAL_TOP_N, ORIGINAL_INSTRUCTION);
        var updatedSettings = (ContextualAiRerankTaskSettings) originalSettings.updatedTaskSettings(
            buildTaskSettingsMap(NEW_RETURN_DOCUMENTS, NEW_TOP_N, NEW_INSTRUCTION)
        );
        assertThat(updatedSettings, is(new ContextualAiRerankTaskSettings(NEW_RETURN_DOCUMENTS, NEW_TOP_N, NEW_INSTRUCTION)));
    }

    public void testUpdatedTaskSettings_EmptyMap_ReturnsEmptySettings() {
        var originalSettings = new ContextualAiRerankTaskSettings(ORIGINAL_RETURN_DOCUMENTS, ORIGINAL_TOP_N, ORIGINAL_INSTRUCTION);
        var updatedSettings = (ContextualAiRerankTaskSettings) originalSettings.updatedTaskSettings(new HashMap<>());
        assertThat(updatedSettings, sameInstance(EMPTY_SETTINGS));
    }

    public void testFromMap_AllValues_CreatesSettingsSuccessfully() {
        var taskSettings = ContextualAiRerankTaskSettings.fromMap(
            buildTaskSettingsMap(TEST_RETURN_DOCUMENTS, TEST_TOP_N, TEST_INSTRUCTION)
        );
        assertThat(taskSettings, is(new ContextualAiRerankTaskSettings(TEST_RETURN_DOCUMENTS, TEST_TOP_N, TEST_INSTRUCTION)));
    }

    public void testFromMap_NullMap_ReturnsEmptySettings() {
        assertThat(ContextualAiRerankTaskSettings.fromMap(null), sameInstance(EMPTY_SETTINGS));
    }

    public void testFromMap_EmptyMap_ReturnsEmptySettings() {
        assertThat(ContextualAiRerankTaskSettings.fromMap(new HashMap<>()), sameInstance(EMPTY_SETTINGS));
    }

    public void testFromMap_ReturnDocumentsIsInvalidValue_ThrowsValidationException() {
        assertFromMap_ThrowsValidationException(
            new HashMap<>(Map.of(RETURN_DOCUMENTS_FIELD, INVALID_FIELD_TYPE_STRING)),
            Strings.format(
                "field [return_documents] is not of the expected type. The value [%s] cannot be converted to a [Boolean]",
                INVALID_FIELD_TYPE_STRING
            )
        );
    }

    public void testFromMap_TopNIsInvalidValue_ThrowsValidationException() {
        assertFromMap_ThrowsValidationException(
            new HashMap<>(Map.of(TOP_N_FIELD, INVALID_FIELD_TYPE_STRING)),
            Strings.format(
                "field [top_n] is not of the expected type. The value [%s] cannot be converted to a [Integer]",
                INVALID_FIELD_TYPE_STRING
            )
        );
    }

    public void testFromMap_InstructionIsInvalidValue_ThrowsValidationException() {
        int invalidInstructionValue = 123;
        assertFromMap_ThrowsValidationException(
            new HashMap<>(Map.of(INSTRUCTION_FIELD, invalidInstructionValue)),
            Strings.format(
                "field [instruction] is not of the expected type. The value [%d] cannot be converted to a [String]",
                invalidInstructionValue
            )
        );
    }

    public void testFromMap_TopNIsZero_ThrowsValidationException() {
        int invalidTopNValue = 0;
        assertFromMap_TopNIsInvalid_ThrowsValidationException(invalidTopNValue);
    }

    public void testFromMap_TopNIsNegative_ThrowsValidationException() {
        int invalidTopNValue = -1;
        assertFromMap_TopNIsInvalid_ThrowsValidationException(invalidTopNValue);
    }

    private static void assertFromMap_TopNIsInvalid_ThrowsValidationException(int invalidTopNValue) {
        assertFromMap_ThrowsValidationException(
            buildTaskSettingsMap(null, invalidTopNValue, null),
            Strings.format("[task_settings] Invalid value [%d]. [top_n] must be a positive integer", invalidTopNValue)
        );
    }

    private static void assertFromMap_ThrowsValidationException(HashMap<String, Object> map, String expectedErrorMessage) {
        var thrownException = expectThrows(ValidationException.class, () -> ContextualAiRerankTaskSettings.fromMap(map));
        assertThat(thrownException.validationErrors().getFirst(), is(expectedErrorMessage));
        assertThat(thrownException.validationErrors().size(), is(1));
    }

    public void testGetWriteableName_ReturnsContextualAiRerankTaskSettingsName() {
        assertThat(EMPTY_SETTINGS.getWriteableName(), is(ContextualAiRerankTaskSettings.NAME));
    }

    public void testOf_KeepsOriginalWhenRequestIsEmpty() {
        var originalSettings = new ContextualAiRerankTaskSettings(ORIGINAL_RETURN_DOCUMENTS, ORIGINAL_TOP_N, ORIGINAL_INSTRUCTION);
        var overrideSettings = new ContextualAiRerankTaskSettings(null, null, null);
        assertThat(ContextualAiRerankTaskSettings.of(originalSettings, overrideSettings), sameInstance(originalSettings));
    }

    public void testOf_ReturnsOriginalInstanceWhenRequestMatchesExistingValues() {
        var originalSettings = new ContextualAiRerankTaskSettings(ORIGINAL_RETURN_DOCUMENTS, ORIGINAL_TOP_N, ORIGINAL_INSTRUCTION);
        var requestWithSameValues = new ContextualAiRerankTaskSettings(ORIGINAL_RETURN_DOCUMENTS, ORIGINAL_TOP_N, ORIGINAL_INSTRUCTION);
        assertThat(ContextualAiRerankTaskSettings.of(originalSettings, requestWithSameValues), sameInstance(originalSettings));
    }

    public void testOf_PartialTopNOverride_RetainsOriginalInstruction() {
        var originalSettings = new ContextualAiRerankTaskSettings(ORIGINAL_RETURN_DOCUMENTS, ORIGINAL_TOP_N, ORIGINAL_INSTRUCTION);
        var requestTopNOnly = new ContextualAiRerankTaskSettings(null, NEW_TOP_N, null);
        assertThat(
            ContextualAiRerankTaskSettings.of(originalSettings, requestTopNOnly),
            is(new ContextualAiRerankTaskSettings(ORIGINAL_RETURN_DOCUMENTS, NEW_TOP_N, ORIGINAL_INSTRUCTION))
        );
    }

    public void testOf_PartialInstructionOverride_RetainsOriginalTopN() {
        var originalSettings = new ContextualAiRerankTaskSettings(ORIGINAL_RETURN_DOCUMENTS, ORIGINAL_TOP_N, ORIGINAL_INSTRUCTION);
        var requestInstructionOnly = new ContextualAiRerankTaskSettings(null, null, NEW_INSTRUCTION);
        assertThat(
            ContextualAiRerankTaskSettings.of(originalSettings, requestInstructionOnly),
            is(new ContextualAiRerankTaskSettings(ORIGINAL_RETURN_DOCUMENTS, ORIGINAL_TOP_N, NEW_INSTRUCTION))
        );
    }

    public void testOf_UsesAllParametersOverride() {
        var originalSettings = new ContextualAiRerankTaskSettings(ORIGINAL_RETURN_DOCUMENTS, ORIGINAL_TOP_N, ORIGINAL_INSTRUCTION);
        var overrideSettings = new ContextualAiRerankTaskSettings(NEW_RETURN_DOCUMENTS, NEW_TOP_N, NEW_INSTRUCTION);
        assertThat(
            ContextualAiRerankTaskSettings.of(originalSettings, overrideSettings),
            is(new ContextualAiRerankTaskSettings(NEW_RETURN_DOCUMENTS, NEW_TOP_N, NEW_INSTRUCTION))
        );
    }

    public void testToXContent_AllFields() throws IOException {
        assertThat(
            getXContentResult(TEST_RETURN_DOCUMENTS, TEST_TOP_N, TEST_INSTRUCTION),
            equalToIgnoringWhitespaceInJsonString(Strings.format("""
                {
                    "return_documents": %b,
                    "top_n": %d,
                    "instruction": "%s"
                }
                """, TEST_RETURN_DOCUMENTS, TEST_TOP_N, TEST_INSTRUCTION))
        );
    }

    public void testToXContent_Empty() throws IOException {
        assertThat(getXContentResult(null, null, null), is("{}"));
    }

    private static String getXContentResult(@Nullable Boolean returnDocuments, @Nullable Integer topN, @Nullable String instruction)
        throws IOException {
        var settings = new ContextualAiRerankTaskSettings(returnDocuments, topN, instruction);
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        return Strings.toString(builder);
    }

    private static HashMap<String, Object> buildTaskSettingsMap(
        @Nullable Boolean returnDocuments,
        @Nullable Integer topN,
        @Nullable String instruction
    ) {
        var map = new HashMap<String, Object>();
        if (returnDocuments != null) {
            map.put(RETURN_DOCUMENTS_FIELD, returnDocuments);
        }
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
        var returnDocuments = instance.getReturnDocuments();
        var topN = instance.getTopN();
        var instruction = instance.getInstruction();
        switch (randomInt(2)) {
            case 0 -> returnDocuments = randomValueOtherThan(returnDocuments, ESTestCase::randomOptionalBoolean);
            case 1 -> topN = randomValueOtherThan(topN, () -> randomBoolean() ? randomIntBetween(1, 1000) : null);
            case 2 -> instruction = randomValueOtherThan(instruction, () -> randomAlphaOfLengthOrNull(12));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new ContextualAiRerankTaskSettings(returnDocuments, topN, instruction);
    }

    @Override
    protected ContextualAiRerankTaskSettings mutateInstanceForVersion(ContextualAiRerankTaskSettings instance, TransportVersion version) {
        return instance;
    }

    private static ContextualAiRerankTaskSettings createRandom() {
        var returnDocuments = randomOptionalBoolean();
        var topN = randomBoolean() ? randomIntBetween(1, 1000) : null;
        var instruction = randomAlphaOfLengthOrNull(12);
        return new ContextualAiRerankTaskSettings(returnDocuments, topN, instruction);
    }
}
