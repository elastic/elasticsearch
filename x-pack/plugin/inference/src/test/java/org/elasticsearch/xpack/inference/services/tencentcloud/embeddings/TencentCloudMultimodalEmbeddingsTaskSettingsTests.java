/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class TencentCloudMultimodalEmbeddingsTaskSettingsTests extends AbstractWireSerializingTestCase<
    TencentCloudMultimodalEmbeddingsTaskSettings> {

    public static TencentCloudMultimodalEmbeddingsTaskSettings createRandom() {
        return new TencentCloudMultimodalEmbeddingsTaskSettings(
            randomBoolean() ? randomFrom(TencentCloudMultimodalEmbeddingsTaskSettings.VALID_INPUT_TYPE_VALUES) : null,
            randomBoolean() ? randomFrom(TencentCloudMultimodalEmbeddingsTaskSettings.VALID_PROMPT_NAMES) : null
        );
    }

    public void testFromMap_ReturnsEmptySettings() {
        assertThat(
            TencentCloudMultimodalEmbeddingsTaskSettings.fromMap(new HashMap<>()),
            is(TencentCloudMultimodalEmbeddingsTaskSettings.EMPTY_SETTINGS)
        );
    }

    public void testFromMap_NullMap_ReturnsEmptySettings() {
        assertThat(
            TencentCloudMultimodalEmbeddingsTaskSettings.fromMap(null),
            is(TencentCloudMultimodalEmbeddingsTaskSettings.EMPTY_SETTINGS)
        );
    }

    public void testFromMap_AllFields_ConsumesMap() {
        var map = new HashMap<String, Object>(
            Map.of(
                TencentCloudMultimodalEmbeddingsTaskSettings.INPUT_TYPE,
                "search",
                TencentCloudMultimodalEmbeddingsTaskSettings.PROMPT_NAME,
                "Clustering"
            )
        );
        var settings = TencentCloudMultimodalEmbeddingsTaskSettings.fromMap(map);
        assertThat(settings, is(new TencentCloudMultimodalEmbeddingsTaskSettings(InputType.SEARCH, "Clustering")));
        assertTrue(map.isEmpty());
    }

    public void testFromMap_InvalidPromptName_Throws() {
        var map = new HashMap<String, Object>(Map.of(TencentCloudMultimodalEmbeddingsTaskSettings.PROMPT_NAME, "searchquery"));
        var exception = expectThrows(ValidationException.class, () -> TencentCloudMultimodalEmbeddingsTaskSettings.fromMap(map));
        assertThat(exception.getMessage(), containsString("Invalid value [searchquery] received. [prompt_name] must be one of"));
    }

    public void testFromMap_InvalidInputType_Throws() {
        var map = new HashMap<String, Object>(Map.of(TencentCloudMultimodalEmbeddingsTaskSettings.INPUT_TYPE, "unspecified"));
        var exception = expectThrows(ValidationException.class, () -> TencentCloudMultimodalEmbeddingsTaskSettings.fromMap(map));
        assertThat(exception.getMessage(), containsString("[input_type]"));
    }

    public void testOf_RequestSettingsOverrideOriginal() {
        var original = new TencentCloudMultimodalEmbeddingsTaskSettings(InputType.INGEST, "Document");
        assertThat(
            TencentCloudMultimodalEmbeddingsTaskSettings.of(
                original,
                new TencentCloudMultimodalEmbeddingsTaskSettings(InputType.SEARCH, null)
            ),
            is(new TencentCloudMultimodalEmbeddingsTaskSettings(InputType.SEARCH, "Document"))
        );
        assertThat(
            TencentCloudMultimodalEmbeddingsTaskSettings.of(original, TencentCloudMultimodalEmbeddingsTaskSettings.EMPTY_SETTINGS),
            is(original)
        );
    }

    public void testOf_FieldUnsetInBoth_StaysUnset() {
        var original = new TencentCloudMultimodalEmbeddingsTaskSettings(InputType.INGEST, null);
        assertThat(
            TencentCloudMultimodalEmbeddingsTaskSettings.of(
                original,
                new TencentCloudMultimodalEmbeddingsTaskSettings(InputType.SEARCH, null)
            ),
            is(new TencentCloudMultimodalEmbeddingsTaskSettings(InputType.SEARCH, null))
        );
        assertThat(
            TencentCloudMultimodalEmbeddingsTaskSettings.of(
                TencentCloudMultimodalEmbeddingsTaskSettings.EMPTY_SETTINGS,
                TencentCloudMultimodalEmbeddingsTaskSettings.EMPTY_SETTINGS
            ),
            is(TencentCloudMultimodalEmbeddingsTaskSettings.EMPTY_SETTINGS)
        );
    }

    public void testUpdatedTaskSettings_MergesFieldsAndConsumesMap() {
        var original = new TencentCloudMultimodalEmbeddingsTaskSettings(InputType.INGEST, null);
        var newSettings = new HashMap<String, Object>(Map.of(TencentCloudMultimodalEmbeddingsTaskSettings.PROMPT_NAME, "STS"));
        var updated = original.updatedTaskSettings(newSettings);
        assertThat(updated, is(new TencentCloudMultimodalEmbeddingsTaskSettings(InputType.INGEST, "STS")));
        assertThat(newSettings.isEmpty(), is(true));
    }

    public void testUpdatedTaskSettings_LeavesUnknownFieldsInMap() {
        var original = new TencentCloudMultimodalEmbeddingsTaskSettings(null, null);
        var newSettings = new HashMap<String, Object>(
            Map.of(TencentCloudMultimodalEmbeddingsTaskSettings.INPUT_TYPE, "search", "prompt", "STS")
        );
        var updated = original.updatedTaskSettings(newSettings);
        assertThat(updated, is(new TencentCloudMultimodalEmbeddingsTaskSettings(InputType.SEARCH, null)));
        assertThat(newSettings, is(Map.of("prompt", "STS")));
    }

    public void testResolvePromptName_ExplicitPromptNameWins() {
        var settings = new TencentCloudMultimodalEmbeddingsTaskSettings(InputType.INGEST, "Reranking");
        assertThat(settings.resolvePromptName(InputType.SEARCH), is("Reranking"));
    }

    public void testResolvePromptName_RequestInputTypeWinsOverSettingsInputType() {
        var settings = new TencentCloudMultimodalEmbeddingsTaskSettings(InputType.INGEST, null);
        assertThat(settings.resolvePromptName(InputType.SEARCH), is("SearchQuery"));
        assertThat(settings.resolvePromptName(InputType.UNSPECIFIED), is("Document"));
        assertThat(settings.resolvePromptName(null), is("Document"));
    }

    public void testResolvePromptName_MapsInputTypes() {
        var settings = TencentCloudMultimodalEmbeddingsTaskSettings.EMPTY_SETTINGS;
        assertThat(settings.resolvePromptName(InputType.SEARCH), is("SearchQuery"));
        assertThat(settings.resolvePromptName(InputType.INTERNAL_SEARCH), is("SearchQuery"));
        assertThat(settings.resolvePromptName(InputType.INGEST), is("Document"));
        assertThat(settings.resolvePromptName(InputType.INTERNAL_INGEST), is("Document"));
        assertThat(settings.resolvePromptName(InputType.CLUSTERING), is("Clustering"));
        assertThat(settings.resolvePromptName(InputType.CLASSIFICATION), is("Classification"));
        assertThat(settings.resolvePromptName(InputType.UNSPECIFIED), is(nullValue()));
    }

    public void testToXContent() throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        new TencentCloudMultimodalEmbeddingsTaskSettings(InputType.SEARCH, "SearchQuery").toXContent(builder, null);
        assertThat(Strings.toString(builder), is("""
            {"input_type":"search","prompt_name":"SearchQuery"}"""));
        assertThat(
            new TencentCloudMultimodalEmbeddingsTaskSettings(InputType.SEARCH, "SearchQuery").getWriteableName(),
            is("tencentcloud_multimodal_embeddings_task_settings")
        );
    }

    @Override
    protected Writeable.Reader<TencentCloudMultimodalEmbeddingsTaskSettings> instanceReader() {
        return TencentCloudMultimodalEmbeddingsTaskSettings::new;
    }

    @Override
    protected TencentCloudMultimodalEmbeddingsTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected TencentCloudMultimodalEmbeddingsTaskSettings mutateInstance(TencentCloudMultimodalEmbeddingsTaskSettings instance)
        throws IOException {
        if (randomBoolean()) {
            var inputType = randomValueOtherThan(
                instance.getInputType(),
                () -> randomFrom(randomFrom(TencentCloudMultimodalEmbeddingsTaskSettings.VALID_INPUT_TYPE_VALUES), null)
            );
            return new TencentCloudMultimodalEmbeddingsTaskSettings(inputType, instance.getPromptName());
        }
        var promptName = randomValueOtherThan(
            instance.getPromptName(),
            () -> randomFrom(randomFrom(TencentCloudMultimodalEmbeddingsTaskSettings.VALID_PROMPT_NAMES), null)
        );
        return new TencentCloudMultimodalEmbeddingsTaskSettings(instance.getInputType(), promptName);
    }

}
