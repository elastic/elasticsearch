/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIServiceSettings;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.multimodal.VoyageAIMultimodalEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.multimodal.VoyageAIMultimodalEmbeddingsTaskSettings;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;

public class VoyageAIMultimodalEmbeddingsRequestEntityTests extends ESTestCase {

    /**
     * Helper method to create InferenceStringGroups from text strings.
     * Each string becomes a separate group with a single text content.
     */
    private static List<InferenceStringGroup> toInferenceStringGroups(String... texts) {
        return java.util.Arrays.stream(texts).map(InferenceStringGroup::new).toList();
    }

    public void testXContent_WritesNestedStructure_WithTextContent() throws IOException {
        var entity = new VoyageAIMultimodalEmbeddingsRequestEntity(
            toInferenceStringGroups("abc", "def"),
            InputType.INTERNAL_SEARCH,
            VoyageAIMultimodalEmbeddingsServiceSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.URL,
                        "https://www.abc.com",
                        ServiceFields.SIMILARITY,
                        SimilarityMeasure.DOT_PRODUCT.toString(),
                        ServiceFields.DIMENSIONS,
                        2048,
                        ServiceFields.MAX_INPUT_TOKENS,
                        512,
                        VoyageAIServiceSettings.MODEL_ID,
                        "voyage-multimodal-3",
                        VoyageAIMultimodalEmbeddingsServiceSettings.EMBEDDING_TYPE,
                        "float"
                    )
                ),
                ConfigurationParseContext.PERSISTENT
            ),
            new VoyageAIMultimodalEmbeddingsTaskSettings(InputType.INGEST, null),
            "voyage-multimodal-3"
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"inputs":[{"content":[{"type":"text","text":"abc"}]},{"content":[{"type":"text","text":"def"}]}],\
            "model":"voyage-multimodal-3","input_type":"query"}"""));
    }

    public void testXContent_WritesNestedStructure_WithInputTypeFromTaskSettings() throws IOException {
        var entity = new VoyageAIMultimodalEmbeddingsRequestEntity(
            toInferenceStringGroups("abc"),
            null,
            VoyageAIMultimodalEmbeddingsServiceSettings.EMPTY_SETTINGS,
            new VoyageAIMultimodalEmbeddingsTaskSettings(InputType.INGEST, null),
            "voyage-multimodal-3"
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"inputs":[{"content":[{"type":"text","text":"abc"}]}],"model":"voyage-multimodal-3","input_type":"document"}"""));
    }

    public void testXContent_WritesNestedStructure_WithTruncation() throws IOException {
        var entity = new VoyageAIMultimodalEmbeddingsRequestEntity(
            toInferenceStringGroups("abc"),
            InputType.INTERNAL_SEARCH,
            VoyageAIMultimodalEmbeddingsServiceSettings.EMPTY_SETTINGS,
            new VoyageAIMultimodalEmbeddingsTaskSettings(InputType.INGEST, true),
            "voyage-multimodal-3"
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"inputs":[{"content":[{"type":"text","text":"abc"}]}],"model":"voyage-multimodal-3","input_type":"query","truncation":true}"""));
    }

    public void testXContent_WritesNoOptionalFields_WhenTheyAreNotDefined() throws IOException {
        var entity = new VoyageAIMultimodalEmbeddingsRequestEntity(
            toInferenceStringGroups("abc"),
            null,
            VoyageAIMultimodalEmbeddingsServiceSettings.EMPTY_SETTINGS,
            VoyageAIMultimodalEmbeddingsTaskSettings.EMPTY_SETTINGS,
            "voyage-multimodal-3"
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"inputs":[{"content":[{"type":"text","text":"abc"}]}],"model":"voyage-multimodal-3"}"""));
    }

    public void testXContent_PrefersRootInputType_OverTaskSettingsInputType() throws IOException {
        var entity = new VoyageAIMultimodalEmbeddingsRequestEntity(
            toInferenceStringGroups("abc"),
            InputType.INTERNAL_SEARCH,
            VoyageAIMultimodalEmbeddingsServiceSettings.EMPTY_SETTINGS,
            new VoyageAIMultimodalEmbeddingsTaskSettings(InputType.INGEST, null),
            "voyage-multimodal-3"
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        // Should use "query" from root level INTERNAL_SEARCH, not "document" from task settings INGEST
        MatcherAssert.assertThat(xContentResult, is("""
            {"inputs":[{"content":[{"type":"text","text":"abc"}]}],"model":"voyage-multimodal-3","input_type":"query"}"""));
    }

    public void testXContent_WritesMultimodalContent_WithImageBase64() throws IOException {
        // Create a group with both text and image content
        var group = new InferenceStringGroup(List.of(
            new InferenceString(InferenceString.DataType.TEXT, "Describe this image"),
            new InferenceString(InferenceString.DataType.IMAGE, InferenceString.DataFormat.BASE64, "base64encodedimage")
        ));

        var entity = new VoyageAIMultimodalEmbeddingsRequestEntity(
            List.of(group),
            InputType.INGEST,
            VoyageAIMultimodalEmbeddingsServiceSettings.EMPTY_SETTINGS,
            VoyageAIMultimodalEmbeddingsTaskSettings.EMPTY_SETTINGS,
            "voyage-multimodal-3"
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"inputs":[{"content":[{"type":"text","text":"Describe this image"},\
            {"type":"image_base64","image_base64":"base64encodedimage"}]}],"model":"voyage-multimodal-3","input_type":"document"}"""));
    }

    public void testConvertToString_MapsInputTypesToVoyageAIFormat() {
        assertEquals("document", VoyageAIMultimodalEmbeddingsRequestEntity.convertToString(InputType.INGEST));
        assertEquals("document", VoyageAIMultimodalEmbeddingsRequestEntity.convertToString(InputType.INTERNAL_INGEST));
        assertEquals("query", VoyageAIMultimodalEmbeddingsRequestEntity.convertToString(InputType.SEARCH));
        assertEquals("query", VoyageAIMultimodalEmbeddingsRequestEntity.convertToString(InputType.INTERNAL_SEARCH));
        assertNull(VoyageAIMultimodalEmbeddingsRequestEntity.convertToString(null));
    }
}
