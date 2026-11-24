/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentHelper.stripWhitespace;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class TencentCloudMultimodalEmbeddingsRequestEntityTests extends ESTestCase {

    private static final String IMAGE_URL = "https://example.com/a.png";
    private static final String VIDEO_URL = "https://example.com/a.mp4";

    public void testXContent_AllModalities() throws IOException {
        var inputs = List.of(
            text("first"),
            new InferenceString(DataType.IMAGE, DataFormat.URL, IMAGE_URL),
            new InferenceString(DataType.VIDEO, DataFormat.BASE64, "data:video/mp4;base64,abcd"),
            text("second")
        );
        var entity = new TencentCloudMultimodalEmbeddingsRequestEntity(inputs, "EmbeddingGemma-2", "SearchQuery");

        assertThat(toJson(entity), is(stripWhitespace("""
            {
              "model": "EmbeddingGemma-2",
              "texts": ["first", "second"],
              "image_url": ["https://example.com/a.png"],
              "video_data": ["data:video/mp4;base64,abcd"],
              "prompt_name": "SearchQuery"
            }""")));
    }

    public void testXContent_Base64ImageAndVideoUrl_WithoutPromptName() throws IOException {
        var inputs = List.of(
            new InferenceString(DataType.IMAGE, DataFormat.BASE64, "data:image/png;base64,abcd"),
            new InferenceString(DataType.VIDEO, DataFormat.URL, VIDEO_URL)
        );
        var entity = new TencentCloudMultimodalEmbeddingsRequestEntity(inputs, "WeMM-Embedding-2B", null);

        assertThat(toJson(entity), is(stripWhitespace("""
            {
              "model": "WeMM-Embedding-2B",
              "image_data": ["data:image/png;base64,abcd"],
              "video_url": ["https://example.com/a.mp4"]
            }""")));
    }

    public void testXContent_OmitsPromptNameWithoutTexts() throws IOException {
        var inputs = List.of(new InferenceString(DataType.IMAGE, DataFormat.URL, IMAGE_URL));
        var entity = new TencentCloudMultimodalEmbeddingsRequestEntity(inputs, "EmbeddingGemma-2", "Document");

        assertThat(toJson(entity), is(stripWhitespace("""
            {
              "model": "EmbeddingGemma-2",
              "image_url": ["https://example.com/a.png"]
            }""")));
    }

    public void testInputField_RejectsUnsupportedTypes() {
        var input = new InferenceString(randomFrom(DataType.AUDIO, DataType.PDF), DataFormat.BASE64, "data:audio/mpeg;base64,abcd");
        var exception = expectThrows(IllegalArgumentException.class, () -> TencentCloudMultimodalEmbeddingsRequestEntity.inputField(input));
        assertThat(exception.getMessage(), containsString("Unsupported input type"));
    }

    private static String toJson(TencentCloudMultimodalEmbeddingsRequestEntity entity) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        return Strings.toString(builder);
    }

    private static InferenceString text(String value) {
        return new InferenceString(DataType.TEXT, value);
    }
}
