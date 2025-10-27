/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.embeddings.multimodal;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class VoyageAIMultimodalEmbeddingsTaskSettingsTests extends AbstractWireSerializingTestCase<
    VoyageAIMultimodalEmbeddingsTaskSettings> {

    public void testFromMap_WithInputType() {
        var taskSettingsMap = getTaskSettingsMap(InputType.INGEST);
        var taskSettings = VoyageAIMultimodalEmbeddingsTaskSettings.fromMap(taskSettingsMap);

        MatcherAssert.assertThat(taskSettings, is(new VoyageAIMultimodalEmbeddingsTaskSettings(InputType.INGEST, null)));
    }

    public void testFromMap_WithNullInputType() {
        var taskSettings = VoyageAIMultimodalEmbeddingsTaskSettings.fromMap(new HashMap<>());

        MatcherAssert.assertThat(taskSettings, is(VoyageAIMultimodalEmbeddingsTaskSettings.EMPTY_SETTINGS));
    }

    public void testToXContent_WithoutInputType() throws IOException {
        var taskSettings = new VoyageAIMultimodalEmbeddingsTaskSettings(null, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        taskSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("{}"));
    }

    public void testToXContent_WithInputType() throws IOException {
        var taskSettings = new VoyageAIMultimodalEmbeddingsTaskSettings(InputType.INGEST, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        taskSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("{\"input_type\":\"ingest\"}"));
    }

    @Override
    protected Writeable.Reader<VoyageAIMultimodalEmbeddingsTaskSettings> instanceReader() {
        return VoyageAIMultimodalEmbeddingsTaskSettings::new;
    }

    @Override
    protected VoyageAIMultimodalEmbeddingsTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected VoyageAIMultimodalEmbeddingsTaskSettings mutateInstance(VoyageAIMultimodalEmbeddingsTaskSettings instance) {
        return randomValueOtherThan(instance, VoyageAIMultimodalEmbeddingsTaskSettingsTests::createRandom);
    }

    private static VoyageAIMultimodalEmbeddingsTaskSettings createRandom() {
        var inputType = randomBoolean() ? randomFrom(InputType.INGEST, InputType.SEARCH) : null;
        return new VoyageAIMultimodalEmbeddingsTaskSettings(inputType, null);
    }

    public static Map<String, Object> getTaskSettingsMap(@Nullable InputType inputType) {
        var map = new HashMap<String, Object>();

        if (inputType != null) {
            map.put(VoyageAIMultimodalEmbeddingsTaskSettings.INPUT_TYPE, inputType.toString());
        }

        return map;
    }
}
