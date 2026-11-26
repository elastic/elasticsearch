/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.embeddings;

import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class TencentCloudMultimodalModelLimitsTests extends ESTestCase {

    public void testForModel() {
        assertThat(
            TencentCloudMultimodalModelLimits.forModel("WeMM-Embedding-2B"),
            is(new TencentCloudMultimodalModelLimits(120, 8, 4, false))
        );
        assertThat(
            TencentCloudMultimodalModelLimits.forModel("WeMM-Embedding-9B"),
            is(new TencentCloudMultimodalModelLimits(60, 8, 4, false))
        );
        assertThat(
            TencentCloudMultimodalModelLimits.forModel("EmbeddingGemma-2"),
            is(new TencentCloudMultimodalModelLimits(200, 8, 4, true))
        );
        assertThat(TencentCloudMultimodalModelLimits.forModel("other"), is(new TencentCloudMultimodalModelLimits(60, 8, 4, false)));
    }

    public void testPartition_Empty() {
        assertThat(TencentCloudMultimodalModelLimits.forModel("WeMM-Embedding-2B").partition(List.of()), empty());
    }

    public void testPartition_FitsInOneBatch() {
        var inputs = List.of(text("a"), imageUrl(), videoBase64(), text("b"));
        assertThat(TencentCloudMultimodalModelLimits.forModel("WeMM-Embedding-2B").partition(inputs), contains(inputs));
    }

    public void testPartition_SplitsOnTextLimit() {
        var inputs = IntStream.range(0, 130).mapToObj(i -> text("t" + i)).toList();
        var batches = TencentCloudMultimodalModelLimits.forModel("WeMM-Embedding-2B").partition(inputs);
        assertThat(batches.size(), is(2));
        assertThat(batches.get(0), is(inputs.subList(0, 120)));
        assertThat(batches.get(1), is(inputs.subList(120, 130)));
    }

    public void testPartition_SplitsOnImageAndVideoLimits() {
        var inputs = new ArrayList<InferenceStringGroup>();
        for (int i = 0; i < 9; i++) {
            inputs.add(imageUrl());
        }
        for (int i = 0; i < 5; i++) {
            inputs.add(videoBase64());
        }
        var batches = TencentCloudMultimodalModelLimits.forModel("EmbeddingGemma-2").partition(inputs);
        // 8 images | 1 image + 4 videos | 1 video
        assertThat(batches.stream().map(List::size).toList(), is(List.of(8, 5, 1)));
        assertThat(batches.stream().flatMap(List::stream).toList(), is(inputs));
    }

    public void testPartition_SplitsWhenBase64AndUrlOfTheSameModalityMix() {
        var inputs = List.of(text("a"), imageUrl(), imageBase64(), videoBase64(), videoUrl(), text("b"));
        var batches = TencentCloudMultimodalModelLimits.forModel("WeMM-Embedding-2B").partition(inputs);
        assertThat(batches, is(List.of(inputs.subList(0, 2), inputs.subList(2, 4), inputs.subList(4, 6))));
    }

    private static InferenceStringGroup text(String value) {
        return new InferenceStringGroup(value);
    }

    private static InferenceStringGroup imageUrl() {
        return new InferenceStringGroup(new InferenceString(DataType.IMAGE, DataFormat.URL, "https://example.com/a.png"));
    }

    private static InferenceStringGroup imageBase64() {
        return new InferenceStringGroup(new InferenceString(DataType.IMAGE, DataFormat.BASE64, "data:image/png;base64,abcd"));
    }

    private static InferenceStringGroup videoUrl() {
        return new InferenceStringGroup(new InferenceString(DataType.VIDEO, DataFormat.URL, "https://example.com/a.mp4"));
    }

    private static InferenceStringGroup videoBase64() {
        return new InferenceStringGroup(new InferenceString(DataType.VIDEO, DataFormat.BASE64, "data:video/mp4;base64,abcd"));
    }
}
