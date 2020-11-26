/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.FrequencyEncodingTests;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncodingTests;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.TargetMeanEncodingTests;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class InferenceToXContentCompressorTests extends ESTestCase {

    public void testInflateAndDeflate() throws IOException {
        for(int i = 0; i < 10; i++) {
            TrainedModelDefinition definition = TrainedModelDefinitionTests.createRandomBuilder().build();
            String firstDeflate = InferenceToXContentCompressor.deflate(definition);
            TrainedModelDefinition inflatedDefinition = InferenceToXContentCompressor.inflate(firstDeflate,
                parser -> TrainedModelDefinition.fromXContent(parser, false).build(),
                xContentRegistry());

            // Did we inflate to the same object?
            assertThat(inflatedDefinition, equalTo(definition));
        }
    }

    public void testInflateTooLargeStream() throws IOException {
        TrainedModelDefinition definition = TrainedModelDefinitionTests.createRandomBuilder()
            .setPreProcessors(Stream.generate(() -> randomFrom(FrequencyEncodingTests.createRandom(),
                OneHotEncodingTests.createRandom(),
                TargetMeanEncodingTests.createRandom()))
                .limit(100)
                .collect(Collectors.toList()))
            .build();
        String firstDeflate = InferenceToXContentCompressor.deflate(definition);
        int max = firstDeflate.getBytes(StandardCharsets.UTF_8).length + 10;
        IOException ex = expectThrows(IOException.class,
            () -> Streams.readFully(InferenceToXContentCompressor.inflate(firstDeflate, max)));
        assertThat(ex.getMessage(), equalTo("" +
            "input stream exceeded maximum bytes of [" + max + "]"));
    }

    public void testInflateGarbage() {
        expectThrows(IOException.class, () -> Streams.readFully(InferenceToXContentCompressor.inflate(randomAlphaOfLength(10), 100L)));
    }

    public void testInflateParsingTooLargeStream() throws IOException {
        TrainedModelDefinition definition = TrainedModelDefinitionTests.createRandomBuilder()
            .setPreProcessors(Stream.generate(() -> randomFrom(FrequencyEncodingTests.createRandom(),
                OneHotEncodingTests.createRandom(),
                TargetMeanEncodingTests.createRandom()))
                .limit(100)
                .collect(Collectors.toList()))
            .build();
        String compressedString = InferenceToXContentCompressor.deflate(definition);
        int max = compressedString.getBytes(StandardCharsets.UTF_8).length + 10;

        IOException e = expectThrows(IOException.class, ()-> InferenceToXContentCompressor.inflate(compressedString,
            parser -> TrainedModelDefinition.fromXContent(parser, true).build(),
            xContentRegistry(),
            max));

        assertThat(e.getMessage(), equalTo("Cannot parse model definition as the content is larger than the maximum stream size of ["
            + max + "] bytes. Max stream size is 10% of the JVM heap or 1GB whichever is smallest"));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }

}
