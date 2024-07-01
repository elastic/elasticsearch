/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.amazonbedrock;

import com.fasterxml.jackson.core.JsonGenerator;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class AmazonBedrockJsonBuilderTests extends ESTestCase {
    private static class TestWriter implements AmazonBedrockJsonWriter {

        private final String input;

        TestWriter(@Nullable String input) {
            this.input = input;
        }

        @Override
        public JsonGenerator writeJson(JsonGenerator generator) throws IOException {
            if (input == null) {
                throw new IOException("No input provided");
            }
            generator.writeStartObject();
            generator.writeStringField("input", input);
            generator.writeEndObject();
            return generator;
        }
    }

    public void testAmazonBedrockJsonBuilder_GeneratesValidJson() throws IOException {
        var writer = new TestWriter("test input");
        var builder = new AmazonBedrockJsonBuilder(writer);
        var result = builder.getStringContent();
        assertThat(result, is("{\"input\":\"test input\"}"));
    }

    public void testAmazonBedrockJsonBuilder_HandlesExceptions() {
        var writer = new TestWriter(null);
        var builder = new AmazonBedrockJsonBuilder(writer);
        var exceptionThrown = assertThrows(IOException.class, builder::getStringContent);
        assertThat(exceptionThrown.getMessage(), is("No input provided"));
    }
}
