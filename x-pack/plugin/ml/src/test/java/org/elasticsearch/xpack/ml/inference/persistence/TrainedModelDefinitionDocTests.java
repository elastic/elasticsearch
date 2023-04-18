/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.persistence;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Base64;

public class TrainedModelDefinitionDocTests extends AbstractXContentTestCase<TrainedModelDefinitionDoc> {

    private final boolean isLenient = randomBoolean();

    public void testParsingDocWithCompressedStringDefinition() throws IOException {
        byte[] bytes = randomByteArrayOfLength(50);
        String base64 = Base64.getEncoder().encodeToString(bytes);

        // The previous storage format was a base64 encoded string.
        // The new format should parse and decode the string storing the raw bytes.
        String compressedStringDoc = Strings.format("""
            {
              "doc_type": "trained_model_definition_doc",
              "model_id": "bntHUo",
              "doc_num": 6,
              "definition_length": 7,
              "total_definition_length": 13,
              "compression_version": 3,
              "definition": "%s",
              "eos": false
            }""", base64);

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, compressedStringDoc)) {
            TrainedModelDefinitionDoc parsed = doParseInstance(parser);
            assertArrayEquals(bytes, parsed.getBinaryData().array());
        }
    }

    @Override
    protected TrainedModelDefinitionDoc doParseInstance(XContentParser parser) throws IOException {
        return TrainedModelDefinitionDoc.fromXContent(parser, isLenient).build();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return isLenient;
    }

    @Override
    protected TrainedModelDefinitionDoc createTestInstance() {
        int length = randomIntBetween(4, 10);

        return new TrainedModelDefinitionDoc.Builder().setModelId(randomAlphaOfLength(6))
            .setDefinitionLength(length)
            .setTotalDefinitionLength(randomIntBetween(length, length * 2))
            .setBinaryData(new BytesArray(randomByteArrayOfLength(length)))
            .setDocNum(randomIntBetween(0, 10))
            .setCompressionVersion(randomIntBetween(1, 5))
            .setEos(randomBoolean())
            .build();
    }
}
