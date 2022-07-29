/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.junit.ComparisonFailure;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

public class AssertObjectNodes {
    private static final YAMLFactory YAML_FACTORY = new YAMLFactory();
    private static final ObjectMapper MAPPER = new ObjectMapper(YAML_FACTORY);
    private static final ObjectWriter WRITER = MAPPER.writerFor(ObjectNode.class);

    public static void areEqual(List<ObjectNode> actualTests, List<ObjectNode> expectedTests) {
        for (int i = 0; i < expectedTests.size(); i++) {
            ObjectNode expected = expectedTests.get(i);
            ObjectNode actual = actualTests.get(i);
            if (expected.equals(actual) == false) {
                throw new ComparisonFailure(
                    "The actual transformation is different than expected in _transformed.yml file.",
                    toString(expectedTests),
                    toString(actualTests)
                );
            }
        }
    }

    private static String toString(List<ObjectNode> tests) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {
            SequenceWriter sequenceWriter = WRITER.writeValues(baos);

            for (ObjectNode transformedTest : tests) {
                sequenceWriter.write(transformedTest);
            }
            sequenceWriter.close();
            return baos.toString();
        } catch (IOException e) {
            e.printStackTrace();
            return "Exception when serialising a file";
        }
    }
}
