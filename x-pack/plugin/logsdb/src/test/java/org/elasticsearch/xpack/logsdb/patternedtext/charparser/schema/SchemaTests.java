/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.EncodingType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressForbidden(reason = "prints out the schema for debugging purposes within test")
public class SchemaTests extends ESTestCase {
    public void testReadAndPrintSchema() {
        Schema schema = Schema.getInstance();
        assertNotNull(schema);
        System.out.println(schema);
    }

    public void testMultiTokenTypesParsing() {
        Schema schema = Schema.getInstance();
        assertNotNull(schema);

        // build name->type map for easy assertions
        Map<String, MultiTokenType> nameToType = schema.getMultiTokenTypes()
            .stream()
            .collect(Collectors.toMap(MultiTokenType::name, t -> t));

        // ensure expected multi-token types from schema.yaml are present
        assertTrue("RFC-1123-timestamp should be present", nameToType.containsKey("RFC-1123-timestamp"));
        assertTrue(
            "logging-libraries-datetime-timestamp should be present",
            nameToType.containsKey("logging-libraries-datetime-timestamp")
        );

        MultiTokenType t1 = nameToType.get("RFC-1123-timestamp");
        MultiTokenType t2 = nameToType.get("logging-libraries-datetime-timestamp");

        // encoding type expected to be %T (Timestamp)
        assertEquals("encodingType for RFC-1123-timestamp", EncodingType.TIMESTAMP, t1.encodingType());
        assertEquals("encodingType for logging-libraries-datetime-timestamp", EncodingType.TIMESTAMP, t2.encodingType());

        nameToType.values().forEach(t -> {
            StringBuilder formatFromParts = new StringBuilder();
            List<String> delimiterParts = t.getFormat().getDelimiterParts();
            List<TokenType> tokens = t.getFormat().getTokens();
            for (int i = 0; i < tokens.size(); i++) {
                formatFromParts.append('$').append(tokens.get(i).name());
                if (i < delimiterParts.size()) {
                    formatFromParts.append(delimiterParts.get(i));
                }
            }
            assertEquals(t.getFormat().getRawFormat(), formatFromParts.toString());
        });
    }
}
