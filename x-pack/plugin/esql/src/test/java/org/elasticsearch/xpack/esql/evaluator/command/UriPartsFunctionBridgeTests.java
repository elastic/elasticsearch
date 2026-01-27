/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.command;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;
import java.util.Map;
import java.util.SequencedCollection;

import static org.elasticsearch.xpack.esql.evaluator.command.UriPartsFunctionBridge.DOMAIN;
import static org.elasticsearch.xpack.esql.evaluator.command.UriPartsFunctionBridge.EXTENSION;
import static org.elasticsearch.xpack.esql.evaluator.command.UriPartsFunctionBridge.FRAGMENT;
import static org.elasticsearch.xpack.esql.evaluator.command.UriPartsFunctionBridge.PASSWORD;
import static org.elasticsearch.xpack.esql.evaluator.command.UriPartsFunctionBridge.PATH;
import static org.elasticsearch.xpack.esql.evaluator.command.UriPartsFunctionBridge.PORT;
import static org.elasticsearch.xpack.esql.evaluator.command.UriPartsFunctionBridge.QUERY;
import static org.elasticsearch.xpack.esql.evaluator.command.UriPartsFunctionBridge.SCHEME;
import static org.elasticsearch.xpack.esql.evaluator.command.UriPartsFunctionBridge.USERNAME;
import static org.elasticsearch.xpack.esql.evaluator.command.UriPartsFunctionBridge.USER_INFO;
import static org.hamcrest.Matchers.is;

public class UriPartsFunctionBridgeTests extends ESTestCase {

    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    private static class TestUriPartsEvaluator extends UriPartsFunctionBridge {
        TestUriPartsEvaluator(SequencedCollection<String> outputFields, Warnings warnings) {
            super(DataType.TEXT, warnings, new UriPartsCollectorImpl(outputFields, new BlocksBearer()));
        }
    }

    public void testFullOutput() {
        List<String> requestedFields = List.of(SCHEME, DOMAIN, PORT, PATH, EXTENSION, QUERY, FRAGMENT, USER_INFO, USERNAME, PASSWORD);
        String input = "http://user:pass@example.com:8080/path/to/file.html?query=val#fragment";
        Object[] expected = new Object[] {
            "http",
            "example.com",
            8080,
            "/path/to/file.html",
            "html",
            "query=val",
            "fragment",
            "user:pass",
            "user",
            "pass" };
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testPartialFieldsRequested() {
        List<String> requestedFields = List.of(DOMAIN, PORT);
        String input = "http://user:pass@example.com:8080/path/to/file.html?query=val#fragment";
        Object[] expected = new Object[] { "example.com", 8080 };
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testMissingPortAndUserInfo() {
        List<String> requestedFields = List.of(SCHEME, DOMAIN, PORT, USERNAME);
        String input = "https://elastic.co/downloads";
        Object[] expected = new Object[] { "https", "elastic.co", null, null };
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testMissingExtension() {
        List<String> requestedFields = List.of(PATH, EXTENSION);
        String input = "https://elastic.co/downloads";
        Object[] expected = new Object[] { "/downloads", null };
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testAllMissingFields() {
        List<String> requestedFields = List.of(FRAGMENT, QUERY, USER_INFO);
        String input = "https://elastic.co/downloads";
        Object[] expected = new Object[] { null, null, null };
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testInvalidInput() {
        List<String> requestedFields = List.of(DOMAIN, PORT);
        String input = "not a valid url";
        Object[] expected = new Object[] { null, null };
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, 1, 2, "invalid_input");
        evaluateAndCompare(input, requestedFields, expected, warnings);
        assertCriticalWarnings(
            "Line 1:2: evaluation of [invalid_input] failed, treating result as null. Only first 20 failures recorded.",
            "Line 1:2: java.lang.IllegalArgumentException: unable to parse URI [not a valid url]"
        );
    }

    private void evaluateAndCompare(String input, List<String> requestedFields, Object[] expectedRowComputationOutput) {
        evaluateAndCompare(input, requestedFields, expectedRowComputationOutput, Warnings.NOOP_WARNINGS);
    }

    private void evaluateAndCompare(String input, List<String> requestedFields, Object[] expectedRowComputationOutput, Warnings warnings) {
        TestUriPartsEvaluator evaluator = new TestUriPartsEvaluator(requestedFields, warnings);
        Block.Builder[] targetBlocks = new Block.Builder[requestedFields.size()];
        try (BytesRefBlock.Builder inputBuilder = blockFactory.newBytesRefBlockBuilder(1)) {
            inputBuilder.appendBytesRef(new BytesRef(input));
            BytesRefBlock inputBlock = inputBuilder.build();

            Map<String, Class<?>> allFields = UriPartsFunctionBridge.getAllOutputFields();

            int i = 0;
            for (String fieldName : requestedFields) {
                Class<?> type = allFields.get(fieldName);
                if (type == String.class) {
                    targetBlocks[i++] = blockFactory.newBytesRefBlockBuilder(1);
                } else if (type == Integer.class) {
                    targetBlocks[i++] = blockFactory.newIntBlockBuilder(1);
                } else {
                    throw new IllegalArgumentException("Unsupported field type for: " + fieldName);
                }
            }
            evaluator.computeRow(inputBlock, 0, targetBlocks, new BytesRef());

            for (int j = 0; j < expectedRowComputationOutput.length; j++) {
                Object expected = expectedRowComputationOutput[j];
                try (Block builtBlock = targetBlocks[j].build()) {
                    if (expected == null) {
                        assertThat("Expected null for field [" + requestedFields.get(j) + "]", builtBlock.isNull(0), is(true));
                    } else if (expected instanceof String s) {
                        BytesRefBlock fieldBlock = (BytesRefBlock) builtBlock;
                        assertThat(fieldBlock.isNull(0), is(false));
                        assertThat(fieldBlock.getBytesRef(0, new BytesRef()).utf8ToString(), is(s));
                    } else if (expected instanceof Integer v) {
                        IntBlock fieldBlock = (IntBlock) builtBlock;
                        assertThat(fieldBlock.isNull(0), is(false));
                        assertThat(fieldBlock.getInt(0), is(v));
                    } else {
                        throw new IllegalArgumentException("Unsupported expected output type: " + expected.getClass());
                    }
                }
            }
        } finally {
            Releasables.closeExpectNoException(targetBlocks);
        }
    }
}
