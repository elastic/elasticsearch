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
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedCollection;
import java.util.function.BiConsumer;
import java.util.function.ObjIntConsumer;

import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.NOOP_INT_COLLECTOR;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.NOOP_STRING_COLLECTOR;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.intValueCollector;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.nullValueCollector;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.stringValueCollector;
import static org.hamcrest.Matchers.is;

/**
 * Testing different scenarios where the coordinating node predefines a list of requested output fields and the actual execution occurs on
 * a data node with a different version, where the evaluating function produces outputs that may not fully match the predefined list.
 */
public class CompoundOutputEvaluatorTests extends ESTestCase {

    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    private static Map<String, Object> testFunction(String input) {
        Map<String, Object> result = new HashMap<>();
        String[] parts = input.split("-");
        for (String part : parts) {
            String[] entry = part.trim().split(":");
            if (entry.length != 2) {
                throw new IllegalArgumentException("Invalid input: " + input);
            }
            Object value;
            try {
                value = Integer.parseInt(entry[1]);
            } catch (NumberFormatException e) {
                value = entry[1];
            }
            result.put(entry[0], value);
        }
        return result;
    }

    private static class TestFieldsCollector extends CompoundOutputEvaluator.OutputFieldsCollector {
        private BiConsumer<Block.Builder[], String> fieldA = NOOP_STRING_COLLECTOR;
        private ObjIntConsumer<Block.Builder[]> fieldB = NOOP_INT_COLLECTOR;
        private BiConsumer<Block.Builder[], String> fieldC = NOOP_STRING_COLLECTOR;

        TestFieldsCollector(SequencedCollection<String> outputFields, CompoundOutputEvaluator.BlocksBearer blocksBearer) {
            super(blocksBearer);
            int index = 0;
            for (String fieldName : outputFields) {
                switch (fieldName) {
                    case "field_a" -> fieldA = stringValueCollector(index);
                    case "field_b" -> fieldB = intValueCollector(index, value -> value >= 0);
                    case "field_c" -> fieldC = stringValueCollector(index);
                    default -> unknownFieldCollectors.add(nullValueCollector(index));
                }
                index++;
            }
        }

        public void fieldA(String value) {
            fieldA.accept(blocksBearer.get(), value);
        }

        public void fieldB(Integer value) {
            fieldB.accept(blocksBearer.get(), value);
        }

        public void fieldC(String value) {
            fieldC.accept(blocksBearer.get(), value);
        }

        @Override
        protected boolean evaluate(String input) {
            Map<String, Object> evaluationFunctionOutput = testFunction(input);
            try {
                fieldA((String) evaluationFunctionOutput.get("field_a"));
                Object valueB = evaluationFunctionOutput.get("field_b");
                valueB = valueB == null ? -1 : ((Number) valueB).intValue();
                fieldB((Integer) valueB);
                fieldC((String) evaluationFunctionOutput.get("field_c"));
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid input: " + input, e);
            }
            return true;
        }
    }

    private static class TestEvaluator extends CompoundOutputEvaluator<TestFieldsCollector> {
        TestEvaluator(SequencedCollection<String> outputFields) {
            super(DataType.TEXT, Warnings.NOOP_WARNINGS, new TestFieldsCollector(outputFields, new BlocksBearer()));
        }
    }

    public void testMatchingOutput() {
        List<String> requestedFields = List.of("field_a", "field_b", "field_c");
        String input = "field_a:valueA-field_b:2-field_c:valueC";
        Object[] expectedRowComputationOutput = new Object[] { "valueA", 2, "valueC" };
        evaluateAndCompare(input, requestedFields, expectedRowComputationOutput);
    }

    public void testPartialFieldsRequested_1() {
        List<String> requestedFields = List.of("field_a", "field_b");
        String input = "field_a:valueA-field_b:2-field_c:valueC";
        Object[] expectedRowComputationOutput = new Object[] { "valueA", 2 };
        evaluateAndCompare(input, requestedFields, expectedRowComputationOutput);
    }

    public void testPartialFieldsRequested_2() {
        List<String> requestedFields = List.of("field_b");
        String input = "field_a:valueA-field_b:2-field_c:valueC";
        Object[] expectedRowComputationOutput = new Object[] { 2 };
        evaluateAndCompare(input, requestedFields, expectedRowComputationOutput);
    }

    public void testUnsupportedField() {
        List<String> requestedFields = List.of("field_a", "field_b", "field_c");
        String input = "field_a:valueA-field_b:2-field_c:valueC-extra_field:extraValue";
        Object[] expectedRowComputationOutput = new Object[] { "valueA", 2, "valueC" };
        evaluateAndCompare(input, requestedFields, expectedRowComputationOutput);
    }

    public void testMissingField_1() {
        List<String> requestedFields = List.of("field_a", "field_b", "field_c");
        String input = "field_b:2-field_c:valueC";
        Object[] expectedRowComputationOutput = new Object[] { null, 2, "valueC" };
        evaluateAndCompare(input, requestedFields, expectedRowComputationOutput);
    }

    public void testMissingField_2() {
        List<String> requestedFields = List.of("field_a", "field_b", "field_c");
        String input = "foo:1-field_b:2-bar:3";
        Object[] expectedRowComputationOutput = new Object[] { null, 2, null };
        evaluateAndCompare(input, requestedFields, expectedRowComputationOutput);
    }

    public void testMissingField_3() {
        List<String> requestedFields = List.of("field_a", "field_b", "field_c");
        String input = "foo:1-bar:2-field_b:3-baz:4-field_c:valueC";
        Object[] expectedRowComputationOutput = new Object[] { null, 3, "valueC" };
        evaluateAndCompare(input, requestedFields, expectedRowComputationOutput);
    }

    public void testAllMissingFields() {
        List<String> requestedFields = List.of("field_a", "field_b", "field_c");
        String input = "field_d:2-field_e:valueE";
        Object[] expectedRowComputationOutput = new Object[] { null, null, null };
        evaluateAndCompare(input, requestedFields, expectedRowComputationOutput);
    }

    public void testWrongFieldType() {
        List<String> requestedFields = List.of("field_a", "field_b", "field_c");
        String input = "field_a:1-field_c:valueC";
        Object[] expectedRowComputationOutput = new Object[] { null, null, null };
        evaluateAndCompare(input, requestedFields, expectedRowComputationOutput);
    }

    public void testKnownAndUnknownFields() {
        List<String> requestedFields = List.of("field_a", "field_b", "unknown_field");
        String input = "field_a:valueA-field_b:2-field_c:valueC";
        Object[] expectedRowComputationOutput = new Object[] { "valueA", 2, null };
        evaluateAndCompare(input, requestedFields, expectedRowComputationOutput);
    }

    public void testOnlyUnknownFields() {
        List<String> requestedFields = List.of("unknown_field_a", "unknown_field_b");
        String input = "field_a:valueA-field_b:2-field_c:valueC";
        Object[] expectedRowComputationOutput = new Object[] { null, null };
        evaluateAndCompare(input, requestedFields, expectedRowComputationOutput);
    }

    private void evaluateAndCompare(String input, List<String> requestedFields, Object[] expectedRowComputationOutput) {
        TestEvaluator evaluator = new TestEvaluator(requestedFields);
        Block.Builder[] targetBlocks = new Block.Builder[requestedFields.size()];
        try (BytesRefBlock.Builder inputBuilder = blockFactory.newBytesRefBlockBuilder(1)) {
            inputBuilder.appendBytesRef(new BytesRef(input));
            BytesRefBlock inputBlock = inputBuilder.build();

            int i = 0;
            for (String fieldName : requestedFields) {
                // noinspection SwitchStatementWithTooFewBranches
                targetBlocks[i++] = switch (fieldName) {
                    case "field_b" -> blockFactory.newIntBlockBuilder(1);
                    default -> blockFactory.newBytesRefBlockBuilder(1);
                };
            }
            evaluator.computeRow(inputBlock, 0, targetBlocks, new BytesRef());

            for (int j = 0; j < expectedRowComputationOutput.length; j++) {
                Object o = expectedRowComputationOutput[j];
                switch (o) {
                    case String s -> {
                        BytesRefBlock fieldBlock = (BytesRefBlock) targetBlocks[j].build();
                        assertThat(fieldBlock.isNull(0), is(false));
                        assertThat(fieldBlock.getBytesRef(0, new BytesRef()).utf8ToString(), is(s));
                    }
                    case Integer v -> {
                        IntBlock fieldBlock = (IntBlock) targetBlocks[j].build();
                        assertThat(fieldBlock.isNull(0), is(false));
                        assertThat(fieldBlock.getInt(0), is(v));
                    }
                    case null -> {
                        Block fieldBlock = targetBlocks[j].build();
                        assertThat(fieldBlock.isNull(0), is(true));
                    }
                    default -> throw new IllegalArgumentException("Unsupported expected output type: " + o);
                }
            }
        } finally {
            Releasables.closeExpectNoException(targetBlocks);
        }
    }
}
