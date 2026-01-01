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
import org.elasticsearch.xpack.esql.evaluator.CompoundOutputFunction;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

/**
 * Testing different scenarios where the coordinating node predefines a list of requested output fields and the actual execution occurs on
 * a data node with a different version, where the evaluating function produces outputs that may not fully match the predefined list.
 */
public class CompoundOutputEvaluatorTests extends ESTestCase {

    /**
     * All tests assume that the predefined output fields are as follows:
     * <ul>
     *     <li>field_a: KEYWORD</li>
     *     <li>field_b: INTEGER</li>
     *     <li>field_c: KEYWORD</li>
     * </ul>
     */
    private static final LinkedHashMap<String, DataType> PREDEFINED_OUTPUT_FIELDS;

    static {
        PREDEFINED_OUTPUT_FIELDS = new LinkedHashMap<>();
        PREDEFINED_OUTPUT_FIELDS.putLast("field_a", DataType.KEYWORD);
        PREDEFINED_OUTPUT_FIELDS.putLast("field_b", DataType.INTEGER);
        PREDEFINED_OUTPUT_FIELDS.putLast("field_c", DataType.KEYWORD);
    }

    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    /**
     * In order to imitate real scenarios, {@link CompoundOutputFunction#outputFields()} and {@link CompoundOutputFunction#evaluate(String)}
     * should be in sync.
     */
    private static class TestFunction implements CompoundOutputFunction {
        private final LinkedHashMap<String, DataType> outputColumns;
        private final Map<String, Object> evaluationOutput;

        TestFunction(Map<String, Object> evaluationOutput) {
            this.evaluationOutput = evaluationOutput;
            this.outputColumns = new LinkedHashMap<>(evaluationOutput.size());
            evaluationOutput.forEach((fieldName, value) -> {
                switch (value) {
                    case String ignored -> outputColumns.putLast(fieldName, DataType.KEYWORD);
                    case Integer ignored -> outputColumns.putLast(fieldName, DataType.INTEGER);
                    default -> throw new IllegalArgumentException("Unsupported value type: " + value);
                }
            });
        }

        @Override
        public LinkedHashMap<String, DataType> outputFields() {
            return outputColumns;
        }

        @Override
        public Map<String, Object> evaluate(String input) {
            return evaluationOutput;
        }
    }

    public void testMatchingOutput() {
        Map<String, Object> evaluationFunctionOutput = new LinkedHashMap<>();
        evaluationFunctionOutput.put("field_a", "value_a");
        evaluationFunctionOutput.put("field_b", 2);
        evaluationFunctionOutput.put("field_c", "value_c");
        Object[] expectedRowComputationOutput = new Object[] { "value_a", 2, "value_c" };
        evaluateAndCompare(evaluationFunctionOutput, expectedRowComputationOutput);
    }

    public void testMismatchedOutput_missingField() {
        Map<String, Object> evaluationFunctionOutput = new LinkedHashMap<>();
        evaluationFunctionOutput.put("field_a", "value_a");
        evaluateAndCompare(evaluationFunctionOutput, new Object[] { "value_a", null, null });
    }

    public void testMismatchedOutput_extraField() {
        Map<String, Object> evaluationFunctionOutput = new LinkedHashMap<>();
        evaluationFunctionOutput.put("field_a", "value_a");
        evaluationFunctionOutput.put("field_b", 2);
        evaluationFunctionOutput.put("field_c", "value_c");
        evaluationFunctionOutput.put("field_d", "extra_value");
        Object[] expectedRowComputationOutput = new Object[] { "value_a", 2, "value_c" };
        evaluateAndCompare(evaluationFunctionOutput, expectedRowComputationOutput);
    }

    public void testMismatchedOutput_sameLength() {
        Map<String, Object> evaluationFunctionOutput = new LinkedHashMap<>();
        evaluationFunctionOutput.put("field_a", "value_a");
        evaluationFunctionOutput.put("field_b", 2);
        evaluationFunctionOutput.put("field_d", "extra_value");
        evaluateAndCompare(evaluationFunctionOutput, new Object[] { "value_a", 2, null });
    }

    private void evaluateAndCompare(Map<String, Object> evaluationFunctionOutput, Object[] expectedRowComputationOutput) {
        Block.Builder[] targetBlocks = new Block.Builder[PREDEFINED_OUTPUT_FIELDS.size()];
        try (BytesRefBlock.Builder inputBuilder = blockFactory.newBytesRefBlockBuilder(1)) {
            inputBuilder.appendBytesRef(new BytesRef("test_input"));
            BytesRefBlock inputBlock = inputBuilder.build();

            int i = 0;
            for (DataType valueType : PREDEFINED_OUTPUT_FIELDS.values()) {
                targetBlocks[i++] = switch (valueType) {
                    case KEYWORD -> blockFactory.newBytesRefBlockBuilder(1);
                    case INTEGER -> blockFactory.newIntBlockBuilder(1);
                    default -> throw new IllegalArgumentException("Unsupported data type: " + valueType);
                };
            }

            CompoundOutputFunction function = new TestFunction(evaluationFunctionOutput);
            CompoundOutputEvaluator evaluator = new CompoundOutputEvaluator(
                PREDEFINED_OUTPUT_FIELDS,
                function,
                DataType.KEYWORD,
                Warnings.NOOP_WARNINGS
            );
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
