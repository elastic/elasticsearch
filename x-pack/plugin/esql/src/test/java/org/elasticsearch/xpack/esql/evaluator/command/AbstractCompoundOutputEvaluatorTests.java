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

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public abstract class AbstractCompoundOutputEvaluatorTests<T extends CompoundOutputEvaluator.OutputFieldsCollector> extends ESTestCase {

    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    protected abstract CompoundOutputEvaluator<T> createEvaluator(List<String> requestedFields, Warnings warnings);

    protected abstract Map<String, Class<?>> getSupportedOutputFieldMappings();

    protected void evaluateAndCompare(List<String> input, List<String> requestedFields, List<Object[]> expectedRowComputationOutput) {
        evaluateAndCompare(input, requestedFields, expectedRowComputationOutput, Warnings.NOOP_WARNINGS);
    }

    protected void evaluateAndCompare(
        List<String> inputList,
        List<String> requestedFields,
        List<Object[]> expectedRowComputationOutput,
        Warnings warnings
    ) {
        CompoundOutputEvaluator<T> evaluator = createEvaluator(requestedFields, warnings);
        Block.Builder[] targetBlocks = new Block.Builder[requestedFields.size()];
        try (BytesRefBlock.Builder inputBuilder = blockFactory.newBytesRefBlockBuilder(inputList.size())) {
            inputBuilder.beginPositionEntry();
            inputList.forEach(s -> inputBuilder.appendBytesRef(new BytesRef(s)));
            inputBuilder.endPositionEntry();
            BytesRefBlock inputBlock = inputBuilder.build();

            Map<String, Class<?>> supportedFields = getSupportedOutputFieldMappings();

            int i = 0;
            for (String fieldName : requestedFields) {
                Class<?> type = supportedFields.get(fieldName);
                if (type == Integer.class) {
                    // noinspection resource - closed in the finally block
                    targetBlocks[i] = blockFactory.newIntBlockBuilder(1);
                } else {
                    // either String or unknown fields
                    // noinspection resource - closed in the finally block
                    targetBlocks[i] = blockFactory.newBytesRefBlockBuilder(1);
                }
                i++;
            }
            evaluator.computeRow(inputBlock, 0, targetBlocks, new BytesRef());

            for (int j = 0; j < expectedRowComputationOutput.size(); j++) {
                Object[] expectedValues = expectedRowComputationOutput.get(j);
                try (Block builtBlock = targetBlocks[j].build()) {
                    for (int k = 0; k < expectedValues.length; k++) {
                        Object value = expectedValues[k];
                        switch (value) {
                            case null -> assertThat(
                                "Expected null for field [" + requestedFields.get(k) + "]",
                                builtBlock.isNull(k),
                                is(true)
                            );
                            case String s -> {
                                BytesRefBlock fieldBlock = (BytesRefBlock) builtBlock;
                                assertThat(fieldBlock.isNull(k), is(false));
                                assertThat(fieldBlock.getBytesRef(k, new BytesRef()).utf8ToString(), is(s));
                            }
                            case Integer v -> {
                                IntBlock fieldBlock = (IntBlock) builtBlock;
                                assertThat(fieldBlock.isNull(k), is(false));
                                assertThat(fieldBlock.getInt(k), is(v));
                            }
                            default -> throw new IllegalArgumentException("Unsupported expected output type: " + value.getClass());
                        }
                    }
                }
            }
        } finally {
            Releasables.closeExpectNoException(targetBlocks);
        }
    }
}
