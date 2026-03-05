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
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.ColumnExtractOperator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.operator.blocksource.SequenceBytesRefBlockSourceOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.is;

public abstract class AbstractCompoundOutputEvaluatorTests extends OperatorTestCase {

    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return is("ColumnExtractOperator[evaluator=CompoundOutputEvaluator[collector=" + getCollectorName() + "]]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return is("ColumnExtractOperator[evaluator=CompoundOutputEvaluator[collector=" + getCollectorName() + "]]");
    }

    private String getCollectorName() {
        return createOutputFieldsCollector(List.of()).toString();
    }

    protected abstract List<String> getRequestedFieldsForSimple();

    protected abstract List<String> getSampleInputForSimple();

    protected abstract List<Object[]> getExpectedOutputForSimple();

    protected DataType getInputTypeForSimple() {
        return DataType.TEXT;
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        List<String> sampleInput = getSampleInputForSimple();
        return new SequenceBytesRefBlockSourceOperator(
            blockFactory,
            IntStream.range(0, size).mapToObj(i -> new BytesRef(sampleInput.get(i % sampleInput.size())))
        );
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        final List<String> requestedFields = getRequestedFieldsForSimple();
        Map<String, Class<?>> supportedFields = getSupportedOutputFieldMappings();

        ElementType[] outputTypes = new ElementType[requestedFields.size()];
        for (int i = 0; i < requestedFields.size(); i++) {
            Class<?> javaType = supportedFields.get(requestedFields.get(i));
            DataType dataType = DataType.fromJavaType(javaType);
            outputTypes[i] = PlannerUtils.toElementType(dataType);
        }

        return new ColumnExtractOperator.Factory(outputTypes, dvrCtx -> new EvalOperator.ExpressionEvaluator() {
            @Override
            public Block eval(Page page) {
                Block input = page.getBlock(0);
                input.incRef();
                return input;
            }

            @Override
            public long baseRamBytesUsed() {
                return 0;
            }

            @Override
            public void close() {}
        }, new CompoundOutputEvaluator.Factory(getInputTypeForSimple(), null, new CompoundOutputEvaluator.OutputFieldsCollectorProvider() {
            @Override
            public CompoundOutputEvaluator.OutputFieldsCollector createOutputFieldsCollector() {
                return AbstractCompoundOutputEvaluatorTests.this.createOutputFieldsCollector(requestedFields);
            }

            @Override
            public String collectorSimpleName() {
                return AbstractCompoundOutputEvaluatorTests.this.collectorSimpleName();
            }
        }));
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        List<Object[]> expectedSampleColumns = getExpectedOutputForSimple();
        Map<String, Class<?>> supportedFields = getSupportedOutputFieldMappings();
        List<String> requestedFields = getRequestedFieldsForSimple();

        // 1. Get total number of rows from results
        int totalRows = 0;
        for (Page page : results) {
            totalRows += page.getPositionCount();
        }

        // 2. Tile the expected output to match the total number of rows
        List<List<Object>> expectedColumns = new ArrayList<>();
        if (totalRows > 0) {
            for (Object[] sampleColumn : expectedSampleColumns) {
                List<Object> fullColumn = new ArrayList<>(totalRows);
                if (sampleColumn.length > 0) {
                    for (int i = 0; i < totalRows; i++) {
                        fullColumn.add(sampleColumn[i % sampleColumn.length]);
                    }
                } else {
                    for (int i = 0; i < totalRows; i++) {
                        fullColumn.add(null);
                    }
                }
                expectedColumns.add(fullColumn);
            }
        }

        // 3. Materialize actual result columns
        List<List<Object>> actualColumns = new ArrayList<>();
        for (int i = 0; i < requestedFields.size(); i++) {
            actualColumns.add(new ArrayList<>(totalRows));
        }

        BytesRef scratch = new BytesRef();
        for (Page page : results) {
            for (int colIdx = 0; colIdx < requestedFields.size(); colIdx++) {
                Block block = page.getBlock(colIdx + 1); // +1 to skip input block
                String fieldName = requestedFields.get(colIdx);
                Class<?> type = supportedFields.get(fieldName);

                for (int rowIdx = 0; rowIdx < page.getPositionCount(); rowIdx++) {
                    if (block.isNull(rowIdx)) {
                        actualColumns.get(colIdx).add(null);
                    } else if (type == Integer.class) {
                        actualColumns.get(colIdx).add(((IntBlock) block).getInt(rowIdx));
                    } else {
                        actualColumns.get(colIdx).add(((BytesRefBlock) block).getBytesRef(rowIdx, scratch).utf8ToString());
                    }
                }
            }
        }

        // 4. Compare
        assertEquals("Number of columns mismatch", expectedColumns.size(), actualColumns.size());
        for (int i = 0; i < expectedColumns.size(); i++) {
            assertEquals("Column " + requestedFields.get(i) + " data mismatch", expectedColumns.get(i), actualColumns.get(i));
        }
    }

    protected abstract CompoundOutputEvaluator.OutputFieldsCollector createOutputFieldsCollector(List<String> requestedFields);

    protected abstract String collectorSimpleName();

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
        CompoundOutputEvaluator.OutputFieldsCollector outputFieldsCollector = createOutputFieldsCollector(requestedFields);
        CompoundOutputEvaluator evaluator = new CompoundOutputEvaluator(DataType.TEXT, warnings, outputFieldsCollector);
        Block.Builder[] targetBlocks = new Block.Builder[requestedFields.size()];
        try (BytesRefBlock.Builder inputBuilder = blockFactory.newBytesRefBlockBuilder(inputList.size())) {
            inputBuilder.beginPositionEntry();
            inputList.forEach(s -> inputBuilder.appendBytesRef(new BytesRef(s)));
            inputBuilder.endPositionEntry();
            try (BytesRefBlock inputBlock = inputBuilder.build()) {
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
            }
        } finally {
            Releasables.closeExpectNoException(targetBlocks);
        }
    }
}
