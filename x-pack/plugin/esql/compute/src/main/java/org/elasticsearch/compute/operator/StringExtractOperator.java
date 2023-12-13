/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class StringExtractOperator extends AbstractPageMappingOperator {

    public record StringExtractOperatorFactory(
        String[] fieldNames,
        ExpressionEvaluator.Factory expressionEvaluator,
        Supplier<Function<String, Map<String, String>>> parserSupplier
    ) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new StringExtractOperator(fieldNames, expressionEvaluator.get(driverContext), parserSupplier.get(), driverContext);
        }

        @Override
        public String describe() {
            return "StringExtractOperator[fields=[" + Arrays.stream(fieldNames).collect(Collectors.joining(", ")) + "]]";
        }
    }

    private final String[] fieldNames;
    private final EvalOperator.ExpressionEvaluator inputEvaluator;
    private final Function<String, Map<String, String>> parser;  // TODO parser should consume ByteRef instead of String
    private final DriverContext driverContext;

    public StringExtractOperator(
        String[] fieldNames,
        EvalOperator.ExpressionEvaluator inputEvaluator,
        Function<String, Map<String, String>> parser,
        DriverContext driverContext
    ) {
        this.fieldNames = fieldNames;
        this.inputEvaluator = inputEvaluator;
        this.parser = parser;
        this.driverContext = driverContext;
    }

    @Override
    protected Page process(Page page) {
        int rowsCount = page.getPositionCount();

        BytesRefBlock.Builder[] blockBuilders = new BytesRefBlock.Builder[fieldNames.length];
        try {
            for (int i = 0; i < fieldNames.length; i++) {
                blockBuilders[i] = BytesRefBlock.newBlockBuilder(rowsCount, driverContext.blockFactory());
            }

            try (BytesRefBlock input = (BytesRefBlock) inputEvaluator.eval(page)) {
                BytesRef spare = new BytesRef();
                for (int row = 0; row < rowsCount; row++) {
                    if (input.isNull(row)) {
                        for (int i = 0; i < fieldNames.length; i++) {
                            blockBuilders[i].appendNull();
                        }
                        continue;
                    }

                    int position = input.getFirstValueIndex(row);
                    int valueCount = input.getValueCount(row);
                    if (valueCount == 1) {
                        Map<String, String> items = parser.apply(input.getBytesRef(position, spare).utf8ToString());
                        if (items == null) {
                            for (int i = 0; i < fieldNames.length; i++) {
                                blockBuilders[i].appendNull();
                            }
                            continue;
                        }
                        for (int i = 0; i < fieldNames.length; i++) {
                            String val = items.get(fieldNames[i]);
                            BlockUtils.appendValue(blockBuilders[i], val, ElementType.BYTES_REF);
                        }
                    } else {
                        // multi-valued input
                        String[] firstValues = new String[fieldNames.length];
                        boolean[] positionEntryOpen = new boolean[fieldNames.length];
                        for (int c = 0; c < valueCount; c++) {
                            Map<String, String> items = parser.apply(input.getBytesRef(position + c, spare).utf8ToString());
                            if (items == null) {
                                continue;
                            }
                            for (int i = 0; i < fieldNames.length; i++) {
                                String val = items.get(fieldNames[i]);
                                if (val == null) {
                                    continue;
                                }
                                if (firstValues[i] == null) {
                                    firstValues[i] = val;
                                } else {
                                    if (positionEntryOpen[i] == false) {
                                        positionEntryOpen[i] = true;
                                        blockBuilders[i].beginPositionEntry();
                                        BlockUtils.appendValue(blockBuilders[i], firstValues[i], ElementType.BYTES_REF);
                                    }
                                    BlockUtils.appendValue(blockBuilders[i], val, ElementType.BYTES_REF);
                                }
                            }
                        }
                        for (int i = 0; i < fieldNames.length; i++) {
                            if (positionEntryOpen[i]) {
                                blockBuilders[i].endPositionEntry();
                            } else if (firstValues[i] == null) {
                                blockBuilders[i].appendNull();
                            } else {
                                BlockUtils.appendValue(blockBuilders[i], firstValues[i], ElementType.BYTES_REF);
                            }
                        }
                    }
                }
            }

            Block[] blocks = new Block[blockBuilders.length];
            for (int i = 0; i < blockBuilders.length; i++) {
                blocks[i] = blockBuilders[i].build();
            }
            return page.appendBlocks(blocks);
        } finally {
            Releasables.closeExpectNoException(blockBuilders);
        }

    }

    @Override
    public String toString() {
        return "StringExtractOperator[fields=[" + Arrays.stream(fieldNames).collect(Collectors.joining(", ")) + "]]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(inputEvaluator, super::close);
    }
}
