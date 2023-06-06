/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Experimental
public class StringExtractOperator extends AbstractPageMappingOperator {

    public record StringExtractOperatorFactory(
        String[] fieldNames,
        Supplier<EvalOperator.ExpressionEvaluator> expressionEvaluator,
        Supplier<Function<String, Map<String, String>>> parserSupplier
    ) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new StringExtractOperator(fieldNames, expressionEvaluator.get(), parserSupplier.get());
        }

        @Override
        public String describe() {
            return "StringExtractOperator[fields=[" + Arrays.stream(fieldNames).collect(Collectors.joining(", ")) + "]]";
        }
    }

    private final String[] fieldNames;
    private final EvalOperator.ExpressionEvaluator inputEvaluator;
    private final Function<String, Map<String, String>> parser;  // TODO parser should consume ByteRef instead of String

    public StringExtractOperator(
        String[] fieldNames,
        EvalOperator.ExpressionEvaluator inputEvaluator,
        Function<String, Map<String, String>> parser
    ) {
        this.fieldNames = fieldNames;
        this.inputEvaluator = inputEvaluator;
        this.parser = parser;
    }

    @Override
    protected Page process(Page page) {
        int rowsCount = page.getPositionCount();

        BytesRefBlock.Builder[] blockBuilders = new BytesRefBlock.Builder[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            blockBuilders[i] = BytesRefBlock.newBlockBuilder(rowsCount);
        }

        BytesRefBlock input = (BytesRefBlock) inputEvaluator.eval(page);
        BytesRef spare = new BytesRef();
        for (int row = 0; row < rowsCount; row++) {
            if (input.isNull(row)) {
                for (int i = 0; i < fieldNames.length; i++) {
                    blockBuilders[i].appendNull();
                }
                continue;
            }

            // For now more than a single input value will just read the first one
            int position = input.getFirstValueIndex(row);
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
        }

        Block[] blocks = new Block[blockBuilders.length];
        for (int i = 0; i < blockBuilders.length; i++) {
            blocks[i] = blockBuilders[i].build();
        }
        return page.appendBlocks(blocks);
    }

    @Override
    public String toString() {
        return "StringExtractOperator[fields=[" + Arrays.stream(fieldNames).collect(Collectors.joining(", ")) + "]]";
    }

    public interface ExtractEvaluator {
        Map<String, Object> computeRow(Page page, int position);
    }
}
