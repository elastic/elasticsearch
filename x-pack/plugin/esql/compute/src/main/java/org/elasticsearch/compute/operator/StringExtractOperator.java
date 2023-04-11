/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

@Experimental
public class StringExtractOperator extends AbstractPageMappingOperator {

    public record StringExtractOperatorFactory(
        String[] fieldNames,
        Supplier<EvalOperator.ExpressionEvaluator> expressionEvaluator,
        Supplier<Function<String, Map<String, String>>> parserSupplier
    ) implements OperatorFactory {

        @Override
        public Operator get() {
            return new StringExtractOperator(fieldNames, expressionEvaluator.get(), parserSupplier.get());
        }

        @Override
        public String describe() {
            return "StringExtractOperator[]"; // TODO refine
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

        for (int row = 0; row < rowsCount; row++) {
            Object input = inputEvaluator.computeRow(page, row);
            if (input == null) {
                for (int i = 0; i < fieldNames.length; i++) {
                    blockBuilders[i].appendNull();
                }
                continue;
            }

            String stringInput = BytesRefs.toString(input);
            Map<String, String> items = parser.apply(stringInput);
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
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[]");
        return sb.toString();
    }

    public interface ExtractEvaluator {
        Map<String, Object> computeRow(Page page, int position);
    }
}
