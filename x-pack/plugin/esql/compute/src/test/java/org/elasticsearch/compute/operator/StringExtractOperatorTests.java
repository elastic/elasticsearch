/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class StringExtractOperatorTests extends OperatorTestCase {
    @Override
    protected SourceOperator simpleInput(int end) {
        List<BytesRef> input = LongStream.range(0, end)
            .mapToObj(l -> new BytesRef("word1_" + l + " word2_" + l + " word3_" + l))
            .collect(Collectors.toList());
        return new BytesRefBlockSourceOperator(input);
    }

    record FirstWord(String fieldName) implements Function<String, Map<String, String>> {
        @Override
        public Map<String, String> apply(String s) {
            return Map.of(fieldName, s.split(" ")[0]);
        }
    }

    @Override
    protected Operator.OperatorFactory simple(BigArrays bigArrays) {
        Supplier<Function<String, Map<String, String>>> expEval = () -> new FirstWord("test");
        return new StringExtractOperator.StringExtractOperatorFactory(new String[] { "test" }, () -> page -> page.getBlock(0), expEval);
    }

    @Override
    protected String expectedDescriptionOfSimple() {
        return "StringExtractOperator[fields=[test]]";
    }

    @Override
    protected String expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        BytesRef buffer = new BytesRef();
        int pos = 0;
        for (var page : results) {
            BytesRefBlock block1 = page.getBlock(1);

            for (int i = 0; i < page.getPositionCount(); i++) {
                assertEquals(new BytesRef("word1_" + pos).utf8ToString(), block1.getBytesRef(i, buffer).utf8ToString());
                pos++;
            }
        }
    }

    @Override
    protected ByteSizeValue smallEnoughToCircuitBreak() {
        assumeTrue("doesn't use big arrays so can't break", false);
        return null;
    }
}
