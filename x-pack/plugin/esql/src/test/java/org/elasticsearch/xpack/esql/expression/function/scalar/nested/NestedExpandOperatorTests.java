/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class NestedExpandOperatorTests extends NestedIndexOperatorTestCase {
    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        NestedChildren.Factory children = NestedChildren.factory(
            new IndexedByShardIdFromSingleton<>(shard),
            "items",
            List.of("price", "note"),
            List.of(DataType.LONG, DataType.TEXT),
            Collections.nCopies(2, (ValuesSourceReaderOperator.ConverterFactory) null),
            PlannerSettings.DEFAULTS
        );
        return new NestedExpandOperator.Factory(0, children, "items", List.of(DataType.LONG, DataType.TEXT));
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        List<List<Object>> expected = new ArrayList<>();
        for (Page page : input) {
            for (int p = 0; p < page.getPositionCount(); p++) {
                int parent = parent(page, p);
                if (parent % 4 == 0) {
                    expected.add(Arrays.asList(parent, null, null));
                }
                for (int j = 0; j < parent % 4; j++) {
                    expected.add(List.of(parent, 10L * parent + j, "n" + j));
                }
            }
        }
        List<List<Object>> actual = new ArrayList<>();
        for (Page page : results) {
            LongBlock prices = page.getBlock(1);
            BytesRefBlock notes = page.getBlock(2);
            for (int p = 0; p < page.getPositionCount(); p++) {
                actual.add(
                    Arrays.asList(
                        parent(page, p),
                        prices.isNull(p) ? null : prices.getLong(prices.getFirstValueIndex(p)),
                        notes.isNull(p) ? null : notes.getBytesRef(notes.getFirstValueIndex(p), new BytesRef()).utf8ToString()
                    )
                );
            }
        }
        assertThat(actual, equalTo(expected));
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("NestedExpandOperator[path=items]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("NestedExpandOperator[path=items]");
    }
}
