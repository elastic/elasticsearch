/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * Four reductions over {@code items} that share one read of its children: each takes its own fields out of the pages the
 * read gives.
 */
public class NestedReduceOperatorTests extends NestedIndexOperatorTestCase {
    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        Attribute price = new ReferenceAttribute(Source.EMPTY, "price", DataType.LONG);
        Attribute note = new ReferenceAttribute(Source.EMPTY, "note", DataType.TEXT);
        List<NestedReduction> reductions = List.of(
            reduction("NESTED(COUNT(items.price))", new Count(Source.EMPTY, price), false, List.of(price)),
            reduction("NESTED(SUM(items.price))", new Sum(Source.EMPTY, price), false, List.of(price)),
            reduction(
                "NESTED_ANY(items.note == \"n2\")",
                new Equals(Source.EMPTY, note, new Literal(Source.EMPTY, new BytesRef("n2"), DataType.KEYWORD)),
                false,
                List.of(note)
            ),
            reduction("NESTED_JSON(items)", Literal.NULL, true, List.of(price, note))
        );
        NestedChildren.Factory children = NestedChildren.factory(
            new IndexedByShardIdFromSingleton<>(shard),
            "items",
            List.of("price", "note"),
            List.of(DataType.LONG, DataType.TEXT)
        );
        var group = new NestedReduceOperator.Group(
            children,
            reductions.stream().map(r -> r.reducer(FoldContext.small())).toList(),
            List.of(new int[] { 0, 1 }, new int[] { 0, 1 }, new int[] { 0, 2 }, new int[] { 0, 1, 2 })
        );
        return new NestedReduceOperator.Factory(List.of(group));
    }

    private static NestedReduction reduction(String text, Expression body, boolean json, List<Attribute> inputs) {
        return new NestedReduction(
            new Source(1, 0, text),
            "items",
            body,
            List.of(),
            json,
            false,
            inputs,
            inputs.stream().map(Attribute::name).toList()
        );
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        List<List<Object>> expected = new ArrayList<>();
        for (Page page : input) {
            for (int p = 0; p < page.getPositionCount(); p++) {
                int parent = parent(page, p);
                int children = parent % 4;
                StringBuilder json = new StringBuilder("[");
                for (int j = 0; j < children; j++) {
                    json.append(j == 0 ? "" : ",").append("{\"price\":").append(10L * parent + j).append(",\"note\":\"n").append(j);
                    json.append("\"}");
                }
                expected.add(
                    Arrays.asList(
                        parent,
                        (long) children,
                        children == 0 ? null : 10L * parent * children + children * (children - 1) / 2,
                        children >= 3,
                        children == 0 ? null : new BytesRef(json.append("]").toString())
                    )
                );
            }
        }
        List<List<Object>> actual = new ArrayList<>();
        for (Page page : results) {
            for (int p = 0; p < page.getPositionCount(); p++) {
                List<Object> row = new ArrayList<>();
                row.add(parent(page, p));
                for (int b = 1; b < page.getBlockCount(); b++) {
                    row.add(BlockUtils.toJavaObject(page.getBlock(b), p));
                }
                actual.add(row);
            }
        }
        assertThat(actual, equalTo(expected));
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo(
            "NestedReduceOperator[[NESTED(COUNT(items.price)), NESTED(SUM(items.price)), NESTED_ANY(items.note == \"n2\"), "
                + "NESTED_JSON(items)]]"
        );
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("NestedReduceOperator[1 nested fields]");
    }
}
