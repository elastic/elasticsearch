/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Location;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class ConcatTests extends AbstractScalarFunctionTestCase {
    @Override
    protected List<Object> simpleData() {
        return List.of(new BytesRef(randomAlphaOfLength(3)), new BytesRef(randomAlphaOfLength(3)));
    }

    @Override
    protected Expression expressionForSimpleData() {
        return new Concat(Source.EMPTY, field("first", DataTypes.KEYWORD), List.of(field("second", DataTypes.KEYWORD)));
    }

    @Override
    protected DataType expressionForSimpleDataType() {
        return DataTypes.KEYWORD;
    }

    @Override
    protected Matcher<Object> resultMatcher(List<Object> simpleData) {
        return equalTo(new BytesRef(simpleData.stream().map(o -> ((BytesRef) o).utf8ToString()).collect(Collectors.joining())));
    }

    @Override
    protected String expectedEvaluatorSimpleToString() {
        return "Concat{values=[Keywords[channel=0], Keywords[channel=1]]}";
    }

    @Override
    protected Expression constantFoldable(List<Object> simpleData) {
        return new Concat(
            Source.EMPTY,
            new Literal(Source.EMPTY, simpleData.get(0), DataTypes.KEYWORD),
            List.of(new Literal(Source.EMPTY, simpleData.get(1), DataTypes.KEYWORD))
        );
    }

    @Override
    public void testResolveTypeInvalid() {
        for (Concat c : new Concat[] {
            new Concat(
                new Source(Location.EMPTY, "foo"),
                new Literal(new Source(Location.EMPTY, "1"), 1, DataTypes.INTEGER),
                List.of(new Literal(Source.EMPTY, "a", DataTypes.KEYWORD))
            ),
            new Concat(
                new Source(Location.EMPTY, "foo"),
                new Literal(Source.EMPTY, "a", DataTypes.KEYWORD),
                List.of(new Literal(new Source(Location.EMPTY, "1"), 1, DataTypes.INTEGER))
            ) }) {
            Expression.TypeResolution resolution = c.resolveType();
            assertTrue(resolution.unresolved());
            assertThat(resolution.message(), equalTo("argument of [foo] must be [string], found value [1] type [integer]"));
        }
    }

    public void testMany() {
        List<Object> simpleData = Stream.of("cats", " ", "and", " ", "dogs").map(s -> (Object) new BytesRef(s)).toList();
        assertThat(
            evaluator(
                new Concat(
                    Source.EMPTY,
                    field("a", DataTypes.KEYWORD),
                    IntStream.range(1, 5).mapToObj(i -> field(Integer.toString(i), DataTypes.KEYWORD)).toList()
                )
            ).get().computeRow(row(simpleData), 0),
            equalTo(new BytesRef("cats and dogs"))
        );
    }

    public void testSomeConstant() {
        List<Object> simpleData = Stream.of("cats", "and", "dogs").map(s -> (Object) new BytesRef(s)).toList();
        assertThat(
            evaluator(
                new Concat(
                    Source.EMPTY,
                    field("a", DataTypes.KEYWORD),
                    List.of(
                        new Literal(Source.EMPTY, new BytesRef(" "), DataTypes.KEYWORD),
                        field("b", DataTypes.KEYWORD),
                        new Literal(Source.EMPTY, new BytesRef(" "), DataTypes.KEYWORD),
                        field("c", DataTypes.KEYWORD)
                    )
                )
            ).get().computeRow(row(simpleData), 0),
            equalTo(new BytesRef("cats and dogs"))
        );
    }
}
