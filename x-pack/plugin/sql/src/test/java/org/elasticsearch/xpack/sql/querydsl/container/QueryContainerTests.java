/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.AttributeMap;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.tree.SourceTests;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.sql.querydsl.query.BoolQuery;
import org.elasticsearch.xpack.sql.querydsl.query.MatchAll;
import org.elasticsearch.xpack.sql.querydsl.query.NestedQuery;
import org.elasticsearch.xpack.sql.querydsl.query.Query;
import org.elasticsearch.xpack.sql.querydsl.query.RangeQuery;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public class QueryContainerTests extends ESTestCase {
    private Source source = SourceTests.randomSource();
    private String path = randomAlphaOfLength(5);
    private String name = randomAlphaOfLength(5);
    private String format = null;
    private boolean hasDocValues = randomBoolean();

    public void testRewriteToContainNestedFieldNoQuery() {
        Query expected = new NestedQuery(source, path, singletonMap(name, new SimpleImmutableEntry<>(hasDocValues, format)),
                new MatchAll(source));
        assertEquals(expected, QueryContainer.rewriteToContainNestedField(null, source, path, name, format, hasDocValues));
    }

    public void testRewriteToContainsNestedFieldWhenContainsNestedField() {
        Query original = new BoolQuery(source, true,
            new NestedQuery(source, path, singletonMap(name, new SimpleImmutableEntry<>(hasDocValues, format)),
                    new MatchAll(source)),
            new RangeQuery(source, randomAlphaOfLength(5), 0, randomBoolean(), 100, randomBoolean()));
        assertSame(original, QueryContainer.rewriteToContainNestedField(original, source, path, name, format, randomBoolean()));
    }

    public void testRewriteToContainsNestedFieldWhenCanAddNestedField() {
        Query buddy = new RangeQuery(source, randomAlphaOfLength(5), 0, randomBoolean(), 100, randomBoolean());
        Query original = new BoolQuery(source, true,
            new NestedQuery(source, path, emptyMap(), new MatchAll(source)),
            buddy);
        Query expected = new BoolQuery(source, true,
            new NestedQuery(source, path, singletonMap(name, new SimpleImmutableEntry<>(hasDocValues, format)),
                    new MatchAll(source)),
            buddy);
        assertEquals(expected, QueryContainer.rewriteToContainNestedField(original, source, path, name, format, hasDocValues));
    }

    public void testRewriteToContainsNestedFieldWhenDoesNotContainNestedFieldAndCantAdd() {
        Query original = new RangeQuery(source, randomAlphaOfLength(5), 0, randomBoolean(), 100, randomBoolean());
        Query expected = new BoolQuery(source, true,
            original,
            new NestedQuery(source, path, singletonMap(name, new SimpleImmutableEntry<>(hasDocValues, format)),
                    new MatchAll(source)));
        assertEquals(expected, QueryContainer.rewriteToContainNestedField(original, source, path, name, format, hasDocValues));
    }

    public void testColumnMaskShouldDuplicateSameAttributes() {

        EsField esField = new EsField("str", DataType.TEXT, emptyMap(), true);

        Attribute first = new FieldAttribute(Source.EMPTY, "first", esField);
        Attribute second = new FieldAttribute(Source.EMPTY, "second", esField);
        Attribute third = new FieldAttribute(Source.EMPTY, "third", esField);
        Attribute fourth = new FieldAttribute(Source.EMPTY, "fourth", esField);
        Alias firstAliased = new Alias(Source.EMPTY, "firstAliased", first);

        Map<Attribute, Expression> aliasesMap = new LinkedHashMap<>();
        aliasesMap.put(firstAliased.toAttribute(), first);

        QueryContainer queryContainer = new QueryContainer()
            .withAliases(new AttributeMap<>(aliasesMap))
            .addColumn(third)
            .addColumn(first)
            .addColumn(fourth)
            .addColumn(firstAliased.toAttribute())
            .addColumn(second)
            .addColumn(first)
            .addColumn(fourth);

        BitSet result = queryContainer.columnMask(Arrays.asList(
            first,
            first,
            second,
            third,
            firstAliased.toAttribute()
        ));

        BitSet expected = new BitSet();
        expected.set(0, true);
        expected.set(1, true);
        expected.set(2, false);
        expected.set(3, true);
        expected.set(4, true);
        expected.set(5, true);
        expected.set(6, false);


        assertEquals(expected, result);
    }
}
