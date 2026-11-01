/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.TermsQuery;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteralValue;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Non-parameterized tests for {@code MvIntersects}
 */
public class MvIntersectsStaticTests extends ESTestCase {

    public void testNotTranslatableWithLiteralArguments() {
        for (DataType dataType : DataType.types()) {
            if (DataType.isRepresentable(dataType) == false) {
                continue;
            }
            MvIntersects mvIntersects = new MvIntersects(Source.EMPTY, randomLiteral(dataType), randomLiteral(dataType));
            assertThat(mvIntersects.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.NO));
        }
    }

    public void testNotTranslatableWithNonFoldableArguments() {
        for (DataType dataType : DataType.types()) {
            if (DataType.isRepresentable(dataType) == false) {
                continue;
            }
            FieldAttribute fieldAttr = new FieldAttribute(
                EMPTY,
                "foo",
                new EsField("abc", dataType, emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
            );
            // field on left, non-foldable on right
            MvIntersects left = new MvIntersects(Source.EMPTY, fieldAttr, referenceAttribute("bar", dataType));
            assertThat(left.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.NO));
            // non-foldable on left, field on right
            MvIntersects right = new MvIntersects(Source.EMPTY, referenceAttribute("bar", dataType), fieldAttr);
            assertThat(right.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.NO));
        }
    }

    static List<DataType> pushableDataTypes = List.of(
        DataType.BOOLEAN,
        DataType.KEYWORD,
        DataType.DOUBLE,
        DataType.DATE_NANOS,
        DataType.IP,
        DataType.VERSION,
        DataType.INTEGER,
        DataType.DATETIME,
        DataType.LONG,
        DataType.UNSIGNED_LONG
    );

    public void testTranslatable() {
        for (DataType dataType : DataType.types()) {
            if (DataType.isRepresentable(dataType) == false) {
                continue;
            }
            FieldAttribute fieldAttr = new FieldAttribute(
                EMPTY,
                "abc",
                new EsField("abc", dataType, emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
            );
            TranslationAware.Translatable expected = pushableDataTypes.contains(dataType)
                ? TranslationAware.Translatable.YES
                : TranslationAware.Translatable.NO;
            // field on left, literal on right
            assertThat(
                new MvIntersects(Source.EMPTY, fieldAttr, randomLiteral(dataType)).translatable(LucenePushdownPredicates.DEFAULT),
                equalTo(expected)
            );
            // literal on left, field on right (mv_intersects is commutative, so pushdown works both ways)
            assertThat(
                new MvIntersects(Source.EMPTY, randomLiteral(dataType), fieldAttr).translatable(LucenePushdownPredicates.DEFAULT),
                equalTo(expected)
            );
        }
    }

    public void testAsQuery() {
        String fieldName = "my_field";
        for (DataType dataType : pushableDataTypes) {
            int size = randomIntBetween(2, 10);
            Set<Object> values = IntStream.range(0, size).mapToObj(i -> randomLiteralValue(dataType)).collect(Collectors.toSet());
            Literal literal = new Literal(EMPTY, values.stream().toList(), dataType);
            FieldAttribute fieldAttr = new FieldAttribute(
                EMPTY,
                fieldName,
                new EsField(fieldName, dataType, emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
            );

            // field on left, literal on right
            MvIntersects withFieldOnLeft = new MvIntersects(Source.EMPTY, fieldAttr, literal);
            assertThat(withFieldOnLeft.typeResolved(), equalTo(Expression.TypeResolution.TYPE_RESOLVED));
            assertThat(withFieldOnLeft.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.YES));
            assertTermsQuery(withFieldOnLeft, fieldName, values.size());

            // literal on left, field on right
            MvIntersects withFieldOnRight = new MvIntersects(Source.EMPTY, literal, fieldAttr);
            assertThat(withFieldOnRight.typeResolved(), equalTo(Expression.TypeResolution.TYPE_RESOLVED));
            assertThat(withFieldOnRight.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.YES));
            assertTermsQuery(withFieldOnRight, fieldName, values.size());
        }
    }

    private static void assertTermsQuery(MvIntersects mvIntersects, String fieldName, int expectedValueCount) {
        Query query = mvIntersects.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER);
        assertThat(query, instanceOf(TermsQuery.class));
        assertThat(query.toQueryBuilder(), instanceOf(TermsQueryBuilder.class));
        TermsQueryBuilder queryBuilder = (TermsQueryBuilder) query.toQueryBuilder();
        assertThat(queryBuilder.fieldName(), equalTo(fieldName));
        assertThat(queryBuilder.values(), hasSize(expectedValueCount));
    }
}
