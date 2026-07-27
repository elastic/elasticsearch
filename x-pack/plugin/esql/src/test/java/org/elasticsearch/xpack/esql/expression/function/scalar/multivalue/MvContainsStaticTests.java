/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.index.query.TermsSetQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.TermsSetQuery;
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
 Non-parameterized tests for `MvContains`
 */
public class MvContainsStaticTests extends ESTestCase {
    public void testNotTranslatableWithLiteralArguments() {
        for (DataType dataType : DataType.types()) {
            // these types are not supported in MV_CONTAINS
            if (DataType.isRepresentable(dataType) == false) {
                continue;
            }
            MvContains mvContains = new MvContains(Source.EMPTY, randomLiteral(dataType), randomLiteral(dataType));
            assertThat(mvContains.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.NO));
        }
    }

    public void testNotTranslatableWithNonFoldableArgument() {
        for (DataType dataType : DataType.types()) {
            // these types are not supported in MV_CONTAINS
            if (DataType.isRepresentable(dataType) == false) {
                continue;
            }

            FieldAttribute fieldAttr = new FieldAttribute(
                EMPTY,
                "foo",
                new EsField("abc", dataType, emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
            );
            MvContains mvContains = new MvContains(Source.EMPTY, fieldAttr, referenceAttribute("bar", dataType));
            assertThat(mvContains.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.NO));
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
            // these types are not supported in MV_CONTAINS
            if (DataType.isRepresentable(dataType) == false) {
                continue;
            }
            FieldAttribute fieldAttr = new FieldAttribute(
                EMPTY,
                "abc",
                new EsField("abc", dataType, emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
            );
            MvContains mvContains = new MvContains(Source.EMPTY, fieldAttr, randomLiteral(dataType));
            TranslationAware.Translatable translatable = pushableDataTypes.contains(dataType)
                ? TranslationAware.Translatable.YES
                : TranslationAware.Translatable.NO;
            assertThat(mvContains.translatable(LucenePushdownPredicates.DEFAULT), equalTo(translatable));
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

            MvContains mvContains = new MvContains(Source.EMPTY, fieldAttr, literal);
            assertThat(mvContains.typeResolved(), equalTo(Expression.TypeResolution.TYPE_RESOLVED));

            assertThat(mvContains.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.YES));

            Query query = mvContains.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER);
            assertThat(query, instanceOf(TermsSetQuery.class));
            assertThat(query.toQueryBuilder(), instanceOf(TermsSetQueryBuilder.class));

            TermsSetQueryBuilder queryBuilder = (TermsSetQueryBuilder) query.toQueryBuilder();
            assertThat(queryBuilder.getMinimumShouldMatch(), equalTo(Integer.toString(values.size())));
            assertThat(queryBuilder.getValues(), hasSize(values.size()));
        }
    }
}
