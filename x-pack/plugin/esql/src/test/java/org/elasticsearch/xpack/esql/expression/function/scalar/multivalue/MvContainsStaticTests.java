/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.querydsl.query.TermsSetQuery;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;

public class MvContainsStaticTests extends ESTestCase {

    public void testMvContainsLucenePushdown() {
        FieldAttribute field = new FieldAttribute(
            EMPTY,
            "tags",
            new EsField("tags", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );

        List<Object> values = Arrays.asList(
            new BytesRef("important"),
            new BytesRef("urgent")
        );
        Literal subset = new Literal(EMPTY, values, DataType.KEYWORD);

        MvContains mvContains = new MvContains(EMPTY, field, subset);

        assertEquals(
            TranslationAware.Translatable.YES,
            mvContains.translatable(LucenePushdownPredicates.DEFAULT)
        );

        var query = mvContains.asQuery(
            LucenePushdownPredicates.DEFAULT,
            TranslatorHandler.TRANSLATOR_HANDLER
        );

        assertTrue(query instanceof TermsSetQuery);
        TermsSetQuery termsSetQuery = (TermsSetQuery) query;
        assertEquals("tags", termsSetQuery.field());
        assertEquals(values, termsSetQuery.terms());
        assertEquals(2, termsSetQuery.minimumShouldMatch());
    }

    public void testMvContainsEmptySubset() {
        FieldAttribute field = new FieldAttribute(
            EMPTY,
            "tags",
            new EsField("tags", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Literal emptySubset = new Literal(EMPTY, List.of(), DataType.KEYWORD);

        MvContains mvContains = new MvContains(EMPTY, field, emptySubset);

        var query = mvContains.asQuery(
            LucenePushdownPredicates.DEFAULT,
            TranslatorHandler.TRANSLATOR_HANDLER
        );

        assertTrue(query instanceof org.elasticsearch.xpack.esql.core.querydsl.query.MatchAll);
    }

    public void testMvContainsNullSubset() {
        FieldAttribute field = new FieldAttribute(
            EMPTY,
            "tags",
            new EsField("tags", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Literal nullSubset = new Literal(EMPTY, null, DataType.NULL);

        MvContains mvContains = new MvContains(EMPTY, field, nullSubset);

        var query = mvContains.asQuery(
            LucenePushdownPredicates.DEFAULT,
            TranslatorHandler.TRANSLATOR_HANDLER
        );

        assertTrue(query instanceof org.elasticsearch.xpack.esql.core.querydsl.query.MatchAll);
    }

    public void testMvContainsNotPushable() {
        FieldAttribute field1 = new FieldAttribute(
            EMPTY,
            "tags",
            new EsField("tags", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        FieldAttribute field2 = new FieldAttribute(
            EMPTY,
            "categories",
            new EsField("categories", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );

        MvContains mvContains = new MvContains(EMPTY, field1, field2);

        assertEquals(
            TranslationAware.Translatable.NO,
            mvContains.translatable(LucenePushdownPredicates.DEFAULT)
        );
    }

    public void testMvContainsNotIndexedField() {
        FieldAttribute notIndexed = new FieldAttribute(
            EMPTY,
            "description",
            new EsField("description", DataType.TEXT, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );

        List<Object> values = Arrays.asList(1, 2, 3);
        Literal subset = new Literal(EMPTY, values, DataType.INTEGER);

        MvContains mvContains = new MvContains(EMPTY, notIndexed, subset);

        assertEquals(
            TranslationAware.Translatable.NO,
            mvContains.translatable(LucenePushdownPredicates.DEFAULT)
        );
    }
}
