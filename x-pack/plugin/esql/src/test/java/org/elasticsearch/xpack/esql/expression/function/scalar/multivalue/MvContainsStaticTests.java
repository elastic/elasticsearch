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

    // 테스트 케이스 1: 기본 pushdown
    public void testMvContainsLucenePushdown() {
        FieldAttribute field = new FieldAttribute(
            EMPTY,
            "tags",
            new EsField("tags", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );

        // KEYWORD 타입은 BytesRef를 사용해야 함!
        List<Object> values = Arrays.asList(
            new BytesRef("important"),
            new BytesRef("urgent")
        );
        Literal subset = new Literal(EMPTY, values, DataType.KEYWORD);

        MvContains mvContains = new MvContains(EMPTY, field, subset);

        // translatable 확인
        assertEquals(
            TranslationAware.Translatable.YES,
            mvContains.translatable(LucenePushdownPredicates.DEFAULT)
        );

        // Query 생성 확인
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

    // 테스트 케이스 2: 빈 리스트
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

        // 빈 subset은 MatchAll 반환
        assertTrue(query instanceof org.elasticsearch.xpack.esql.core.querydsl.query.MatchAll);
    }

    // 테스트 케이스 3: null 처리
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

    // 테스트 케이스 4: pushdown 불가능한 경우
    public void testMvContainsNotPushable() {
        // Literal이 아닌 경우
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

    // 테스트 케이스 5: 인덱스되지 않은 필드
    public void testMvContainsNotIndexedField() {
        FieldAttribute notIndexed = new FieldAttribute(
            EMPTY,
            "description",
            new EsField("description", DataType.TEXT, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );

        // INTEGER 타입으로 변경 (BytesRef 문제 회피)
        List<Object> values = Arrays.asList(1, 2, 3);
        Literal subset = new Literal(EMPTY, values, DataType.INTEGER);

        MvContains mvContains = new MvContains(EMPTY, notIndexed, subset);

        assertEquals(
            TranslationAware.Translatable.NO,
            mvContains.translatable(LucenePushdownPredicates.DEFAULT)
        );
    }
}
