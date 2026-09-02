/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.querydsl.query.WildcardQuery;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class StartsWithStaticTests extends ESTestCase {

    public void testLuceneQuery_AllLiterals_NonTranslatable() {
        var function = new StartsWith(Source.EMPTY, Literal.keyword(Source.EMPTY, "test"), Literal.keyword(Source.EMPTY, "test"));

        assertThat(function.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.NO));
    }

    public void testLuceneQuery_NonFoldablePrefix_NonTranslatable() {
        var function = new StartsWith(
            Source.EMPTY,
            new FieldAttribute(
                Source.EMPTY,
                "field",
                new EsField("field", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
            ),
            new FieldAttribute(
                Source.EMPTY,
                "field",
                new EsField("prefix", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
            )
        );

        assertThat(function.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.NO));
    }

    public void testLuceneQuery_NonFoldablePrefix_Translatable() {
        var function = new StartsWith(
            Source.EMPTY,
            new FieldAttribute(
                Source.EMPTY,
                "field",
                new EsField("prefix", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
            ),
            Literal.keyword(Source.EMPTY, "a*b?c\\")
        );

        assertThat(function.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.YES));

        var query = function.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER);

        assertThat(query, equalTo(new WildcardQuery(Source.EMPTY, "field", "a\\*b\\?c\\\\*", false, true)));
    }

    public void testLuceneQuery_OnlyEscapesWildcardChars() {
        // Characters that are special in Lucene query-parser syntax but NOT in wildcard syntax.
        // QueryParser.escape would escape these, but our wildcard escaping must leave them untouched.
        for (String ch : new String[] { "+", "-", "!", "(", ")", "^", "\"", "~", "/" }) {
            var function = new StartsWith(
                Source.EMPTY,
                new FieldAttribute(
                    Source.EMPTY,
                    "field",
                    new EsField("prefix", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
                ),
                Literal.keyword(Source.EMPTY, "k8s" + ch + "idx")
            );

            var query = function.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER);

            assertThat(
                "character '" + ch + "' must not be escaped",
                query,
                equalTo(new WildcardQuery(Source.EMPTY, "field", "k8s" + ch + "idx*", false, true))
            );
        }
    }

    public void testLuceneQuery_StringLikeOnIndexFalse() {
        var function = new StartsWith(
            Source.EMPTY,
            new FieldAttribute(
                Source.EMPTY,
                "field",
                new EsField("prefix", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
            ),
            Literal.keyword(Source.EMPTY, "test")
        );

        var predicates = LucenePushdownPredicates.forCanMatch(null, new EsqlFlags(false));
        var query = function.asQuery(predicates, TranslatorHandler.TRANSLATOR_HANDLER);

        assertThat(query, equalTo(new WildcardQuery(Source.EMPTY, "field", "test*", false, false)));
    }
}
