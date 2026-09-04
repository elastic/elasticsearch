/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;

import java.util.Collections;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

/**
 * Binary-DV string {@code ==}/{@code !=} must stay in the compute engine when the field will also be
 * loaded (avoid Lucene + extract dual pass), or when the value is an empty string (which binary DV
 * cannot distinguish from a missing/null value). Sorted-set keywords still push in all cases.
 */
public class BinaryDvEqualityPushdownTests extends ESTestCase {

    public void testNotEqualsEmptyNotPushedOnBinaryDocValues() {
        assertThat(notEqualsEmpty("url").translatable(binaryDvPredicates()), equalTo(TranslationAware.Translatable.NO));
    }

    public void testEqualsEmptyNotPushedOnBinaryDocValues() {
        assertThat(equalsEmpty("url").translatable(binaryDvPredicates()), equalTo(TranslationAware.Translatable.NO));
    }

    public void testNotEqualsEmptyStillPushedOnSortedSetKeyword() {
        assertThat(notEqualsEmpty("url").translatable(sortedSetPredicates()), equalTo(TranslationAware.Translatable.YES));
    }

    public void testEqualsEmptyStillPushedOnSortedSetKeyword() {
        assertThat(equalsEmpty("url").translatable(sortedSetPredicates()), equalTo(TranslationAware.Translatable.YES));
    }

    public void testEqualsNonEmptyStillPushedOnSortedSetKeywordWhenLoaded() {
        // Sorted-set fields push regardless of whether the field is also loaded by another operator.
        Equals eq = equalsLiteral("url", "http://example.com");
        assertThat(eq.translatable(sortedSetPredicatesLoading("url")), equalTo(TranslationAware.Translatable.YES));
    }

    public void testNotEqualsNonEmptyStillPushedOnSortedSetKeywordWhenLoaded() {
        NotEquals neq = notEqualsLiteral("url", "http://example.com");
        assertThat(neq.translatable(sortedSetPredicatesLoading("url")), equalTo(TranslationAware.Translatable.YES));
    }

    public void testEqualsNonEmptyStillPushedOnBinaryDocValuesWhenNotLoaded() {
        // Selective filter-only query: field not loaded beyond the filter → Lucene pushdown OK.
        Equals eq = equalsLiteral("url", "http://example.com");
        assertThat(eq.translatable(binaryDvPredicates()), equalTo(TranslationAware.Translatable.YES));
    }

    public void testNotEqualsNonEmptyStillPushedOnBinaryDocValuesWhenNotLoaded() {
        // Selective filter-only query: field not loaded beyond the filter → Lucene pushdown OK.
        NotEquals neq = notEqualsLiteral("url", "http://example.com");
        assertThat(neq.translatable(binaryDvPredicates()), equalTo(TranslationAware.Translatable.YES));
    }

    public void testNotEqualsNonEmptyNotPushedWhenFieldWillBeLoaded() {
        NotEquals neq = notEqualsLiteral("url", "http://example.com");
        assertThat(neq.translatable(binaryDvPredicatesLoading("url")), equalTo(TranslationAware.Translatable.NO));
    }

    public void testEqualsNonEmptyNotPushedWhenFieldWillBeLoaded() {
        Equals eq = equalsLiteral("url", "http://example.com");
        assertThat(eq.translatable(binaryDvPredicatesLoading("url")), equalTo(TranslationAware.Translatable.NO));
    }

    private static NotEquals notEqualsEmpty(String name) {
        return notEqualsLiteral(name, "");
    }

    private static Equals equalsEmpty(String name) {
        return equalsLiteral(name, "");
    }

    private static NotEquals notEqualsLiteral(String name, String value) {
        return new NotEquals(Source.EMPTY, keywordField(name), new Literal(Source.EMPTY, new BytesRef(value), DataType.KEYWORD));
    }

    private static Equals equalsLiteral(String name, String value) {
        return new Equals(Source.EMPTY, keywordField(name), new Literal(Source.EMPTY, new BytesRef(value), DataType.KEYWORD));
    }

    private static FieldAttribute keywordField(String name) {
        return new FieldAttribute(
            Source.EMPTY,
            name,
            new EsField(name, DataType.KEYWORD, Collections.emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
        );
    }

    private static LucenePushdownPredicates binaryDvPredicates() {
        return LucenePushdownPredicates.from(new EsqlTestUtils.TestSearchStats() {
            @Override
            public boolean usesBinaryDocValues(FieldAttribute.FieldName field) {
                return true;
            }
        }, new EsqlFlags(true));
    }

    private static LucenePushdownPredicates binaryDvPredicatesLoading(String... fields) {
        return LucenePushdownPredicates.from(new EsqlTestUtils.TestSearchStats() {
            @Override
            public boolean usesBinaryDocValues(FieldAttribute.FieldName field) {
                return true;
            }
        }, new EsqlFlags(true), Set.of(fields));
    }

    private static LucenePushdownPredicates sortedSetPredicates() {
        return LucenePushdownPredicates.from(new EsqlTestUtils.TestSearchStats(), new EsqlFlags(true));
    }

    private static LucenePushdownPredicates sortedSetPredicatesLoading(String... fields) {
        return LucenePushdownPredicates.from(new EsqlTestUtils.TestSearchStats(), new EsqlFlags(true), Set.of(fields));
    }
}
