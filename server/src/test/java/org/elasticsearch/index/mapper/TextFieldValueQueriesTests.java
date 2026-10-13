/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.lucene.queries.BinaryDocValuesQueries;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * The queries a {@code text} field answers over the values it keeps as the columnar codec's payload, which match a
 * document's value whole rather than the tokens that value analyzes into.
 */
public class TextFieldValueQueriesTests extends MapperServiceTestCase {

    private static final List<String> DOCS = List.of("the quick brown fox", "quick", "jumps over the lazy dog");

    public void testColumnarTextAnswersOverItsValues() throws IOException {
        assertWholeValueSemantics(columnar(b -> b.field("type", "text")));
    }

    /**
     * Keeping values is not enough: outside the columnar modes they are framed so that a query reads them a document
     * at a time, which is no better than reading the rows, so the field answers nothing here.
     */
    public void testOutsideTheColumnarModesThereAreNoValueQueries() throws IOException {
        final MapperService mapperService = mapper(b -> {
            b.field("type", "text");
            b.field("doc_values", true);
        });
        assertTrue(mapperService.fieldType("field").hasDocValues());
        assertThat(((TextFamilyFieldType) mapperService.fieldType("field")).valueQueries(), nullValue());
    }

    /** A field keeping no values has none to answer over. */
    public void testWithoutDocValuesThereAreNoValueQueries() throws IOException {
        final MapperService mapperService = mapper(b -> b.field("type", "text"));
        assertThat(valueQueries(mapperService), nullValue());
    }

    /**
     * Each predicate the field answers over its values, matched against the value whole: the document whose value is
     * {@code quick} answers a term, and the document that merely holds that token does not.
     */
    private void assertWholeValueSemantics(MapperService mapperService) throws IOException {
        final TextFamilyFieldType field = (TextFamilyFieldType) mapperService.fieldType("field");
        assertThat(field.valueQueries(), notNullValue());
        withIndexOf(mapperService, (searcher, context) -> {
            assertEquals("the value itself", 1, count(searcher, field.termLikeQuery("quick", context)));
            assertEquals("a token of a value is not the value", 0, count(searcher, field.termLikeQuery("brown", context)));
            assertEquals(
                "any of the values",
                2,
                count(searcher, field.termsLikeQuery(List.of("quick", "jumps over the lazy dog"), context))
            );
            assertEquals("a prefix of the value", 1, count(searcher, field.wildcardLikeQuery("the quick*", null, false, context)));
            assertEquals("a pattern spanning the value", 1, count(searcher, field.wildcardLikeQuery("the*fox", null, false, context)));
            assertEquals("a regular expression over the value", 2, count(searcher, regexpLike(field, ".*the.*", context)));
            assertEquals("the values from a bound on", 2, count(searcher, field.rangeLikeQuery("q", null, true, false, context)));
            assertEquals(
                "and up to a bound, in the order of the values",
                2,
                count(searcher, field.rangeLikeQuery(null, "quick", false, true, context))
            );
        });
    }

    /**
     * A field keeping no values answers these as it always has, over the tokens its index holds: the document holding
     * {@code brown} as a token answers a term for it.
     */
    public void testWithoutValuesTheFieldsOwnQueriesAnswer() throws IOException {
        final MapperService mapperService = mapper(b -> b.field("type", "text"));
        final TextFamilyFieldType field = (TextFamilyFieldType) mapperService.fieldType("field");
        assertThat(field.valueQueries(), nullValue());
        withIndexOf(mapperService, (searcher, context) -> {
            assertEquals("a token of a value", 2, count(searcher, field.termLikeQuery("quick", context)));
            assertEquals("and another", 1, count(searcher, field.termLikeQuery("brown", context)));
        });
    }

    private static Query regexpLike(TextFamilyFieldType field, String pattern, SearchExecutionContext context) {
        return field.regexpLikeQuery(pattern, RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, null, context);
    }

    private void withIndexOf(MapperService mapperService, CheckedBiConsumer<IndexSearcher, SearchExecutionContext, IOException> check)
        throws IOException {
        withLuceneIndex(mapperService, iw -> {
            for (String doc : DOCS) {
                iw.addDocument(mapperService.documentMapper().parse(source(b -> b.field("field", doc))).rootDoc());
            }
        }, reader -> check.accept(newSearcher(reader), createSearchExecutionContext(mapperService)));
    }

    private static int count(IndexSearcher searcher, Query query) throws IOException {
        return searcher.count(query);
    }

    private static BinaryDocValuesQueries valueQueries(MapperService mapperService) {
        return ((TextFamilyFieldType) mapperService.fieldType("field")).valueQueries();
    }

    private MapperService mapper(CheckedConsumer<XContentBuilder, IOException> field) throws IOException {
        return createMapperService(fieldMapping(field));
    }

    private MapperService columnar(CheckedConsumer<XContentBuilder, IOException> field) throws IOException {
        return createMapperService(
            Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build(),
            fieldMapping(field)
        );
    }
}
